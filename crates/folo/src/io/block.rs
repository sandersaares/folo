use crate::{io, util::PinnedSlabChain};
use std::{
    cell::RefCell,
    fmt,
    mem::{self, ManuallyDrop},
    ptr,
};
use tracing::{event, Level};
use windows::Win32::{
    Foundation::{ERROR_IO_PENDING, NTSTATUS, STATUS_SUCCESS},
    System::IO::{OVERLAPPED, OVERLAPPED_ENTRY},
};

/// Maintains the backing storage for I/O blocks and organizes their allocation/release.
///
/// # Safety
///
/// Contents of a BlockStore are exposed to native code. For safe operation, the BlockStore must
/// be freed after all native I/O operations referencing the blocks have been terminated. This
/// includes:
///
/// * All file handles must be closed.
/// * All I/O completion ports must be closed.
///
/// It is not required to wait for scheduled I/O completions to finish - closing the handles is
/// enough. TODO: Prove it.
#[derive(Debug)]
pub(super) struct BlockStore {
    blocks: RefCell<PinnedSlabChain<Block>>,
}

impl BlockStore {
    pub fn new() -> Self {
        Self {
            blocks: RefCell::new(PinnedSlabChain::new()),
        }
    }

    /// Allocates a new block for I/O operations.
    pub fn allocate(&self) -> PrepareBlock {
        let mut blocks = self.blocks.borrow_mut();

        let inserter = blocks.begin_insert();
        let key = inserter.index();
        let block = inserter.insert(Block::new(key));

        PrepareBlock {
            // SAFETY: We deliberately disconnect from Rust lifetime tracking because the block
            // is owned by a hybrid lifetime management logic built on the chain of types below
            // and the operating system for part of its life cycle.
            block: unsafe { mem::transmute(block) },
            control: self.control_node(),
            // We start by assuming each buffer is fully used. If the caller wants to use a subset,
            // they have the opportunity to specify a smaller size via `PrepareBlock::set_length()`.
            length: BLOCK_SIZE_BYTES,
        }
    }

    /// Converts a block that has been handed over to the operating system into a `CompleteBlock`
    /// that represents the completed operation. We consume here the OVERLAPPED_ENTRY structure
    /// that contains not only the block but also the status and the number of bytes transferred.
    ///
    /// # Safety
    ///
    /// The input value must be the result of delivering to the operating system a legitimate
    /// OVERLAPPED pointer obtained from the callback given to `PrepareBlock::begin()` earlier.
    /// You must also have received a completion notification from the OS, saying that the block is
    /// again ready for your use.
    pub unsafe fn complete(&self, overlapped_entry: OVERLAPPED_ENTRY) -> CompleteBlock {
        let bytes_transferred = overlapped_entry.dwNumberOfBytesTransferred as usize;
        let status = NTSTATUS(overlapped_entry.Internal as i32);

        // Transform the *OVERLAPPED back into a &'static Block.
        let block = &mut *(overlapped_entry.lpOverlapped as *mut Block);

        CompleteBlock {
            // SAFETY: We deliberately disconnect from Rust lifetime tracking because the block
            // is owned by a hybrid lifetime management logic built on the chain of types below
            // and the operating system for part of its life cycle.
            block: mem::transmute(block),
            control: self.control_node(),
            length: bytes_transferred,
            status,
        }
    }

    /// Converts a block that was never handed over to the operating system into a `CompleteBlock`
    /// that represents the completed operation. We consume here the original OVERLAPPED that was
    /// provided to the native I/O function.
    ///
    /// This is for use with synchronous I/O operations that complete immediately, without
    /// triggering a completion notification.
    unsafe fn complete_immediately(&self, overlapped: *mut OVERLAPPED) -> CompleteBlock {
        // Transform the *OVERLAPPED back into a &'static Block.
        let block = &mut *(overlapped as *mut Block);

        let bytes_transferred = block.immediate_bytes_transferred as usize;
        assert!(bytes_transferred <= block.buffer.len());

        CompleteBlock {
            // SAFETY: We deliberately disconnect from Rust lifetime tracking because the block
            // is owned by a hybrid lifetime management logic built on the chain of types below
            // and the operating system for part of its life cycle.
            block: mem::transmute(block),
            control: self.control_node(),
            length: bytes_transferred,
            status: STATUS_SUCCESS,
        }
    }

    fn release(&self, key: BlockKey) {
        assert!(key != BlockKey::MAX);

        self.blocks.borrow_mut().remove(key);
    }

    fn control_node(&self) -> ControlNode {
        ControlNode {
            // SAFETY: We deliberately disconnect from Rust lifetime tracking because we create
            // a circular reference. See type-level safety comments to understand safe handling.
            store: unsafe { mem::transmute(self) },
        }
    }
}

type BlockKey = usize;

/// Constrained API surface that allows a block to command the BlockStore that owns it. This creates
/// a circular reference between a block and the BlockStore, so we always use BlockStore via
/// interior mutability.
#[derive(Clone, Debug)]
#[repr(transparent)]
struct ControlNode {
    // SAFETY: This is not actually a 'static reference - we rely on the fact that all ControlNodes
    // are inside blocks owned by the very same BlockStore we reference here. The only way to reach
    // an invalid BlockStore via this is if something already reached an invalid Block/ControlNode.
    store: &'static BlockStore,
}

impl ControlNode {
    fn release(&mut self, key: BlockKey) {
        self.store.release(key);
    }

    unsafe fn complete_immediately(&mut self, overlapped: *mut OVERLAPPED) -> CompleteBlock {
        self.store.complete_immediately(overlapped)
    }
}

// Rust... puts the slab chunks on the stack?! Before putting them in memory. What? Well, it does.
// That means if you make a large block, you will overflow the stack before it gets copied to heap.
const BLOCK_SIZE_BYTES: usize = 64 * 1024;

/// An I/O block contains the data structures required to communicate with the operating system
/// and obtain the result of an asynchronous I/O operation.
///
/// As they participate in FFI calls, they can be leaked to the operating system. There are two
/// forms in which blocks are exposed in code:
///
/// * PrepareBlock is a reference to a block that is currently owned by Folo code and is being
///   prepared as part of starting an I/O operation (e.g. a buffer is being filled or offset set).
/// * CompleteBlock is a reference to a block that carries the result of a completed operation and
///   is intended to be consumed before being reed.
///
/// Between the above steps, the operating system owns the block in the form of a raw pointer to
/// OVERLAPPED (wrapped in OVERLAPPED_ENTRY when handed back to us).
///
/// Dropping the block in any of the states releases all the resources, except when you call the
/// function to convert it to the native operating system objects (`.into_buffer_and_overlapped()`).
#[repr(C)] // Facilitates conversion to/from OVERLAPPED.
struct Block {
    /// The part of the operation block visible to the operating system.
    ///
    /// NB! This must be the first item in the struct because
    /// we treat `*Block` and `*OVERLAPPED` as equivalent!
    overlapped: OVERLAPPED,

    /// The buffer containing the data affected by the operation.
    buffer: [u8; BLOCK_SIZE_BYTES],

    /// Used to operate the control node, which requires us to know our own key.
    key: BlockKey,

    /// If the operation completed immediately (synchronously), this stores the number of bytes
    /// transferred. Normally getting this information requires a round-trip through the IOCP but
    /// we expect that for immediate completions this value is set inline.
    immediate_bytes_transferred: u32,

    /// This is where the I/O completion handler will deliver the result of the operation.
    /// Value is cleared when consumed, to make it obvious if any accidental reuse occurs.
    result_tx: Option<oneshot::Sender<io::Result<CompleteBlock>>>,
    result_rx: Option<oneshot::Receiver<io::Result<CompleteBlock>>>,
}

impl Block {
    pub fn new(key: BlockKey) -> Self {
        let (result_tx, result_rx) = oneshot::channel();

        Self {
            overlapped: OVERLAPPED::default(),
            buffer: [0; BLOCK_SIZE_BYTES],
            key,
            immediate_bytes_transferred: 0,
            result_tx: Some(result_tx),
            result_rx: Some(result_rx),
        }
    }
}

impl fmt::Debug for Block {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Block")
            .field("buffer", &self.buffer)
            .field("key", &self.key)
            .field(
                "immediate_bytes_transferred",
                &self.immediate_bytes_transferred,
            )
            .field("result_tx", &self.result_tx)
            .field("result_rx", &self.result_rx)
            .finish()
    }
}

#[derive(Debug)]
pub(crate) struct PrepareBlock {
    // SAFETY: We do not participate in the Rust lifetime tracking system with Block/BlockStore.
    // The chain of allocate() -> PrepareBlock -> SealedBlock -> CompleteBlock -> release() is
    // what ensures that references are cleaned up properly, while lifetime tracking relies partly
    // on unsafe code as we hand over references to the operating system and lose track of them.
    // In other words, this is not a 'static reference but we pretend it is to avoid lifetimes.
    block: &'static mut Block,

    control: ControlNode,

    length: usize,
}

impl PrepareBlock {
    /// Obtains a buffer to fill with data for a write operation. You do not need to fill the entire
    /// buffer - call `set_length()` to indicate how many bytes you have written.
    pub fn buffer(&mut self) -> &mut [u8] {
        &mut self.block.buffer
    }

    /// Sets the length of the data in the buffer. This is used to indicate how many bytes you have
    /// written to the buffer. The value is ignored for read operations - they will always try to
    /// fill the entire buffer.
    ///
    /// # Panics
    ///
    /// Panics if the length is greater than the buffer size.
    pub fn set_length(&mut self, length: usize) {
        assert!(length <= self.block.buffer.len());

        self.length = length;
    }

    /// For seekable I/O primitives (e.g. files), sets the offset in the file where the operation
    /// should be performed.
    pub fn set_offset(&mut self, offset: usize) {
        self.block.overlapped.Anonymous.Anonymous.Offset = offset as u32;
        self.block.overlapped.Anonymous.Anonymous.OffsetHigh = (offset >> 32) as u32;
    }

    /// Executes an I/O operation, using the specified callback to pass the operation buffer and
    /// OVERLAPPED metadata structure to native OS functions.
    ///
    /// # Callback arguments
    ///
    /// 1. The buffer to be used for the operation. For reads, just pass it along to a native API.
    ///    For writes, fill it with data and constrain the size as needed before passing it on.
    /// 2. The OVERLAPPED structure to be used for the operation. Pass it along to the native API
    ///    without modification.
    /// 3. A mutable reference to a variable that is to receive the number of bytes transferred
    ///    if the I/O operation completes synchronously (i.e. with `Ok(())`). This value is ignored
    ///    if the I/O operation completes asynchronously (i.e. with `Err(ERROR_IO_PENDING)`).
    ///
    /// All arguments are only valid for the duration of the callback. Do not store them, even if
    /// they claim to have a 'static lifetime! It is a lie!
    pub async unsafe fn begin<F>(self, f: F) -> io::Result<CompleteBlock>
    where
        F: FnOnce(&'static mut [u8], *mut OVERLAPPED, &mut u32) -> io::Result<()>,
    {
        let result_rx = self
            .block
            .result_rx
            .take()
            .expect("block is always expected to have result rx when beginning I/O operation");

        // We clone the control node because we may need to release the block if the callback fails
        // or resurrect it immediately if the callback completes synchronously.
        let mut control_node = self.control.clone();

        let block_key = self.block.key;

        let (buffer, overlapped, immediate_bytes_transferred) = self.into_callback_arguments();

        match f(buffer, overlapped, immediate_bytes_transferred) {
            // The operation was started asynchronously. This is what we want to see.
            Err(io::Error::External(e)) if e.code() == ERROR_IO_PENDING.into() => {}

            // The operation completed synchronously. This means we will not get a completion
            // notification and must handle the result inline (because we set a flag saying this
            // when binding to the completion port).
            Ok(()) => {
                let mut block = control_node.complete_immediately(overlapped);
                let result_tx = block.result_tx();
                // We ignore the tx return value because the receiver may have dropped already.
                _ = result_tx.send(Ok(block));

                event!(
                    Level::TRACE,
                    message = "I/O operation completed immediately",
                    key = block_key,
                    length = immediate_bytes_transferred
                );
            }

            // Something went wrong. In this case, the operation block was not consumed by the OS.
            // We need to resurrect and free the block ourselves to avoid leaking it forever.
            Err(e) => {
                control_node.release(block_key);
                return Err(e);
            }
        }

        result_rx
            .await
            .expect("no expected code path drops the I/O block without signaling completion result")
    }

    fn into_callback_arguments(self) -> (&'static mut [u8], *mut OVERLAPPED, &'static mut u32) {
        // We do not want to run Drop - this is an intentional cleanupless handover.
        let this = ManuallyDrop::new(self);

        // SAFETY: This is just a manual move between compatible fields - no worries.
        let block = unsafe { ptr::read(&this.block) };

        (
            &mut block.buffer.as_mut()[0..this.length],
            &mut block.overlapped as *mut _,
            &mut block.immediate_bytes_transferred,
        )
    }
}

impl Drop for PrepareBlock {
    fn drop(&mut self) {
        self.control.release(self.block.key);
    }
}

#[derive(Debug)]
pub(crate) struct CompleteBlock {
    // SAFETY: We do not participate in the Rust lifetime tracking system with Block/BlockStore.
    // The chain of allocate() -> PrepareBlock -> SealedBlock -> CompleteBlock -> release() is
    // what ensures that references are cleaned up properly, while lifetime tracking relies partly
    // on unsafe code as we hand over references to the operating system and lose track of them.
    // In other words, this is not a 'static reference but we pretend it is to avoid lifetimes.
    block: &'static mut Block,

    control: ControlNode,

    length: usize,

    status: NTSTATUS,
}

impl CompleteBlock {
    /// References the data affected by the completed operation. Typically only meaningful for read
    /// operations but even for writes, the data remains available here. The buffer is sized to the
    /// data.
    pub fn buffer(&self) -> &[u8] {
        &self.block.buffer.as_ref()[..self.length]
    }

    pub(super) fn status(&self) -> NTSTATUS {
        self.status
    }

    /// # Panics
    ///
    /// If called more than once. You can only get the result_tx once.
    pub(super) fn result_tx(&mut self) -> oneshot::Sender<io::Result<CompleteBlock>> {
        self.block
            .result_tx
            .take()
            .expect("block is always expected to have result tx when completing I/O operation")
    }
}

impl Drop for CompleteBlock {
    fn drop(&mut self) {
        self.control.release(self.block.key);
    }
}
