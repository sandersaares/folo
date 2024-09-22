use crate::{
    io::{self, OperationResultSharedExt},
    mem::isolation::Shared,
    net::{winsock, TcpConnection},
    rt::{current_async_agent, current_runtime, spawn, RemoteJoinHandle, SynchronousTaskType},
    windows::OwnedHandle,
};
use core::slice;
use futures::{
    future::{select, Either},
    stream::FuturesUnordered,
    StreamExt,
};
use negative_impl::negative_impl;
use std::{future::Future, mem, num::NonZeroU16, sync::Arc};
use tracing::{event, Level};
use windows::Win32::Networking::WinSock::{
    bind, htons, listen, setsockopt, AcceptEx, GetAcceptExSockaddrs, WSAIoctl, WSASocketA, AF_INET,
    INADDR_ANY, IN_ADDR, IPPROTO_TCP, SIO_QUERY_RSS_PROCESSOR_INFO, SOCKADDR, SOCKADDR_IN, SOCKET,
    SOCKET_PROCESSOR_AFFINITY, SOCK_STREAM, SOL_SOCKET, SO_UPDATE_ACCEPT_CONTEXT, WSAEACCES,
    WSAEOPNOTSUPP, WSA_FLAG_OVERLAPPED,
};

#[derive(Debug)]
pub struct TcpServerBuilder<A, AF>
where
    A: Fn(TcpConnection) -> AF + Clone + Send + 'static,
    AF: Future<Output = io::Result<()>> + 'static,
{
    port: Option<NonZeroU16>,
    on_accept: Option<A>,
}

impl<A, AF> TcpServerBuilder<A, AF>
where
    A: Fn(TcpConnection) -> AF + Clone + Send + 'static,
    AF: Future<Output = io::Result<()>> + 'static,
{
    pub fn new() -> Self {
        Self {
            port: None,
            on_accept: None,
        }
    }

    pub fn port(mut self, port: NonZeroU16) -> Self {
        self.port = Some(port);
        self
    }

    /// Sets the function to call when a new connection is accepted. The function may be called
    /// from any async task worker thread and any number of times concurrently.
    ///
    /// The connection will be closed when the provided TcpConnection is dropped.
    pub fn on_accept(mut self, callback: A) -> Self {
        self.on_accept = Some(callback);
        self
    }

    /// Builds the TCP server and starts accepting new connections.
    ///
    /// The startup process is gradual and connections may be received even before the result of
    /// this function is returned. Connections may even be received if this function ultimately
    /// returns an error.
    pub async fn build(self) -> io::Result<TcpServerHandle> {
        let port = self
            .port
            .ok_or_else(|| io::Error::InvalidOptions("port must be set".to_string()))?;
        let on_accept = self
            .on_accept
            .clone()
            .ok_or_else(|| io::Error::InvalidOptions("on_accept must be set".to_string()))?;

        let listen_socket = Arc::new(Self::create_listen_socket(port).await?);

        // We spawn a dispatcher on every async worker.
        let mut shutdown_txs = Vec::new();

        let join_handles = current_runtime::with(|runtime| {
            runtime.spawn_on_all(|| {
                // This code runs on the originating thread and allows us to wire up per-worker.
                let (shutdown_tx, shutdown_rx) = oneshot::channel();

                shutdown_txs.push(shutdown_tx);

                let on_accept_clone = on_accept.clone();
                let listen_socket_clone = Arc::clone(&listen_socket);

                || async move {
                    // This code will run on each worker thread.
                    TcpDispatcher::new(listen_socket_clone, on_accept_clone, shutdown_rx)
                        .run()
                        .await
                }
            })
        });

        event!(
            Level::DEBUG,
            message = "TCP server started",
            port = port.get()
        );

        Ok(TcpServerHandle::new(join_handles, shutdown_txs))
    }

    async fn create_listen_socket(port: NonZeroU16) -> io::Result<OwnedHandle<SOCKET>> {
        winsock::ensure_initialized();

        // SAFETY: We are required to close the handle once we are done with it,
        // which we do via OwnedHandle that closes the handle on drop.
        let listen_socket = unsafe {
            OwnedHandle::new(WSASocketA(
                AF_INET.0 as i32,
                SOCK_STREAM.0,
                IPPROTO_TCP.0,
                None,
                0,
                WSA_FLAG_OVERLAPPED,
            )?)
        };

        // TODO: Set send/receiver buffer sizes (will be inherited by spawned connections).

        let mut addr = IN_ADDR::default();
        addr.S_un.S_addr = INADDR_ANY;

        let socket_addr = SOCKADDR_IN {
            sin_family: AF_INET,
            // SAFETY: Nothing unsafe here, just an FFI call.
            sin_port: unsafe { htons(port.get()) },
            sin_addr: addr,
            sin_zero: [0; 8],
        };

        // SAFETY: All we need to be concerned about is passing in valid arguments, which we do.
        unsafe {
            winsock::to_io_result(bind(
                *listen_socket,
                &socket_addr as *const _ as *const _,
                mem::size_of::<SOCKADDR_IN>() as i32,
            ))?;

            // A raw value for the queue length must be wrapped in the SOMAXCONN_HINT macro,
            // which really is just negation - a negative value means "use the absolute value".
            winsock::to_io_result(listen(*listen_socket, -PENDING_CONNECTION_LIMIT))?;
        };

        // Bind the socket to the I/O completion port so we can process I/O completions.
        current_async_agent::with_io_shared(|io| {
            io.bind_io_primitive(&*listen_socket).unwrap();
        });

        event!(Level::TRACE, "created TCP socket for accepting connections");
        Ok(listen_socket)
    }
}

impl<A, AF> Default for TcpServerBuilder<A, AF>
where
    A: Fn(TcpConnection) -> AF + Clone + Send + 'static,
    AF: Future<Output = io::Result<()>> + 'static,
{
    fn default() -> Self {
        Self::new()
    }
}

#[negative_impl]
impl<A, AF> !Send for TcpServerBuilder<A, AF>
where
    A: Fn(TcpConnection) -> AF + Clone + Send + 'static,
    AF: Future<Output = io::Result<()>> + 'static,
{
}
#[negative_impl]
impl<A, AF> !Sync for TcpServerBuilder<A, AF>
where
    A: Fn(TcpConnection) -> AF + Clone + Send + 'static,
    AF: Future<Output = io::Result<()>> + 'static,
{
}

/// Control surface to operate the TCP server. The lifetime of this is not directly connected to the
/// TCP server. Dropping this will not stop the server - you must explicitly call `stop()` to stop
/// the server.
pub struct TcpServerHandle {
    dispatcher_join_handles: Box<[RemoteJoinHandle<()>]>,
    dispatcher_shutdown_txs: Vec<oneshot::Sender<()>>,
}

impl TcpServerHandle {
    fn new(
        dispatcher_join_handles: Box<[RemoteJoinHandle<()>]>,
        dispatcher_shutdown_txs: Vec<oneshot::Sender<()>>,
    ) -> Self {
        Self {
            dispatcher_join_handles,
            dispatcher_shutdown_txs,
        }
    }

    /// Stop the server. This will signal the server that it is to stop accepting new connections,
    /// and will start terminating existing connections. The method returns immediately. It may take
    /// some unspecified time for connection dispatch to actually stop and for ongoing connections
    /// to finish processing - the TCP server handle does not facilitate waiting for that.
    pub fn stop(&mut self) {
        event!(Level::TRACE, "signaling TCP dispatcher tasks to stop");

        for tx in self.dispatcher_shutdown_txs.drain(..) {
            // We ignore the result (maybe the remote side is already terminated).
            let _ = tx.send(());
        }
    }
}

#[negative_impl]
impl !Send for TcpServerHandle {}
#[negative_impl]
impl !Sync for TcpServerHandle {}

// A small value here seems to yield better overall throughput as connections end up more
// distributed over different threads.
const CONCURRENT_ACCEPT_OPERATIONS_PER_DISPATCHER: usize = 8;

// The default assigned by the OS seems to be around 128, which is not enough under high load.
const PENDING_CONNECTION_LIMIT: i32 = 4096;

/// The TCP dispatcher manages the listen socket used to receive new connections. When a new
/// connection is received, it is dispatched to be handled by the user-defined callback on a
/// suitable worker, at which point the dispatcher is no longer involved.
struct TcpDispatcher<A, AF>
where
    A: Fn(TcpConnection) -> AF + Clone + Send + 'static,
    AF: Future<Output = io::Result<()>> + 'static,
{
    // If we receive a message from here, it means we need to shut down. Consumed on use.
    shutdown_rx: Option<oneshot::Receiver<()>>,

    listen_socket: Arc<OwnedHandle<SOCKET>>,

    // Whenever we receive a new connection, we spawn a new task with this callback to handle it.
    // Once we schedule a task to call this, the dispatcher forgets about the connection - anything
    // that happens afterward is the responsibility of the TcpConnection to organize.
    on_accept: A,
    // TODO: on_connection_error (callback if connection fails, probably without affecting other connections or general health)
    // TODO: on_worker_error (callback if worker-level operation fails and we probably will not receive more traffic on this worker)
    // TODO: on_handler_error (callback if on_accept fails; do we need this or just let on_accept worry about it?)
}

impl<A, AF> TcpDispatcher<A, AF>
where
    A: Fn(TcpConnection) -> AF + Clone + Send + 'static,
    AF: Future<Output = io::Result<()>> + 'static,
{
    fn new(
        listen_socket: Arc<OwnedHandle<SOCKET>>,
        on_accept: A,
        shutdown_rx: oneshot::Receiver<()>,
    ) -> Self {
        Self {
            listen_socket,
            on_accept,
            shutdown_rx: Some(shutdown_rx),
        }
    }

    async fn run(&mut self) {
        // The act of accepting a connection is simply the first part of the lifecycle of a
        // TcpConnection, so we can think of this as just a very long drawn-out constructor.
        // On the other hand, accepting a connection does require use of resources owned by the
        // dispatcher, so it is not an entirely isolated concept. We ensure proper resource
        // management by always polling the accept future directly from within the dispatcher
        // itself, so we do not need to create any complex resource sharing scheme for e.g. the
        // socket because we stop polling if we release the resources. Any ongoing accept operations
        // will be terminated when the socket is closed, after which the I/O driver will process a
        // completion that will not be received by any awaiter any more and thus will be ignored.
        // When we are shutting down, this operation will simply be abandoned.
        //
        // TODO: If a completion is ignored, won't that leave a dangling socket?
        // Do we need some Operation::on_cancel() callback to clean up the socket in that case?
        //
        // Because we are doing two things concurrently (accepting connections + awaiting orders),
        // we must use interior mutability or exclusive mutability only for one of these futures.
        // We cannot give an exclusive reference to both futures.

        // All the ongoing accept operations. We will keep this filled up to the limit of
        // CONCURRENT_ACCEPT_OPERATIONS, so whenever some get accepted, more accepts get queued.
        let mut accept_futures = Box::pin(FuturesUnordered::new());

        // If this completes, we shut down the dispatcher.
        let mut shutdown_received_future = self.shutdown_rx.take().expect("we only take this once");

        loop {
            while accept_futures.len() < CONCURRENT_ACCEPT_OPERATIONS_PER_DISPATCHER {
                accept_futures.push(
                    AcceptOne {
                        listen_socket: Arc::clone(&self.listen_socket),
                    }
                    .execute(),
                );
            }

            event!(
                Level::TRACE,
                message = "waiting for new connection or shutdown",
                accept_futures_len = accept_futures.len(),
            );

            let accept_result = match select(accept_futures.next(), shutdown_received_future).await
            {
                Either::Left((Some(accept_result), new_shutdown_received_future)) => {
                    shutdown_received_future = new_shutdown_received_future;
                    accept_result
                }
                Either::Left((None, _)) => {
                    panic!("accept_futures stream ended unexpectedly - we are supposed to refill it before checking");
                }
                Either::Right(_) => {
                    event!(Level::DEBUG, "TCP dispatcher shutting down",);
                    // We will not accept any new connections. The existing "accept one" operations
                    // will be dropped soon and any pending I/O will likewise be canceled as soon
                    // as the OwnedHandle is dropped and the socket gets closed.
                    return;
                }
            };

            event!(
                Level::TRACE,
                message = "detected incoming TCP connection (or error)",
                accept_futures_len = accept_futures.len(),
                ?accept_result
            );

            let Ok(connection_socket) = accept_result else {
                event!(
                    Level::ERROR,
                    message = "error accepting new connection - ignoring",
                    error = accept_result.unwrap_err().to_string()
                );
                // TODO: Report error to callback if not successfully accepted..
                continue;
            };

            // New connection accepted! Spawn as task and detach.
            let on_accept_clone = self.on_accept.clone();

            // We spawn it on the same async worker that caught the connection.
            _ = spawn(async move {
                current_async_agent::with_io(|io| {
                    io.bind_io_primitive(&**connection_socket).unwrap()
                });

                let tcp_connection = TcpConnection {
                    socket: connection_socket,
                };

                _ = (on_accept_clone)(tcp_connection).await;

                // TODO: If callback result is error, report this error.
            });
        }
    }
}

/// The state of a single "accept one connection" operation. We create this separate type to more
/// easily separate the resource management of the command-processing loop from the resource
/// management of the connection-accepting tasks.
struct AcceptOne {
    listen_socket: Arc<OwnedHandle<SOCKET>>,
}

impl AcceptOne {
    async fn execute(self) -> io::Result<Arc<OwnedHandle<SOCKET>>> {
        event!(Level::TRACE, "listening for an incoming connection");

        // Creating the socket is an expensive synchronous operation, so do it on a synchronous
        // worker thread.
        let connection_socket = current_runtime::with(move |runtime| {
            runtime.spawn_sync_on_any(
                SynchronousTaskType::Syscall,
                move || -> io::Result<OwnedHandle<SOCKET>> {
                    event!(
                        Level::TRACE,
                        "creating fresh socket for next incoming connection"
                    );

                    // SAFETY: All we need to worry about here is cleanup, which we do via OwnedHandle.
                    Ok(unsafe {
                        OwnedHandle::new(WSASocketA(
                            AF_INET.0 as i32,
                            SOCK_STREAM.0,
                            IPPROTO_TCP.0,
                            None,
                            0,
                            WSA_FLAG_OVERLAPPED,
                        )?)
                    })
                },
            )
        })
        .await?;

        // The connection will be accepted on an arbitrary worker thread and delivered back to the
        // dispatcher thread that asked for one to be accepted. Hence, management of the handle
        // must be thread-safely performed via Arc.
        let connection_socket = Arc::new(connection_socket);

        event!(Level::TRACE, "socket created for next incoming connection");

        // NOTE: AcceptEx supports immediately pasting the first block of received data in here,
        // which may provide a performance boost when accepting the connection. This is optional
        // and for now we disable this via setting dwReceiveDataLength to 0.
        //
        // Contents (not in order):
        // * Local address
        // * Remote address
        // * (Optional) first block of data received
        //
        // Reference of relevant length calculations:
        // bRetVal = lpfnAcceptEx(ListenSocket, AcceptSocket, lpOutputBuf,
        //      outBufLen - ((sizeof (sockaddr_in) + 16) * 2),
        //      sizeof (sockaddr_in) + 16, sizeof (sockaddr_in) + 16,
        //      &dwBytes, &olOverlap);
        let buffer = io::Buffer::<Shared>::from_pool();

        // The data length in the buffer (if we were to want to use some) would be the buffer size
        // minus double of this (local + remote address).
        const ADDRESS_LENGTH: usize = mem::size_of::<SOCKADDR_IN>() + 16;

        assert!(buffer.len() >= ADDRESS_LENGTH * 2);

        // NOTE: This is an operation on the **listen socket**, not on the connection socekt, so it
        // is bound to the completion port of the listen socket. Note that we have not yet bound the
        // connection socket to any completion port.
        let accept_operation = current_async_agent::with_io_shared(|io| io.new_operation(buffer));

        event!(Level::TRACE, "waiting for incoming connection to arrive");

        // SAFETY: We are required to pass the OVERLAPPED struct to the native I/O function to avoid
        // a resource leak. We do.
        let accept_result = unsafe {
            accept_operation.begin({
                let listen_socket = Arc::clone(&self.listen_socket);
                let connection_socket = Arc::clone(&connection_socket);

                move |buffer, overlapped, immediate_bytes_transferred| {
                    if AcceptEx(
                        **listen_socket,
                        **connection_socket,
                        buffer.as_mut_ptr() as *mut _,
                        0,
                        ADDRESS_LENGTH as u32,
                        ADDRESS_LENGTH as u32,
                        immediate_bytes_transferred,
                        overlapped,
                    )
                    .as_bool()
                    {
                        Ok(())
                    } else {
                        // The docs say it sets ERROR_IO_PENDING in WSAGetLastError. We do not strictly
                        // speaking read that but it also seems to set ERROR_IO_PENDING in the regular
                        // GetLastError, apparently, so all is well.
                        Err(windows::core::Error::from_win32().into())
                    }
                }
            })
        }
        .await
        .into_inner()?;

        event!(
            Level::TRACE,
            "incoming connection accepted; identifying addresses"
        );

        let mut local_addr: *mut SOCKADDR = std::ptr::null_mut();
        let mut local_addr_len: i32 = 0;
        let mut remote_addr: *mut SOCKADDR = std::ptr::null_mut();
        let mut remote_addr_len: i32 = 0;

        // This function will replace the pointer above to point to the actual data in question.
        // Calling this is optional - we do not currently use this data, but we would in a real app.
        // SAFETY: As long as we pass in valid pointers that match the AcceptEx call, we are good.
        unsafe {
            GetAcceptExSockaddrs(
                accept_result.as_slice().as_ptr() as *const _,
                0,
                ADDRESS_LENGTH as u32,
                ADDRESS_LENGTH as u32,
                &mut local_addr as *mut _,
                &mut local_addr_len as *mut _,
                &mut remote_addr as *mut _,
                &mut remote_addr_len as *mut _,
            )
        };

        // This post-processing is synchronous work that is not free, so move it to a synchronous
        // worker thread.
        let listen_socket = Arc::clone(&self.listen_socket);

        event!(
            Level::TRACE,
            "configuring socket for incoming connection (part 1)"
        );

        let _affinity_info = current_runtime::with({
            let connection_socket = Arc::clone(&connection_socket);

            move |runtime| {
            runtime.spawn_sync_on_any(
                SynchronousTaskType::Syscall,
                move || -> io::Result<SOCKET_PROCESSOR_AFFINITY> {
                    event!(Level::TRACE, "configuring socket for incoming connection (part 2)");

                    // We need to refer to this via pointer, so let's copy it out to a place first.
                    let listen_socket = listen_socket.0;

                    // SAFETY: The size is right, so creating the slice is OK. We only use it for the single
                    // call on the next line, so no lifetime concerns - the slice is gone before the storage
                    // goes away in all cases.
                    let listen_socket_as_slice = unsafe {
                        slice::from_raw_parts(
                            mem::transmute::<*const usize, *const u8>(&listen_socket as *const _),
                            mem::size_of::<usize>(),
                        )
                    };

                    // This does some internal updates in the socket. The documentation is a little vague about
                    // what this accomplishes but we might as well do it to be right and proper in all ways.
                    winsock::to_io_result(unsafe {
                        setsockopt(
                            **connection_socket,
                            SOL_SOCKET,
                            SO_UPDATE_ACCEPT_CONTEXT,
                            Some(listen_socket_as_slice),
                        )
                    })?;

                    // Inspect processor affinity configuration. We do not currently use this information but
                    // one day we might. We execute this in synchronous mode because we cannot bind the socket
                    // to our completion port - we are on the TCP dispatcher thread and the socket actually
                    // needs to be bound to the completion port of the thread where we dispatch it to. Which we
                    // do not know yet. So synchronous it is, on some synchronous worker thread.
                    let affinity_info: SOCKET_PROCESSOR_AFFINITY = SOCKET_PROCESSOR_AFFINITY::default();
                    let mut bytes_returned: u32 = 0;

                    // Prerequisite:
                    // 1) adapter must support RSS and have it enabled (e.g. not loopback)
                    // 2) adapter must be connected to peer
                    //
                    // Errors encountered if above conditions are not met:
                    // 10013 - WSAEACCES - An attempt was made to access a socket in a way forbidden by its access permissions.
                    // 10045 - WSAEOPNOTSUPP - The attempted operation is not supported for the type of object referenced.
                    //
                    // Output will be something like:
                    // SOCKET_PROCESSOR_AFFINITY { Processor: PROCESSOR_NUMBER { Group: 0, Number: 18, Reserved: 0 }, NumaNodeId: 0, Reserved: 0 }
                    // Processor number will be different for different connections.
                    // Not all processors will be used - typically only 16 processors are used for low level I/O.
                    // NB! This data may change during life of a connection - it is not fixed!
                    let affinity_result = unsafe {
                        winsock::to_io_result(WSAIoctl(
                            **connection_socket,
                            SIO_QUERY_RSS_PROCESSOR_INFO,
                            None,
                            0,
                            Some(&affinity_info as *const _ as *mut _),
                            mem::size_of::<SOCKET_PROCESSOR_AFFINITY>() as u32,
                            &mut bytes_returned as *mut _,
                            None,
                            None,
                        ))
                    };

                    match affinity_result {
                        Ok(()) => {
                            event!(Level::TRACE, message = "RSS processor info for new connection", affinity_info = ?affinity_info);
                        }
                        Err(io::Error::Winsock { detail, .. })
                            if detail == WSAEOPNOTSUPP || detail == WSAEACCES =>
                        {
                            event!(
                                Level::TRACE,
                                message =
                                    "RSS not supported/enabled on network adapter used for new connection"
                            );
                        }
                        Err(e) => {
                            event!(
                                Level::ERROR,
                                message = "error querying RSS processor info for new connection",
                                error = e.to_string()
                            );
                        }
                    }

                    event!(Level::TRACE, "socket configured for incoming connection");

                    Ok(affinity_info)
                },
            )
        }}).await?;

        // The new socket is connected and ready! Finally!
        // TODO: Attach RSS info so it can actually be used for smart dispatch decisions.
        Ok(connection_socket)
    }
}

#[negative_impl]
impl<A, AF> !Send for TcpDispatcher<A, AF>
where
    A: Fn(TcpConnection) -> AF + Clone + Send + 'static,
    AF: Future<Output = io::Result<()>> + 'static,
{
}
#[negative_impl]
impl<A, AF> !Sync for TcpDispatcher<A, AF>
where
    A: Fn(TcpConnection) -> AF + Clone + Send + 'static,
    AF: Future<Output = io::Result<()>> + 'static,
{
}
