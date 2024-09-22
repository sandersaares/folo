use crate::{
    io::{self, Buffer, OperationResultFuture},
    mem::isolation::Isolated,
    net::winsock,
    rt::{current_async_agent, current_runtime, RemoteJoinHandle, SynchronousTaskType},
    windows::OwnedHandle,
};
use negative_impl::negative_impl;
use pin_project::pin_project;
use std::{future::Future, sync::Arc, task::Poll};
use windows::{
    core::PSTR,
    Win32::Networking::WinSock::{WSARecv, WSASend, WSASendDisconnect, SOCKET, WSABUF},
};

#[derive(Debug)]
pub struct TcpConnection {
    // This is an Arc because some operations (e.g. shutdown) involve synchronous logic and
    // therefore we must share the socket between multiple threads.
    pub(super) socket: Arc<OwnedHandle<SOCKET>>,
}

impl TcpConnection {
    /// Receives the next buffer of data.
    ///
    /// The buffer will be returned in the result with the active region set to the bytes read, with
    /// a length of 0 if the connection was closed.
    ///
    /// You should not call this multiple times concurrently because there is no guarantee that the
    /// continuations will be called in a particular order.
    pub fn receive(&mut self, buffer: Buffer<Isolated>) -> OperationResultFuture {
        socket_receive(Arc::clone(&self.socket), buffer)
    }

    /// Sends a buffer of data to the peer.
    ///
    /// The buffer will be returned in the result to allow reuse.
    ///
    /// You may call this multiple times concurrently. The buffers will be sent in the order they
    /// are submitted.
    pub fn send(&mut self, buffer: Buffer<Isolated>) -> OperationResultFuture {
        socket_send(Arc::clone(&self.socket), buffer)
    }

    /// Performs a graceful shutdown of the connection, allowing time for all pending data transfers
    /// to complete. After this, you may drop the object and be assured that no data was lost in
    /// transit - this guarantee does not exist without calling the shutdown method.
    ///
    /// An error result indicates that a graceful shutdown was not possible.
    pub fn shutdown(&mut self) -> ShutdownFuture {
        ShutdownFuture::new(Arc::clone(&self.socket))
    }
}

#[negative_impl]
impl !Send for TcpConnection {}
#[negative_impl]
impl !Sync for TcpConnection {}

fn socket_receive(
    socket: Arc<OwnedHandle<SOCKET>>,
    buffer: Buffer<Isolated>,
) -> OperationResultFuture {
    // SAFETY: We are required to pass the OVERLAPPED pointer to the completion routine. We do.
    unsafe {
        current_async_agent::with_io(|io| io.new_operation(buffer)).begin(
            move |buffer, overlapped, immediate_bytes_transferred| {
                let wsabuf = WSABUF {
                    len: buffer.len() as u32,
                    buf: PSTR::from_raw(buffer.as_mut_ptr()),
                };

                let wsabufs = [wsabuf];
                let mut flags: u32 = 0;

                winsock::to_io_result(WSARecv(
                    **socket,
                    &wsabufs,
                    Some(immediate_bytes_transferred as *mut u32),
                    &mut flags as *mut u32,
                    Some(overlapped),
                    None,
                ))
            },
        )
    }
}

fn socket_send(
    socket: Arc<OwnedHandle<SOCKET>>,
    buffer: Buffer<Isolated>,
) -> OperationResultFuture {
    // SAFETY: We are required to pass the OVERLAPPED pointer to the completion routine. We do.
    unsafe {
        current_async_agent::with_io(|io| io.new_operation(buffer)).begin(
            move |buffer, overlapped, immediate_bytes_transferred| {
                let wsabuf = WSABUF {
                    len: buffer.len() as u32,
                    buf: PSTR::from_raw(buffer.as_mut_ptr()),
                };

                let wsabufs = [wsabuf];

                winsock::to_io_result(WSASend(
                    **socket,
                    &wsabufs,
                    Some(immediate_bytes_transferred as *mut u32),
                    0,
                    Some(overlapped),
                    None,
                ))
            },
        )
    }
}

#[derive(Debug)]
#[pin_project]
pub struct ShutdownFuture {
    socket: Arc<OwnedHandle<SOCKET>>,

    #[pin]
    state: ShutdownState,
}

#[derive(Debug)]
#[pin_project(project = ShutdownStateProj)]
enum ShutdownState {
    // 1) Tell the OS that we will not send any more data. This sets the wheels in motion.
    Disconnecting(#[pin] RemoteJoinHandle<io::Result<()>>),

    // 2) Try to read more data from the peer and expect to see a 0-byte result (EOF).
    //    Actually getting some data here is an error - this indicates that the caller did not
    //    fully process incoming data before calling shutdown. Potentially valid for some types
    //    of connections (e.g. an infinite stream of values) but not typical for web APIs.
    WaitingForEndOfStream(#[pin] OperationResultFuture),

    // 3) Done! Once we get the EOF, we can be sure that the peer has received all of our data
    //    and our FIN has been acknowledged, so no more activity can occur on the wire
    //    Should not be polled again in this state - we have returned Ready already.
    Completed,
}

impl ShutdownFuture {
    fn new(socket: Arc<OwnedHandle<SOCKET>>) -> Self {
        let disconnecting = current_runtime::with({
            let socket = Arc::clone(&socket);

            |runtime| {
                // We send this as high priority as it is a "release resources" operation that may
                // have efficiency benefits to do as soon as possible.
                runtime.spawn_sync(SynchronousTaskType::HighPrioritySyscall, move || {
                    // SAFETY: Socket liveness is ensured by our shared ownership of the socket handle.
                    winsock::to_io_result(unsafe { WSASendDisconnect(**socket, None) })
                })
            }
        });

        Self {
            socket,
            state: ShutdownState::Disconnecting(disconnecting),
        }
    }
}

impl Future for ShutdownFuture {
    type Output = io::Result<()>;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let mut this = self.project();

        // We loop for fallthrough between some of the states.
        loop {
            match this.state.as_mut().project() {
                ShutdownStateProj::Disconnecting(join_handle) => {
                    match join_handle.poll(cx) {
                        Poll::Ready(Ok(())) => {}
                        Poll::Ready(Err(e)) => {
                            *this.state = ShutdownState::Completed;
                            return Poll::Ready(Err(e));
                        }
                        Poll::Pending => return Poll::Pending,
                    };

                    let receive_future =
                        socket_receive(Arc::clone(this.socket), Buffer::<Isolated>::from_pool());

                    *this.state = ShutdownState::WaitingForEndOfStream(receive_future);
                    // We fall through to the next state here.
                }
                ShutdownStateProj::WaitingForEndOfStream(receive_future) => {
                    match receive_future.poll(cx) {
                        Poll::Ready(Ok(buffer)) => {
                            if !buffer.is_empty() {
                                *this.state = ShutdownState::Completed;
                                return Poll::Ready(Err(io::Error::LogicError("socket received data when shutting down - this may be an error depending on the communication protocol in use".to_string())));
                            }

                            *this.state = ShutdownState::Completed;
                            return Poll::Ready(Ok(()));
                        }
                        Poll::Ready(Err(e)) => {
                            *this.state = ShutdownState::Completed;
                            return Poll::Ready(Err(e.into_inner()));
                        }
                        Poll::Pending => {
                            return Poll::Pending;
                        }
                    };
                }
                // The Future API allows us to panic in this situation.
                ShutdownStateProj::Completed => panic!("ShutdownFuture polled after completion"),
            }
        }
    }
}

#[negative_impl]
impl !Send for ShutdownFuture {}
#[negative_impl]
impl !Sync for ShutdownFuture {}
