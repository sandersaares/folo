use crate::{
    io::{self, OperationResultExt},
    net::{winsock, TcpConnection},
    rt::{current_async_agent, current_runtime, spawn_on_any, RemoteJoinHandle},
    util::OwnedHandle,
};
use core::slice;
use negative_impl::negative_impl;
use std::{future::Future, mem, num::NonZeroU16, rc::Rc};
use tracing::{event, Level};
use windows::Win32::Networking::WinSock::{
    bind, htons, listen, setsockopt, AcceptEx, GetAcceptExSockaddrs, WSAIoctl, WSASocketA, AF_INET,
    INADDR_ANY, IN_ADDR, IPPROTO_TCP, SIO_QUERY_RSS_PROCESSOR_INFO, SOCKADDR, SOCKADDR_IN, SOCKET,
    SOCKET_PROCESSOR_AFFINITY, SOCK_STREAM, SOL_SOCKET, SOMAXCONN, SO_UPDATE_ACCEPT_CONTEXT,
    WSAEACCES, WSAEOPNOTSUPP, WSA_FLAG_OVERLAPPED,
};

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
    /// returns an error (though an error response does imply that no further connections will be
    /// accepted and the server has shut down after a failed start).
    pub async fn build(self) -> io::Result<TcpServerHandle> {
        let port = self
            .port
            .ok_or_else(|| io::Error::InvalidOptions("port must be set".to_string()))?;
        let on_accept = self
            .on_accept
            .ok_or_else(|| io::Error::InvalidOptions("on_accept must be set".to_string()))?;

        let (startup_completed_tx, startup_completed_rx) = oneshot::channel();
        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        let join_handle = current_runtime::with(|x| {
            x.spawn_tcp_dispatcher(move || async move {
                TcpDispatcher::new(port, on_accept, startup_completed_tx, shutdown_rx)
                    .run()
                    .await
            })
        });

        match startup_completed_rx.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                event!(
                    Level::ERROR,
                    message = "TCP dispatcher startup failed - terminating",
                    error = e.to_string()
                );
                return Err(e);
            }
            Err(_) => {
                return Err(io::Error::Internal(
                    "TCP dispatcher died before reporting startup result".to_string(),
                ));
            }
        }

        // We create the server handle even if startup failed because we use it to command the stop
        // in case of a failed startup.
        let server_handle = TcpServerHandle::new(join_handle, shutdown_tx);

        event!(
            Level::INFO,
            message = "TCP server started",
            port = port.get()
        );

        Ok(server_handle)
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
/// the server, and may call `wait()` to wait for the server to complete its shutdown process.
pub struct TcpServerHandle {
    dispatcher_join_handle: RemoteJoinHandle<()>,

    // Consumed after signal is sent.
    dispatcher_shutdown_tx: Option<oneshot::Sender<()>>,
}

impl TcpServerHandle {
    fn new(
        dispatcher_join_handle: RemoteJoinHandle<()>,
        dispatcher_shutdown_tx: oneshot::Sender<()>,
    ) -> Self {
        Self {
            dispatcher_join_handle,
            dispatcher_shutdown_tx: Some(dispatcher_shutdown_tx),
        }
    }

    /// Stop the server. This will signal the server that it is to stop accepting new connections,
    /// and will start terminating existing connections. The method returns immediately. It may take
    /// some unspecified time for connection dispatch to actually stop and for ongoing connections
    /// to finish processing - the TCP server handle does not facilitate waiting for that.
    pub fn stop(&mut self) {
        let Some(dispatcher_shutdown_tx) = self.dispatcher_shutdown_tx.take() else {
            // Shutdown signal already sent.
            return;
        };

        // We ignore the result (maybe the remote side is already terminated).
        let _ = dispatcher_shutdown_tx.send(());
    }
}

#[negative_impl]
impl !Send for TcpServerHandle {}
#[negative_impl]
impl !Sync for TcpServerHandle {}

/// The TCP dispatcher manages the listen socket used to receive new connections. When a new
/// connection is received, it is dispatched to be handled by the user-defined callback on a
/// suitable worker, at which point the dispatcher is no longer involved.
struct TcpDispatcher<A, AF>
where
    A: Fn(TcpConnection) -> AF + Clone + Send + 'static,
    AF: Future<Output = io::Result<()>> + 'static,
{
    // We signal this once we are ready to receive connections (or when startup fails).
    // If this is an error, you can expect that this (or a similar) error will also be included in
    // the result of `TcpServerHandle::wait()`. Consumed on use.
    startup_completed_tx: Option<oneshot::Sender<io::Result<()>>>,

    // If we receive a message from here, it means we need to shut down. Consumed on use.
    shutdown_rx: Option<oneshot::Receiver<()>>,

    port: NonZeroU16,

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
        port: NonZeroU16,
        on_accept: A,
        startup_completed_tx: oneshot::Sender<io::Result<()>>,
        shutdown_rx: oneshot::Receiver<()>,
    ) -> Self {
        Self {
            port,
            on_accept,
            startup_completed_tx: Some(startup_completed_tx),
            shutdown_rx: Some(shutdown_rx),
        }
    }

    async fn run(&mut self) {
        let startup_result = match self.startup().await {
            Ok(x) => {
                _ = self.startup_completed_tx.take().expect("we have completed startup so the tx must still be there because this is the only thing that uses it").send(Ok(()));
                x
            }
            Err(e) => {
                // We ignore the result because it may be that nobody is listening anymore.
                event!(
                    Level::ERROR,
                    message = "TCP dispatcher startup failed",
                    error = e.to_string()
                );
                _ = self.startup_completed_tx.take().expect("we have completed startup so the tx must still be there because this is the only thing that uses it").send(Err(e));
                return;
            }
        };

        // Now we are up and running. Until we receive a shutdown command, we will keep accepting
        // new connections and dispatching them to be handled by the user-defined callback.
        self.run_accept_loop(startup_result).await;
    }

    async fn startup(&mut self) -> io::Result<StartedTcpDispatcher> {
        winsock::ensure_initialized();

        // NOTE: Measure overhead of these operations. In principle, they are synchronous, although
        // sockets also have some thread-specific behaviors so we may want to avoid using them from
        // multiple threads. However, if this can incur significant latency, maybe consider it?

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
            sin_port: unsafe { htons(self.port.get()) },
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

            winsock::to_io_result(listen(*listen_socket, SOMAXCONN as i32))?;
        };

        // Bind the socket to the I/O completion port so we can process I/O completions.
        current_async_agent::with_io(|io| {
            io.bind_io_primitive(&*listen_socket).unwrap();
        });

        Ok(StartedTcpDispatcher {
            listen_socket: Rc::new(listen_socket),
        })
    }

    async fn run_accept_loop(&mut self, startup_result: StartedTcpDispatcher) {
        let listen_socket = startup_result.listen_socket;

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
        // TODO: If a completion is ignored, won't that leave a dangling socket? Actually it will
        // end up with a panic: "no expected code path drops the I/O operation without signaling completion result: RecvError"
        // We probably need some Operation::on_cancel() callback to clean up the socket in that case, and to ignore drops otherwise.
        //
        // Because we are doing two things concurrently (accepting connections + awaiting orders),
        // we must use interior mutability or exclusive mutability only for one of these futures.
        // We cannot give an exclusive reference to both futures.

        // TODO: Should we enqueue multiple accepts concurrently? There is no reason to limit
        // ourselves to just one at a time if we can get more throughput by doing more of them.
        let mut accept_one_fut = Box::pin(
            AcceptOne {
                listen_socket: Rc::clone(&listen_socket),
            }
            .execute(),
        );

        let shutdown_received_fut = self.shutdown_rx.take().expect("we only take this once");

        let mut select_future = Some(futures::future::select(
            accept_one_fut,
            shutdown_received_fut,
        ));

        // Within each iteration, we will either accept a new connection or receive a command.
        // There is no specific guarantee about which one we may process first if both complete.
        // In realistic web services you need to shed load before shutting down anyway, so missing
        // the shutdown signal is a very theoretical concern only in artificial conditions.
        loop {
            match select_future
                .take()
                .expect("we always set this before looping")
                .await
            {
                futures::future::Either::Left((accept_result, new_shutdown_received_fut)) => {
                    if let Ok(socket) = accept_result {
                        // New connection accepted! Spawn as task and detach.
                        let on_accept_clone = self.on_accept.clone();

                        // TODO: Spawn on optimal processor, not a random one.
                        _ = spawn_on_any(move || async move {
                            current_async_agent::with_io(|io| {
                                io.bind_io_primitive(&*socket).unwrap()
                            });

                            let tcp_connection = TcpConnection { socket };

                            _ = (on_accept_clone)(tcp_connection).await;
                            // TODO: If callback result is error, report this error.
                        });
                    }

                    // TODO: Report error if not successfully accepted..

                    // We create a new accept task to accept the next connection and re-fill the
                    // select future with a brand new one.
                    accept_one_fut = Box::pin(
                        AcceptOne {
                            listen_socket: Rc::clone(&listen_socket),
                        }
                        .execute(),
                    );

                    select_future = Some(futures::future::select(
                        accept_one_fut,
                        new_shutdown_received_fut,
                    ));
                }
                futures::future::Either::Right((_, _)) => {
                    event!(Level::INFO, "TCP dispatcher shutting down",);

                    // We are shutting down! We will not accept any new connections and have already
                    // dropped the "accept one" logic on the ground (via discard in match arm). We
                    // return from the accept loop to also terminate the listen socket and enter the
                    // shutdown loop, which waits for active connections to end. Returning from this
                    // function closes the listen socket and cancels any connections queued on it.
                    return;
                }
            }
        }
    }
}

struct StartedTcpDispatcher {
    // This is an Rc because we need to share it between the worker itself and the "AcceptOne"
    // subtasks that it spawns. We use Rc to avoid the need for AcceptOne to take a reference to
    // the worker, which would at the very least conflict with the worker itself using an exclusive
    // reference to itself.
    listen_socket: Rc<OwnedHandle<SOCKET>>,
}

/// The state of a single "accept one connection" operation. We create this separate type to more
/// easily separate the resource management of the command-processing loop from the resource
/// management of the connection-accepting tasks.
struct AcceptOne {
    listen_socket: Rc<OwnedHandle<SOCKET>>,
}

impl AcceptOne {
    async fn execute(self) -> io::Result<OwnedHandle<SOCKET>> {
        // SAFETY: All we need to worry about here is cleanup, which we do via OwnedHandle.
        let connection_socket = unsafe {
            OwnedHandle::new(WSASocketA(
                AF_INET.0 as i32,
                SOCK_STREAM.0,
                IPPROTO_TCP.0,
                None,
                0,
                WSA_FLAG_OVERLAPPED,
            )?)
        };

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
        let buffer = io::PinnedBuffer::from_pool();

        // The data length in the buffer (if we were to want to use some) would be the buffer size
        // minus double of this (local + remote address).
        const ADDRESS_LENGTH: usize = mem::size_of::<SOCKADDR_IN>() + 16;

        assert!(buffer.len() >= ADDRESS_LENGTH * 2);

        let operation = current_async_agent::with_io(|io| io.new_operation(buffer));

        // SAFETY: We are required to pass the OVERLAPPED struct to the native I/O function to avoid
        // a resource leak. We do.
        let payload = unsafe {
            operation.begin(|buffer, overlapped, immediate_bytes_transferred| {
                if AcceptEx(
                    **self.listen_socket,
                    *connection_socket,
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
            })
        }
        .await
        .into_inner()?;

        let mut local_addr: *mut SOCKADDR = std::ptr::null_mut();
        let mut local_addr_len: i32 = 0;
        let mut remote_addr: *mut SOCKADDR = std::ptr::null_mut();
        let mut remote_addr_len: i32 = 0;

        // SAFETY: As long as we pass in valid pointers that match the AcceptEx call, we are good.
        unsafe {
            GetAcceptExSockaddrs(
                payload.as_slice().as_ptr() as *const _,
                0,
                ADDRESS_LENGTH as u32,
                ADDRESS_LENGTH as u32,
                &mut local_addr as *mut _,
                &mut local_addr_len as *mut _,
                &mut remote_addr as *mut _,
                &mut remote_addr_len as *mut _,
            )
        };

        // We need to refer to this via pointer, so let's copy it out to an lvalue first.
        let listen_socket = self.listen_socket.0;
        // SAFETY: The size is right, so creating the slice is OK. We only use it for the single
        // call on the next line, so no lifetime concerns - the slice is gone before the storage
        // goes away in all cases.
        let listen_socket_as_slice = unsafe {
            slice::from_raw_parts(
                mem::transmute(&listen_socket as *const _),
                mem::size_of::<usize>(),
            )
        };

        // This does some internal updates in the socket. The documentation is a little vague about
        // what this accomplishes but we might as well do it to be right and proper in all ways.
        winsock::to_io_result(unsafe {
            setsockopt(
                *connection_socket,
                SOL_SOCKET,
                SO_UPDATE_ACCEPT_CONTEXT,
                Some(listen_socket_as_slice),
            )
        })?;

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
                *connection_socket,
                SIO_QUERY_RSS_PROCESSOR_INFO,
                None,
                0,
                Some(&affinity_info as *const _ as *mut _),
                mem::size_of::<SOCKET_PROCESSOR_AFFINITY>() as u32,
                &mut bytes_returned as *mut _,
                // TODO: Should we do this asynchronously? Note that we are doing this on the dispatcher thread
                // whereas future use will be on an async worker thread - so a different completion port!
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
