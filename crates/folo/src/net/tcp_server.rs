use crate::{
    io::{self, Operation, OperationResultExt},
    net::{winsock, TcpConnection},
    rt::{current_async_agent, spawn, spawn_on_all, RemoteJoinHandle},
    util::OwnedHandle,
};
use core::slice;
use negative_impl::negative_impl;
use tracing::{event, Level};
use std::{future::Future, mem, num::NonZeroU16, rc::Rc};
use windows::Win32::Networking::WinSock::{
    bind, htons, listen, setsockopt, AcceptEx, GetAcceptExSockaddrs, WSASocketA, AF_INET,
    INADDR_ANY, IN_ADDR, IPPROTO_TCP, SOCKADDR, SOCKADDR_IN, SOCKET, SOCK_STREAM, SOL_SOCKET,
    SOMAXCONN, SO_UPDATE_ACCEPT_CONTEXT, WSA_FLAG_OVERLAPPED,
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

        let mut startup_completed_rxs = Vec::new();
        let mut shutdown_txs = Vec::new();

        let join_handles = spawn_on_all(|| {
            let on_accept_clone = on_accept.clone();

            let (shutdown_tx, shutdown_rx) = oneshot::channel();
            shutdown_txs.push(shutdown_tx);

            let (startup_completed_tx, startup_completed_rx) = oneshot::channel();
            startup_completed_rxs.push(startup_completed_rx);

            move || async move {
                TcpWorker::new(port, on_accept_clone, startup_completed_tx, shutdown_rx)
                    .run()
                    .await
            }
        });

        // We wait for all to complete startup (or for some to fail). It is expected that each
        // worker will signal failure also in its error reporting callback, so this error result
        // here is more of a "shortcut" to afford an early-out rather than to receive all the
        // details about what went wrong.
        let mut first_error = None;

        for rx in startup_completed_rxs {
            // There are two things that can go wrong here:
            // 1. The channel fails to receive (because the worker died before reporting startup).
            // 2. The worker reports a startup error.
            // We coalesce these into a single io::Result.

            match rx.await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => _ = first_error.get_or_insert(e),
                Err(_) => {
                    _ = first_error.get_or_insert(io::Error::Internal(
                        "TCP worker died before reporting startup result".to_string(),
                    ))
                }
            }
        }

        // We create the server handle even if startup failed because we use it to command the stop
        // in case of a failed startup.
        let mut server_handle = TcpServerHandle::new(join_handles, shutdown_txs);

        if let Some(e) = first_error {
            // If anything went wrong on startup, we tear it all down. It may be that some of the
            // workers started successfully and already accepted some requests, so this will wait
            // for those requests to end.
            server_handle.stop();
            server_handle.wait().await;

            return Err(e);
        }

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
    join_handles: Box<[RemoteJoinHandle<()>]>,

    // Consumed after signal is sent.
    shutdown_txs: Option<Vec<oneshot::Sender<()>>>,
}

impl TcpServerHandle {
    fn new(
        join_handles: Box<[RemoteJoinHandle<()>]>,
        shutdown_txs: Vec<oneshot::Sender<()>>,
    ) -> Self {
        Self {
            join_handles,
            shutdown_txs: Some(shutdown_txs),
        }
    }

    /// Stop the server. This will signal the server that it is to stop accepting new connections,
    /// and will start terminating existing connections. The method returns immediately. If you want
    /// to wait for the server to complete its shutdown process, call `wait()` after calling this.
    pub fn stop(&mut self) {
        let Some(shutdown_txs) = self.shutdown_txs.take() else {
            // Shutdown signal already sent.
            return;
        };

        for tx in shutdown_txs {
            // We ignore the result (maybe the remote side is already terminated).
            let _ = tx.send(());
        }
    }

    /// Wait for the server to complete its shutdown process. This will not return until the server
    /// has stopped accepting new connections, and all existing connections have been closed.
    pub async fn wait(&mut self) {
        for join_handle in &mut *self.join_handles {
            // We ignore the result because we are not interested in the result of the worker
            // shutting down, only in the knowledge that it has shut down.
            _ = join_handle.await;
        }
    }
}

#[negative_impl]
impl !Send for TcpServerHandle {}
#[negative_impl]
impl !Sync for TcpServerHandle {}

/// Current state of the TCP worker logic executing on a specific async worker thread.
///
/// When starting a TCP server, a TCP worker is spawned on every async worker thread.
struct TcpWorker<A, AF>
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

    // If we have received the shutdown command, we close up shop and wait for existing connections
    // to end. At present, there is no mechanism for a non-graceful shutdown - we give the
    // connections as long as they need. That may be forever (TCP connections that do not send a
    // single packet for DAYS are entirely valid and alive connections!).
    shutting_down: bool,

    port: NonZeroU16,

    // Whenever we receive a new connection, we spawn a new task with this callback to handle it.
    // We close the connection when this is done with the task. The worker observes the state of
    // active connections handed over to this so that it can know when it is safe to shut down.
    on_accept: A,
    // TODO: on_connection_error (callback if connection fails, probably without affecting other connections or general health)
    // TODO: on_worker_error (callback if worker-level operation fails and we probably will not receive more traffic on this worker)
    // TODO: on_handler_error (callback if on_accept fails; do we need this or just let on_accept worry about it?)
}

impl<A, AF> TcpWorker<A, AF>
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
            shutting_down: false,
        }
    }

    async fn run(&mut self) {
        let startup_result = match self.startup().await {
            Ok(x) => x,
            Err(e) => {
                // We ignore the result because it may be that nobody is listening anymore.
                _ = self.startup_completed_tx.take().expect("we have completed startup so the tx must still be there because this is the only thing that uses it").send(Err(e));
                return;
            }
        };

        // Now we are up and running. Until we receive a shutdown command, we will keep accepting new
        // connections and sending them off to be handled by the user-defined callback.
        self.run_accept_loop(startup_result).await;

        // We wait for all active connections to end before we declare ourselves done.
        self.run_shutdown_loop().await;
    }

    async fn startup(&mut self) -> io::Result<StartedTcpWorker> {
        winsock::ensure_initialized();

        // NOTE: Measure overhead of these operations. In principle, they are synchronous, although
        // sockets also have some thread-specific behaviors so we may want to avoid using them from
        // multiple threads. However, if this can incur significant latency, maybe consider it?

        // SAFETY: We are required to close the handle once we are done with it,
        // which we do via OwnedHandle that closes the handle on drop.
        let listen_socket = unsafe {
            OwnedHandle::new(WSASocketA(
                AF_INET.0 as i32,
                SOCK_STREAM.0 as i32,
                IPPROTO_TCP.0 as i32,
                None,
                0,
                WSA_FLAG_OVERLAPPED,
            )?)
        };

        // TODO: Set send/receiver buffer sizes.
        // TODO: Set "reuse address".
        // TODO: Set SIO_SET_PORT_SHARING_PER_PROC_SOCKET.

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

        Ok(StartedTcpWorker {
            listen_socket: Rc::new(listen_socket),
        })
    }

    async fn run_accept_loop(&mut self, startup_result: StartedTcpWorker) {
        let listen_socket = startup_result.listen_socket;

        // The act of accepting a connection is simply the first part of the lifecycle of a
        // TcpConnection, so we can think of this as just a very long drawn-out constructor.
        // On the other hand, accepting a connection does require use of resources owned by the
        // worker, so it is not an entirely isolated concept. We ensure proper resource management
        // by always polling the accept future directly from within the worker itself, so we do not
        // need to create any complex resource sharing scheme for e.g. the socket because we stop
        // polling if we release the resources. Any ongoing accept operations will be terminated
        // when the socket is closed, after which the I/O driver will process a completion that will
        // not be received by any awaiter any more and thus will be ignored. When we are shutting
        // down, this operation will simply be abandoned.
        //
        // Because we are doing two things concurrently (accepting connections + awaiting orders),
        // we must use interior mutability - we cannot give an exclusive reference to both futures.

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
            event!(
                Level::INFO,
                message = "awaiting connection or shutdown",
            );

            match select_future
                .take()
                .expect("we always set this before looping")
                .await
            {
                futures::future::Either::Left((accept_result, new_shutdown_received_fut)) => {
                    if let Ok(connection) = accept_result {
                        event!(
                            Level::INFO,
                            message = "accepted connection",
                        );

                        // New connection accepted! Spawn as task and detach.
                        let on_accept_clone = self.on_accept.clone();
                        _ = spawn(async move {
                            _ = (on_accept_clone)(connection).await;
                            // TODO: If callback result is error, report this error.
                        });
                    }

                    // TODO: Report error if not successfully accepted..

                    // TODO: We need to somehow somewhere remember it so we wait for it to complete
                    // on shutdown. We could do it with a semaphore but we do not have a semaphore.

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
                    event!(
                        Level::INFO,
                        message = "received shutdown",
                    );

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

    async fn run_shutdown_loop(&mut self) {
        // TODO: We do not today have a semaphore that we could use to wait for all connections and
        // we certainly do not want to maintain some giant list of tasks that we need to clean up.
    }
}

struct StartedTcpWorker {
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
    async fn execute(self) -> io::Result<TcpConnection> {
        // SAFETY: All we need to worry about here is cleanup, which we do via OwnedHandle.
        let connection_socket = unsafe {
            OwnedHandle::new(WSASocketA(
                AF_INET.0 as i32,
                SOCK_STREAM.0 as i32,
                IPPROTO_TCP.0 as i32,
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

        let operation = current_async_agent::with_io(|io| -> io::Result<Operation> {
            io.bind_io_primitive(&*connection_socket)?;
            Ok(io.new_operation(buffer))
        })?;

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

        // The new socket is connected and ready! Finally!
        Ok(TcpConnection {
            socket: connection_socket,
        })
    }
}

#[negative_impl]
impl<A, AF> !Send for TcpWorker<A, AF>
where
    A: Fn(TcpConnection) -> AF + Clone + Send + 'static,
    AF: Future<Output = io::Result<()>> + 'static,
{
}
#[negative_impl]
impl<A, AF> !Sync for TcpWorker<A, AF>
where
    A: Fn(TcpConnection) -> AF + Clone + Send + 'static,
    AF: Future<Output = io::Result<()>> + 'static,
{
}
