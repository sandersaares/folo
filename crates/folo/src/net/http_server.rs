use crate::io::{Buffer, OperationResultSharedExt, Shared};
use crate::net::HttpContext;
use crate::rt::{current_async_agent, current_runtime, spawn, RemoteJoinHandle};
use crate::util::ThreadSafe;
use crate::{io, net::http_sys};
use futures::future::{select, Either};
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use negative_impl::negative_impl;
use scopeguard::{guard, ScopeGuard};
use std::ffi::OsStr;
use std::mem;
use std::os::windows::ffi::OsStrExt;
use std::sync::Arc;
use std::{future::Future, num::NonZeroU16};
use tracing::{event, Level};
use windows::core::PCWSTR;
use windows::Win32::Foundation::HANDLE;
use windows::Win32::Networking::HttpServer::{
    HttpAddUrlToUrlGroup, HttpCloseRequestQueue, HttpCloseServerSession, HttpCloseUrlGroup,
    HttpCreateRequestQueue, HttpCreateServerSession, HttpCreateUrlGroup, HttpReceiveHttpRequest,
    HttpServerBindingProperty, HttpServerQueueLengthProperty, HttpSetRequestQueueProperty,
    HttpSetUrlGroupProperty, HttpTerminate, HTTP_BINDING_INFO, HTTP_PROPERTY_FLAGS,
    HTTP_RECEIVE_HTTP_REQUEST_FLAGS, HTTP_REQUEST_V2,
};
use windows::Win32::Networking::HttpServer::{
    HttpInitialize, HTTPAPI_VERSION, HTTP_INITIALIZE_SERVER,
};

#[derive(Debug)]
pub struct HttpServerBuilder<A, AF>
where
    A: Fn(HttpContext) -> AF + Clone + Send + 'static,
    AF: Future<Output = io::Result<()>> + 'static,
{
    port: Option<NonZeroU16>,
    on_request: Option<A>,
}

impl<A, AF> HttpServerBuilder<A, AF>
where
    A: Fn(HttpContext) -> AF + Clone + Send + 'static,
    AF: Future<Output = io::Result<()>> + 'static,
{
    pub fn new() -> Self {
        Self {
            port: None,
            on_request: None,
        }
    }

    pub fn port(mut self, port: NonZeroU16) -> Self {
        self.port = Some(port);
        self
    }

    /// Sets the function to call when a new request is accepted. The function may be called
    /// from any async task worker thread and any number of times concurrently.
    ///
    /// The request will be finalized when the provided HttpContext is dropped.
    pub fn on_request(mut self, callback: A) -> Self {
        self.on_request = Some(callback);
        self
    }

    /// Builds the HTTP server and starts accepting new requests.
    ///
    /// The startup process is gradual and requests may be received even before the result of
    /// this function is returned. Requests may even be received if this function ultimately
    /// returns an error.
    pub async fn build(self) -> io::Result<HttpServerHandle> {
        let port = self
            .port
            .ok_or_else(|| io::Error::InvalidOptions("port must be set".to_string()))?;
        let on_request = self
            .on_request
            .clone()
            .ok_or_else(|| io::Error::InvalidOptions("on_request must be set".to_string()))?;

        let url_prefix = format!("http://*:{port}/");
        let server_session = Arc::new(HttpServerSession::new(&url_prefix)?);

        // We spawn a dispatcher on every async worker.
        let mut shutdown_txs = Vec::new();

        let join_handles = current_runtime::with(|runtime| {
            runtime.spawn_on_all(|| {
                // This code runs on the originating thread and allows us to wire up per-worker.
                let (shutdown_tx, shutdown_rx) = oneshot::channel();

                shutdown_txs.push(shutdown_tx);

                let on_request_clone = on_request.clone();
                let session_clone = Arc::clone(&server_session);

                || async move {
                    // This code will run on each worker thread.
                    HttpDispatcher::new(session_clone, on_request_clone, shutdown_rx)
                        .run()
                        .await
                }
            })
        });

        event!(
            Level::DEBUG,
            message = "HTTP server started",
            port = port.get()
        );

        Ok(HttpServerHandle::new(join_handles, shutdown_txs))
    }
}

impl<A, AF> Default for HttpServerBuilder<A, AF>
where
    A: Fn(HttpContext) -> AF + Clone + Send + 'static,
    AF: Future<Output = io::Result<()>> + 'static,
{
    fn default() -> Self {
        Self::new()
    }
}

#[negative_impl]
impl<A, AF> !Send for HttpServerBuilder<A, AF>
where
    A: Fn(HttpContext) -> AF + Clone + Send + 'static,
    AF: Future<Output = io::Result<()>> + 'static,
{
}
#[negative_impl]
impl<A, AF> !Sync for HttpServerBuilder<A, AF>
where
    A: Fn(HttpContext) -> AF + Clone + Send + 'static,
    AF: Future<Output = io::Result<()>> + 'static,
{
}

/// Control surface to operate the HTTP server. The lifetime of this is not directly connected to
/// the HTTP server. Dropping this will not stop the server - you must explicitly call `stop()` to
/// stop the server.
pub struct HttpServerHandle {
    dispatcher_join_handles: Box<[RemoteJoinHandle<()>]>,
    dispatcher_shutdown_txs: Vec<oneshot::Sender<()>>,
}

impl HttpServerHandle {
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
        event!(Level::TRACE, "signaling HTTP dispatcher tasks to stop");

        for tx in self.dispatcher_shutdown_txs.drain(..) {
            // We ignore the result (maybe the remote side is already terminated).
            let _ = tx.send(());
        }
    }
}

#[negative_impl]
impl !Send for HttpServerHandle {}
#[negative_impl]
impl !Sync for HttpServerHandle {}

/// An active session with the http.sys web server API. When we drop this, all request processing
/// will finish. In principle, we have to stop request processing before dropping this.
///
/// This is a thread-safe entity shared between all the HTTP request dispatchers and related
/// entities. Think of this as the http.sys equivalent to an `OwnedHandle<SOCKET>`.
#[derive(Debug)]
pub(super) struct HttpServerSession {
    session_id: u64,
    url_group_id: u64,
    request_queue_handle: ThreadSafe<HANDLE>,
}

impl HttpServerSession {
    fn new(url_prefix: &str) -> io::Result<Self> {
        let version = HTTPAPI_VERSION {
            HttpApiMajorVersion: 2,
            HttpApiMinorVersion: 0,
        };
        let flags = HTTP_INITIALIZE_SERVER;

        // SAFETY: We must match this with a call to HttpTerminate later. We do that in Drop.
        http_sys::to_io_result(unsafe { HttpInitialize(version, flags, None) })?;

        let http_sys_session_guard = guard((), |_| {
            let flags = HTTP_INITIALIZE_SERVER;

            // SAFETY: Just FFI, nothing unsafe here. Do not really care if anything goes wrong.
            _ = unsafe { HttpTerminate(flags, None) };
        });

        let mut session_id: u64 = 0;

        // SAFETY: We must clean up resources later. We do that in Drop.
        http_sys::to_io_result(unsafe {
            HttpCreateServerSession(version, &mut session_id as *mut _, 0)
        })?;

        let server_session_guard = guard((), |_| {
            // SAFETY: Just FFI, nothing unsafe here. Do not really care if anything goes wrong.
            _ = unsafe { HttpCloseServerSession(session_id) };
        });

        let mut url_group_id: u64 = 0;

        // SAFETY: We must clean up resources later. We do that in Drop.
        http_sys::to_io_result(unsafe {
            HttpCreateUrlGroup(session_id, &mut url_group_id as *mut _, 0)
        })?;

        let url_group_guard = guard((), |_| {
            // SAFETY: Just FFI, nothing unsafe here. Do not really care if anything goes wrong.
            _ = unsafe { HttpCloseUrlGroup(url_group_id) };
        });

        let url_prefix_wide = OsStr::new(url_prefix)
            .encode_wide()
            .chain(Some(0))
            .collect::<Vec<u16>>()
            .into_boxed_slice();

        http_sys::to_io_result(unsafe {
            HttpAddUrlToUrlGroup(
                url_group_id,
                PCWSTR::from_raw(url_prefix_wide.as_ptr()),
                0,
                0,
            )
        })?;

        let mut request_queue_handle: HANDLE = HANDLE::default();
        http_sys::to_io_result(unsafe {
            HttpCreateRequestQueue(version, None, None, 0, &mut request_queue_handle as *mut _)
        })?;

        let request_queue_guard = guard((), |_| {
            // SAFETY: Just FFI, nothing unsafe here. Do not really care if anything goes wrong.
            _ = unsafe { HttpCloseRequestQueue(request_queue_handle) };
        });

        let queue_length = REQUEST_QUEUE_LENGTH as u32;
        // SAFETY: Just FFI, nothing unsafe here as long as our pointer points to valid memory.
        http_sys::to_io_result(unsafe {
            HttpSetRequestQueueProperty(
                request_queue_handle,
                HttpServerQueueLengthProperty,
                &queue_length as *const _ as *const _,
                mem::size_of::<u32>() as u32,
                0,
                None,
            )
        })?;

        // The request queue is bound to the multithreaded I/O driver to ensure that requests are
        // distributed across all our threads. This is not strictly optimal because it also means
        // individual reads/writes of ONE request are spread across threads. Unfortunately, the
        // http.sys API does not let us choose here (there is no per-request/connection binding).
        current_async_agent::with_io_shared(|io| io.bind_io_primitive(&request_queue_handle))?;

        let http_binding_info = HTTP_BINDING_INFO {
            Flags: HTTP_PROPERTY_FLAGS {
                // HTTP_PROPERTY_FLAG_PRESENT: 0x00000001
                _bitfield: 0x00000001,
            },
            RequestQueueHandle: request_queue_handle,
        };

        http_sys::to_io_result(unsafe {
            HttpSetUrlGroupProperty(
                url_group_id,
                HttpServerBindingProperty,
                &http_binding_info as *const _ as *const _,
                std::mem::size_of::<HTTP_BINDING_INFO>() as u32,
            )
        })?;

        // Success! We now have a working HTTP listener that we can dispatch requests from.~
        // Defuse the cleanup guards, since the type now takes over those responsibilities directly.
        ScopeGuard::into_inner(request_queue_guard);
        ScopeGuard::into_inner(url_group_guard);
        ScopeGuard::into_inner(server_session_guard);
        ScopeGuard::into_inner(http_sys_session_guard);

        Ok(HttpServerSession {
            session_id,
            url_group_id,
            // SAFETY: Yes, it really is thread safe - I promise!
            request_queue_handle: unsafe { ThreadSafe::new(request_queue_handle) },
        })
    }

    pub fn request_queue_native_handle(&self) -> &HANDLE {
        &self.request_queue_handle
    }
}

impl Drop for HttpServerSession {
    fn drop(&mut self) {
        // SAFETY: Just FFI, nothing unsafe here. Do not really care if anything goes wrong.
        _ = unsafe { HttpCloseRequestQueue(*self.request_queue_handle) };

        // SAFETY: Just FFI, nothing unsafe here. Do not really care if anything goes wrong.
        _ = unsafe { HttpCloseUrlGroup(self.url_group_id) };

        // SAFETY: Just FFI, nothing unsafe here. Do not really care if anything goes wrong.
        _ = unsafe { HttpCloseServerSession(self.session_id) };

        let flags = HTTP_INITIALIZE_SERVER;

        // SAFETY: Just FFI, nothing unsafe here. Do not really care if anything goes wrong.
        _ = unsafe { HttpTerminate(flags, None) };
    }
}

const CONCURRENT_RECEIVE_OPERATIONS_PER_DISPATCHER: usize = 128;

// Same value as for TCP server - unsure how exactly it matters or what is a good value.
const REQUEST_QUEUE_LENGTH: usize = 4096;

/// The HTTP dispatcher processes the request queue used to receive new requests. When a new
/// requests is received, it is dispatched to be handled by the user-defined callback on the
/// current thread, at which point the dispatcher is no longer involved.
struct HttpDispatcher<A, AF>
where
    A: Fn(HttpContext) -> AF + Clone + Send + 'static,
    AF: Future<Output = io::Result<()>> + 'static,
{
    // If we receive a message from here, it means we need to shut down. Consumed on use.
    shutdown_rx: Option<oneshot::Receiver<()>>,

    session: Arc<HttpServerSession>,

    // Whenever we receive a new request, we spawn a new task with this callback to handle it.
    // Once we schedule a task to call this, the dispatcher forgets about the connection - anything
    // that happens afterward is the responsibility of the HttpContext to organize.
    on_request: A,
    // TODO: on_connection_error (callback if connection fails, probably without affecting other connections or general health)
    // TODO: on_worker_error (callback if worker-level operation fails and we probably will not receive more traffic on this worker)
    // TODO: on_handler_error (callback if on_accept fails; do we need this or just let on_accept worry about it?)
}

impl<A, AF> HttpDispatcher<A, AF>
where
    A: Fn(HttpContext) -> AF + Clone + Send + 'static,
    AF: Future<Output = io::Result<()>> + 'static,
{
    fn new(
        session: Arc<HttpServerSession>,
        on_request: A,
        shutdown_rx: oneshot::Receiver<()>,
    ) -> Self {
        Self {
            session,
            on_request,
            shutdown_rx: Some(shutdown_rx),
        }
    }

    async fn run(&mut self) {
        // All the ongoing receive operations. We will keep this filled up to the limit of
        // CONCURRENT_RECEIVE_OPERATIONS, so whenever some get accepted, more receives get queued.
        let mut receive_futures = Box::pin(FuturesUnordered::new());

        // If this completes, we shut down the dispatcher.
        let mut shutdown_received_future = self.shutdown_rx.take().expect("we only take this once");

        loop {
            while receive_futures.len() < CONCURRENT_RECEIVE_OPERATIONS_PER_DISPATCHER {
                receive_futures.push(
                    ReceiveOne {
                        session: Arc::clone(&self.session),
                    }
                    .execute(),
                );
            }

            event!(
                Level::TRACE,
                message = "waiting for new connection or shutdown",
                receive_futures_len = receive_futures.len(),
            );

            let receive_result = match select(receive_futures.next(), shutdown_received_future)
                .await
            {
                Either::Left((Some(receive_result), new_shutdown_received_future)) => {
                    shutdown_received_future = new_shutdown_received_future;
                    receive_result
                }
                Either::Left((None, _)) => {
                    panic!("receive_futures stream ended unexpectedly - we are supposed to refill it before checking");
                }
                Either::Right(_) => {
                    event!(Level::DEBUG, "HTTP dispatcher shutting down",);
                    // We will not accept any new connections. The existing "receive" operations
                    // will be dropped soon and any pending I/O will likewise be canceled as soon
                    // as the session is dropped gets closed.
                    return;
                }
            };

            event!(
                Level::TRACE,
                message = "detected incoming HTTP request (or error)",
                receive_futures_len = receive_futures.len(),
                ?receive_result
            );

            let Ok(http_context) = receive_result else {
                event!(
                    Level::ERROR,
                    message = "error accepting new request - ignoring",
                    error = receive_result.unwrap_err().to_string()
                );
                // TODO: Report error to callback if not successfully accepted..
                continue;
            };

            // New request accepted! Spawn as task and detach.
            // We spawn it on the same async worker that caught the connection.
            _ = spawn((self.on_request)(http_context));
        }
    }
}

/// The state of a single "receive one requiest" operation. We create this separate type to more
/// easily separate the resource management of the command-processing loop from the resource
/// management of the connection-accepting tasks.
struct ReceiveOne {
    session: Arc<HttpServerSession>,
}

impl ReceiveOne {
    async fn execute(self) -> io::Result<HttpContext> {
        let http_request_buffer = unsafe {
            current_async_agent::with_io_shared(|io| {
                io.new_operation(Buffer::<Shared>::from_pool())
            })
            .begin({
                let session = Arc::clone(&self.session);

                move |buffer, overlapped, immediate_bytes_transferred| {
                    // We specify 0 as flags, which means "do not give the request body, only the headers".
                    // This is suboptimal for performance but gives us the simplest code to start with.
                    // We can opitmize later and complicate the code as needed.
                    let flags = HTTP_RECEIVE_HTTP_REQUEST_FLAGS(0);

                    http_sys::to_io_result(HttpReceiveHttpRequest(
                        *session.request_queue_native_handle(),
                        0,
                        flags,
                        buffer.as_mut_ptr() as *mut _,
                        buffer.len() as u32,
                        Some(immediate_bytes_transferred as *mut _),
                        Some(overlapped),
                    ))
                }
            })
        }
        .await
        .into_inner()?;

        // SAFETY: We are holding the buffer in scope, so the reference will remain valid for as
        // long as this scope remains alive (until end of the function). The native API promises to
        // give us a response of this type.
        let http_request =
            unsafe { &*(http_request_buffer.as_slice().as_ptr() as *const HTTP_REQUEST_V2) };

        // As long as we do not care about headers (we don't, not yet) or the path (also not yet)
        // and do not receive any request body in the first payload (check), this is all we need.
        // With this, we can start receiving the request body (if we wanted to - don't care yet)
        // and sending the response (which we do want to do).
        let request_id = http_request.Base.RequestId;

        Ok(HttpContext::new(request_id, self.session))
    }
}
