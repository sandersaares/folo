use super::HttpServerSession;
use crate::{
    io::{self, OperationResultShared, OperationResultSharedExt, PinnedBufferShared},
    net::http_sys,
    rt::current_async_agent,
};
use std::{mem, ptr, sync::Arc};
use windows::{
    core::PCSTR,
    Win32::Networking::HttpServer::{
        HttpDataChunkFromMemory, HttpHeaderContentLength, HttpHeaderContentType,
        HttpSendHttpResponse, HttpSendResponseEntityBody, HTTP_DATA_CHUNK, HTTP_KNOWN_HEADER,
        HTTP_RESPONSE_HEADERS, HTTP_RESPONSE_V2, HTTP_SEND_RESPONSE_FLAG_MORE_DATA, HTTP_VERSION,
    },
};

#[derive(Debug)]
pub struct HttpContext {
    id: u64,
    session: Arc<HttpServerSession>,
}

impl HttpContext {
    pub(super) fn new(id: u64, session: Arc<HttpServerSession>) -> Self {
        Self { id, session }
    }

    /// Sends response headers to the peer. This is a quick and dirty way to specify the essentials.
    pub async fn send_response_headers(
        &mut self,
        content_type: &str,
        content_length: usize,
    ) -> io::Result<()> {
        // TODO: We must keep this alive until the send completes, even if the future is dropped.
        let content_length_str_bytes = content_length.to_string().into_bytes().into_boxed_slice();
        let content_length = PCSTR::from_raw(content_length_str_bytes.as_ptr());

        // TODO: We must keep this alive until the send completes, even if the future is dropped.
        let content_type_str_bytes = content_type.to_string().into_bytes().into_boxed_slice();
        let content_type = PCSTR::from_raw(content_type_str_bytes.as_ptr());

        let mut headers = HTTP_RESPONSE_HEADERS::default();
        headers.KnownHeaders[HttpHeaderContentLength.0 as usize] = HTTP_KNOWN_HEADER {
            RawValueLength: content_length_str_bytes.len() as u16,
            pRawValue: content_length,
        };
        headers.KnownHeaders[HttpHeaderContentType.0 as usize] = HTTP_KNOWN_HEADER {
            RawValueLength: content_type_str_bytes.len() as u16,
            pRawValue: content_type,
        };

        // We construct our HTTP_RESPONSE in this buffer.
        let mut buffer = PinnedBufferShared::from_pool();
        let http_response_len = mem::size_of::<HTTP_RESPONSE_V2>();
        assert!(buffer.len() >= http_response_len);

        let response =
            buffer.as_mut_slice_with_len(http_response_len).as_mut_ptr() as *mut HTTP_RESPONSE_V2;

        unsafe {
            *response = HTTP_RESPONSE_V2 {
                Base: windows::Win32::Networking::HttpServer::HTTP_RESPONSE_V1 {
                    Flags: 0,
                    Version: HTTP_VERSION::default(), // Ignored.
                    StatusCode: 200,
                    ReasonLength: 0,
                    pReason: PCSTR::null(),
                    Headers: headers,
                    EntityChunkCount: 0,
                    pEntityChunks: ptr::null_mut(),
                },
                ..Default::default()
            };
        }

        unsafe {
            current_async_agent::with_io_shared(|io| io.new_operation(buffer)).begin({
                let session = Arc::clone(&self.session);
                let id = self.id;

                move |buffer, overlapped, immediate_bytes_transferred| {
                    // We assume there will always be a response body.
                    let flags = HTTP_SEND_RESPONSE_FLAG_MORE_DATA;

                    http_sys::to_io_result(HttpSendHttpResponse(
                        *session.request_queue_native_handle(),
                        id,
                        flags,
                        buffer.as_ptr() as *const _,
                        None,
                        Some(immediate_bytes_transferred as *mut _),
                        None,
                        0,
                        Some(overlapped),
                        None,
                    ))
                }
            })
        }
        .await
        .into_inner()
        .map(|_| ()) // We do not give the buffer to the caller.
    }

    /// Sends a buffer of response body data to the peer.
    ///
    /// The buffer will be returned in the result to allow reuse.
    ///
    /// You may call this multiple times concurrently. The buffers will be sent in the order they
    /// are submitted. The sum of all the payloads must equal the content length specified in
    /// the call to `send_response_headers()`. The final send must set the  `is_final` flag.
    pub async fn send_response_body(
        &mut self,
        buffer: PinnedBufferShared,
        is_final: bool,
    ) -> OperationResultShared {
        unsafe {
            current_async_agent::with_io_shared(|io| io.new_operation(buffer)).begin({
                let session = Arc::clone(&self.session);
                let id = self.id;

                move |buffer, overlapped, immediate_bytes_transferred| {
                    let flags = if is_final {
                        0
                    } else {
                        HTTP_SEND_RESPONSE_FLAG_MORE_DATA
                    };

                    // For now we just send 1 chunk. For optimization, we can support sending a list of chunks.
                    let mut chunks = [HTTP_DATA_CHUNK::default(); 1];
                    chunks[0].DataChunkType = HttpDataChunkFromMemory;
                    chunks[0].Anonymous.FromMemory.pBuffer = buffer.as_mut_ptr() as *mut _;
                    chunks[0].Anonymous.FromMemory.BufferLength = buffer.len() as u32;

                    http_sys::to_io_result(HttpSendResponseEntityBody(
                        *session.request_queue_native_handle(),
                        id,
                        flags,
                        Some(&chunks),
                        Some(immediate_bytes_transferred as *mut _),
                        None,
                        0,
                        Some(overlapped),
                        None,
                    ))
                }
            })
        }
        .await
    }
}
