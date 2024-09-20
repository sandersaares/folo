use crate::io;
use windows::Win32::Foundation::WIN32_ERROR;

/// Converts an http.sys result into an IO result.
pub fn to_io_result(http_sys_result: u32) -> io::Result<()> {
    if http_sys_result == 0 {
        Ok(())
    } else {
        // http.sys does not SetLastError, instead we must translate the result code directly.
        Err(windows_result::Error::from_hresult(WIN32_ERROR(http_sys_result).into()).into())
    }
}
