use thiserror::Error;
use windows::Win32::Networking::WinSock::WSA_ERROR;

#[derive(Debug, Error)]
pub enum Error {
    #[error("invalid options specified: {0}")]
    InvalidOptions(String),

    #[error("Winsock error {} ({})", .code, .detail.0)]
    Winsock { code: i32, detail: WSA_ERROR },

    #[error(transparent)]
    Windows(#[from] windows_result::Error),

    #[error(transparent)]
    StdIo(#[from] std::io::Error),

    // This is for unexpected situations like a thread disappearing without ever reporting status.
    // Things that we are not expecting, things that are programming errors in the library itself.
    #[error("internal error: {0}")]
    Internal(String),
}

pub type Result<T> = std::result::Result<T, Error>;


impl From<std::io::Error> for Error {
    fn from(value: std::io::Error) -> Self {
        Error::StdIo(value)
    }
}
