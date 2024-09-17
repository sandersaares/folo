use thiserror::Error;
use windows::Win32::Networking::WinSock::WSA_ERROR;

#[derive(Debug, Error)]
pub enum Error {
    #[error("invalid options specified: {0}")]
    InvalidOptions(String),

    #[error("logic error: {0}")]
    LogicError(String),

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

    #[error(transparent)]
    Other(#[from] Box<dyn std::error::Error + Send + Sync>), 
}

pub type Result<T> = std::result::Result<T, Error>;

impl From<Error> for std::io::Error {
    fn from(value: Error) -> Self {
        match value {
            Error::StdIo(error) => error,
            _ => std::io::Error::other(value)
        }
    }
}