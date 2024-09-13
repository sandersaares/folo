use thiserror::Error;
use windows::Win32::Networking::WinSock::WSA_ERROR;

#[derive(Debug, Error)]
pub enum Error {
    #[error("invalid options specified: {0}")]
    InvalidOptions(String),

    #[error("Winsock error {} ({})", .code, .detail.0)]
    Winsock { code: i32, detail: WSA_ERROR },

    #[error(transparent)]
    External(#[from] windows_result::Error),
}

pub type Result<T> = std::result::Result<T, Error>;
