use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error(transparent)]
    External(#[from] windows_result::Error),
}

pub type Result<T> = std::result::Result<T, Error>;
