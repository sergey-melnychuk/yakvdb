use std::fmt::Formatter;
use std::io;
use std::result;

#[derive(Debug)]
pub enum Error {
    IO(io::Error),
    Tree(u32, String),
    Other(String),
}

pub type Result<T> = result::Result<T, Error>;

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::IO(io) => write!(f, "IO error: '{}'.", io),
            Error::Tree(id, msg) => write!(f, "Tree error (page: {}): '{}'.", id, msg),
            Error::Other(msg) => write!(f, "Other error: '{}'.", msg),
        }
    }
}

impl std::error::Error for Error {
    fn cause(&self) -> Option<&dyn std::error::Error> {
        match self {
            Error::IO(io) => Some(io),
            _ => None,
        }
    }
}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Error::IO(e)
    }
}

impl From<String> for Error {
    fn from(str: String) -> Self {
        Error::Other(str)
    }
}
