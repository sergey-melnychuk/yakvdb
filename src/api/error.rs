use std::io;
use std::result;

enum Error {
    IO(io::Error),
    Other(String),
}

type Result<T> = result::Result<T, Error>;

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
