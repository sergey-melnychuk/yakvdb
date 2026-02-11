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
            Error::IO(io) => write!(f, "IO error: '{io}'."),
            Error::Tree(id, msg) => write!(f, "Tree error (page: {id}): '{msg}'."),
            Error::Other(msg) => write!(f, "Other error: '{msg}'."),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_display() {
        let io_err = Error::IO(io::Error::new(io::ErrorKind::NotFound, "gone"));
        assert!(format!("{io_err}").contains("IO error"));

        let tree_err = Error::Tree(42, "bad node".to_string());
        assert!(format!("{tree_err}").contains("Tree error"));
        assert!(format!("{tree_err}").contains("42"));

        let other_err = Error::Other("oops".to_string());
        assert!(format!("{other_err}").contains("Other error"));
    }

    #[test]
    fn test_source() {
        use std::error::Error as StdError;

        let io_err = Error::IO(io::Error::other("disk fail"));
        assert!(io_err.source().is_some());

        let tree_err = Error::Tree(1, "x".to_string());
        assert!(tree_err.source().is_none());

        let other_err = Error::Other("y".to_string());
        assert!(other_err.source().is_none());
    }

    #[test]
    fn test_from_io() {
        let io_err = io::Error::new(io::ErrorKind::PermissionDenied, "nope");
        let err: Error = io_err.into();
        assert!(matches!(err, Error::IO(_)));
    }

    #[test]
    fn test_from_string() {
        let err: Error = "something broke".to_string().into();
        assert!(matches!(err, Error::Other(_)));
    }
}
