use std::fmt;

#[derive(Debug, PartialEq)]
pub struct Error {
    pub inner: Inner,
}

#[derive(Debug, PartialEq)]
pub enum Inner {
    ParserError { message: String, offset: usize },
    StorageError { message: String },
}

#[macro_export]
macro_rules! parser_error {
    ($msg:expr, $offset:expr) => {
        Err($crate::Error {
            inner: $crate::Inner::ParserError {
                message: $msg.to_string(),
                offset: $offset,
            },
        })
    };
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.inner {
            Inner::ParserError { message, offset: _ } => {
                // FIX: Figure out a better way to implement this
                write!(f, "{message}")
            }
            Inner::StorageError { message } => {
                write!(f, "{message}")
            }
        }
    }
}
