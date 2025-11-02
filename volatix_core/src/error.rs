use std::{
    fmt,
    fs::File,
    io::{self, BufWriter, Write},
    path::Path,
    sync::mpsc::Receiver,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::Context;

#[derive(Debug, Clone, PartialEq)]
pub struct Error {
    pub inner: Inner,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Inner {
    ParserError { message: String, offset: usize },
    StorageError { message: String },
}

impl Error {
    pub fn into_inner(&self) -> Inner {
        self.inner.clone()
    }
}

impl From<Error> for io::Error {
    fn from(value: Error) -> Self {
        match value.into_inner() {
            Inner::ParserError { message, offset: _ } => io::Error::other(message),
            Inner::StorageError { message } => io::Error::other(message),
        }
    }
}

impl Inner {
    pub fn message(&self) -> String {
        match self {
            Inner::ParserError { message, offset: _ } => message.clone(),
            Inner::StorageError { message } => message.clone(),
        }
    }
}

/// Converts a error message to an Error with inner type ParserError.
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
    ($msg:expr, $offset:expr) => {
        Err($crate::Error {
            inner: $crate::Inner::ParserError {
                message: $msg,
                offset: $offset,
            },
        })
    };
}

/// Converts a error message to a Error with inner type StorageError.
#[macro_export]
macro_rules! storage_error {
    ($msg:literal) => {
        Err($crate::Error {
            inner: $crate::Inner::StorageError {
                message: $msg.to_string(),
            },
        })
    };
    ($msg:expr) => {
        Err($crate::Error {
            inner: $crate::Inner::StorageError { message: $msg },
        })
    };
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.inner.message())
    }
}

/// Represents different types of program information and messages
#[derive(Debug, Clone)]
pub enum Message {
    /// Any useful information
    Info(String),
    /// Error Information
    Error(String),
    /// Debug Information
    Debug(String),
    /// Signal to the message handler to quit
    Break,
}

/// Writes log messages to disk
/// The function exits if the handler receives a `Message::Break`.
pub fn handle_messages(log_file: &Path, handler: Receiver<Message>) -> anyhow::Result<()> {
    let log_file = File::options()
        .create(true)
        .append(true)
        .open(log_file)
        .context("Open log file for writing")?;
    let mut f = BufWriter::new(log_file);

    while let Ok(msg) = handler.recv() {
        let now = SystemTime::now();
        match msg {
            Message::Info(m) => {
                let m = format!(
                    "{now} INFO {m}",
                    now = now.duration_since(UNIX_EPOCH).unwrap().as_secs()
                );
                let _ = writeln!(&mut f, "{m}");
            }
            Message::Error(m) => {
                let m = format!(
                    "{now} ERROR {m}",
                    now = now.duration_since(UNIX_EPOCH).unwrap().as_secs()
                );
                let _ = writeln!(&mut f, "{m}");
            }
            Message::Debug(m) => {
                let m = format!(
                    "{now} DEBUG {m}",
                    now = now.duration_since(UNIX_EPOCH).unwrap().as_secs()
                );
                let _ = writeln!(&mut f, "{m}");
            }
            Message::Break => break,
        }
    }

    Ok(())
}
