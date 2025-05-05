use std::io;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ServerError {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    #[error("UTF-8 decoding error: {0}")]
    Utf8Error(#[from] std::string::FromUtf8Error),

    #[error("Invalid protocol line: '{0}'")]
    InvalidProtocol(String),

    #[error("Invalid command: {0}")]
    InvalidCommand(String),

    #[error("Missing argument for command: {0}")]
    MissingArgument(String),

    #[error("Invalid argument for command {command}: {argument}")]
    InvalidArgument { command: String, argument: String },

    // #[error("Payload size mismatch: expected {expected}, got {actual}")]
    // PayloadSizeMismatch { expected: usize, actual: usize },
    #[error("JSON parsing error: {0}")]
    JsonError(#[from] serde_json::Error),

    #[error("Client disconnected unexpectedly")]
    ClientDisconnected,
}

pub type ServerResult<T> = Result<T, ServerError>;
