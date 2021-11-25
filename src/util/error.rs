use crate::jupyter::JupyterMessage;
use datafusion::arrow::error::ArrowError;
use datafusion::error::DataFusionError;
use serde_json::Error;
use std::error;
use std::fmt::{Display, Formatter};
use std::result;

/// Result type for operations that could result in an [BoxError]
pub type Result<T> = result::Result<T, BoxError>;

/// BoxError error
#[derive(Debug)]
#[allow(missing_docs)]
pub enum BoxError {
    ///
    BoxError(String),

    /// Error returned by arrow.
    ArrowError(ArrowError),

    /// Error returned by DataFusion.
    DataFusionError(DataFusionError),

    /// Error returned by serde
    SerdeError(serde_json::Error),

    /// Io errors
    IoError(std::io::Error),

    /// Regex errors
    RegexError(regex::Error),

    /// ZeroMQ errors
    ZmqError(zmq::Error),

    /// Json errors
    JsonError(json::Error),

    /// Hex errors
    HexError(hex::FromHexError),

    /// Mpsc errors
    MpscRecvError(std::sync::mpsc::RecvError),
    MpscSendError(std::sync::mpsc::SendError<JupyterMessage>),

    /// Utf8 errors
    Utf8Error(std::str::Utf8Error),
}

impl BoxError {
    pub fn new(message: String) -> Self {
        BoxError::BoxError(message)
    }
}

impl From<DataFusionError> for BoxError {
    fn from(e: DataFusionError) -> Self {
        BoxError::DataFusionError(e)
    }
}

impl From<serde_json::Error> for BoxError {
    fn from(e: Error) -> Self {
        BoxError::SerdeError(e)
    }
}

impl From<ArrowError> for BoxError {
    fn from(e: ArrowError) -> Self {
        BoxError::ArrowError(e)
    }
}

impl From<std::io::Error> for BoxError {
    fn from(e: std::io::Error) -> Self {
        BoxError::IoError(e)
    }
}
impl From<regex::Error> for BoxError {
    fn from(e: regex::Error) -> Self {
        BoxError::RegexError(e)
    }
}

impl From<zmq::Error> for BoxError {
    fn from(e: zmq::Error) -> Self {
        BoxError::ZmqError(e)
    }
}

impl From<json::Error> for BoxError {
    fn from(e: json::Error) -> Self {
        BoxError::JsonError(e)
    }
}

impl From<hex::FromHexError> for BoxError {
    fn from(e: hex::FromHexError) -> Self {
        BoxError::HexError(e)
    }
}

impl From<std::sync::mpsc::RecvError> for BoxError {
    fn from(e: std::sync::mpsc::RecvError) -> Self {
        BoxError::MpscRecvError(e)
    }
}

impl From<std::sync::mpsc::SendError<JupyterMessage>> for BoxError {
    fn from(e: std::sync::mpsc::SendError<JupyterMessage>) -> Self {
        BoxError::MpscSendError(e)
    }
}

impl From<std::str::Utf8Error> for BoxError {
    fn from(e: std::str::Utf8Error) -> Self {
        BoxError::Utf8Error(e)
    }
}

impl Display for BoxError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match *self {
            BoxError::ArrowError(ref desc) => write!(f, "{}", desc),
            BoxError::BoxError(ref desc) => write!(f, "{}", desc),
            BoxError::DataFusionError(ref desc) => write!(f, "{}", desc),
            BoxError::IoError(ref desc) => write!(f, "{}", desc),
            BoxError::RegexError(ref desc) => write!(f, "{}", desc),
            BoxError::SerdeError(ref desc) => write!(f, "{}", desc),
            BoxError::ZmqError(ref desc) => write!(f, "{}", desc),
            BoxError::JsonError(ref desc) => write!(f, "{}", desc),
            BoxError::HexError(ref desc) => write!(f, "{}", desc),
            BoxError::MpscRecvError(ref desc) => write!(f, "{}", desc),
            BoxError::MpscSendError(ref desc) => write!(f, "{}", desc),
            BoxError::Utf8Error(ref desc) => write!(f, "{}", desc),
        }
    }
}

impl error::Error for BoxError {}
