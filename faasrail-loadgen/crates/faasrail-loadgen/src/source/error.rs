use std::io;

use crate::source::worker::Error as WorkerError;

#[derive(Debug, ::thiserror::Error)]
pub enum Error {
    #[error("CSV deserialization error")]
    CsvDeserialization(#[source] ::csv::Error),

    #[error("I/O error: {msg}")]
    Io {
        msg: Box<str>,
        #[source]
        source: io::Error,
    },

    #[error("JSON serialization error")]
    JsonSerialization(#[source] ::serde_json::Error),

    #[error("invalid minute range: {msg}")]
    Minute {
        msg: Box<str>,
        #[source]
        source: Option<::std::num::ParseIntError>,
    },

    #[error("error in Worker")]
    Worker(#[source] WorkerError),
}
