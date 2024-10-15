use std::{
    convert::Infallible,
    fmt::Debug,
    io::{self, Write},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tracing::{error, info, instrument, Level};

use faasrail_loadgen::{source::SourceBackend, InvocationId, WorkloadRequest};

#[derive(Debug, ::thiserror::Error)]
pub enum Error {
    #[error("JSON serialization error")]
    JsonSerialization(#[source] ::serde_json::Error),

    #[error("I/O Error: {msg}")]
    Io {
        msg: Box<str>,
        #[source]
        err: io::Error,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LoggedRequest {
    epoch_us: u64,
    invocation_id: InvocationId,
    wreq: WorkloadRequest,
}

#[derive(Debug)]
pub struct Logger<W: Write> {
    writer: W,

    tx: Option<mpsc::Sender<LoggedRequest>>,
    rx: mpsc::Receiver<LoggedRequest>,
}

impl<W: Write> Logger<W> {
    const BUFSZ: usize = 1 << 15;

    pub fn new(inner: W) -> Self {
        let (tx, rx) = mpsc::channel(Self::BUFSZ);
        Self {
            writer: inner,
            tx: Some(tx),
            rx,
        }
    }

    pub fn new_ref(&self) -> Option<LoggerRef> {
        self.tx.as_ref().map(|tx| LoggerRef { tx: tx.clone() })
    }

    #[instrument(level = Level::INFO, skip_all)]
    pub fn run(mut self) -> Result<u64, Error> {
        // Drop our sending half, so that we can break out of the receiving loop when all other
        // sending halves have been dropped.
        let Some(tx) = self.tx.take() else {
            todo!("error or panic?")
        };
        drop(tx);

        let mut num_requests = 0;
        while let Some(lreq) = self.rx.blocking_recv() {
            num_requests += 1;

            ::serde_json::to_writer(&mut self.writer, &lreq).map_err(Error::JsonSerialization)?;
            self.writer.write_all(b"\n").map_err(|err| Error::Io {
                msg: "error apending newline to writer".into(),
                err,
            })?;
        }
        info!("Exiting...");
        Ok(num_requests)
    }
}

#[derive(Debug, Clone)]
pub struct LoggerRef {
    tx: mpsc::Sender<LoggedRequest>,
}

impl SourceBackend for LoggerRef {
    type Error = Infallible;

    #[instrument(level = Level::INFO, skip(self, wreq))]
    #[inline]
    async fn issue(
        &mut self,
        invocation_id: InvocationId,
        wreq: &WorkloadRequest,
        minute: u16,
        timeout: Duration,
    ) -> Result<(), Self::Error> {
        if let Err(err) = self
            .tx
            .send_timeout(
                LoggedRequest {
                    epoch_us: SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .expect("UNIX Epoch should be < than all timestamps")
                        .as_micros() as u64,
                    invocation_id,
                    wreq: wreq.clone(),
                },
                timeout,
            )
            .await
        {
            error!(error = ?err, "Failed to send Request to Logger: {err:#}");
        }
        Ok(())
    }
}
