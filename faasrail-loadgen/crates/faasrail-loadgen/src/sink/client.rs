use std::{
    fmt::Debug,
    fs::File,
    io::BufWriter,
    io::{self, Write},
    path::{Path, PathBuf},
};

use tokio::{
    sync::{broadcast, mpsc},
    task::JoinHandle,
};
use tracing::{error, info, instrument, warn_span, Level};

#[derive(Debug, ::thiserror::Error)]
pub enum Error {
    #[error("I/O error: {msg}")]
    Io {
        msg: Box<str>,
        #[source]
        source: Option<io::Error>,
    },
}

#[derive(Debug)]
pub struct SinkClient<B: super::backend::Backend> {
    csv_path: PathBuf,
    backend: B,
}

impl<B: super::backend::Backend> SinkClient<B> {
    pub fn new(path: impl AsRef<Path>, backend: B) -> Result<Self, Error> {
        Ok(Self {
            csv_path: path.as_ref().to_path_buf(),
            backend,
        })
    }

    #[instrument(level = Level::INFO, skip_all)]
    pub async fn run(self, quit_rx: broadcast::Receiver<()>) -> Result<u64, Error> {
        let (to_appender, from_sink) = mpsc::channel(1 << 15); // FIXME: chan cap ?
        let appender_handle = Self::spawn_appender(&self.csv_path, from_sink)?;
        let sink_backend_handle =
            ::tokio::spawn(async move { self.backend.run(to_appender, quit_rx).await });

        let mut num_responses = 0;
        match ::tokio::try_join!(appender_handle, sink_backend_handle) {
            Ok((appender, sink_backend)) => {
                match appender {
                    Ok(appended_responses) => {
                        num_responses = appended_responses;
                        info!(?appended_responses, "File-appender task joined");
                    }
                    Err(err) => error!(error = ?err, "File-appender task joined: {err:#}"),
                }
                match sink_backend {
                    Ok(num_responses) => info!(?num_responses, "Sink-backend task joined"),
                    Err(err) => error!(error = ?err, "Sink-backend task joined: {err:#}"),
                }
            }
            Err(err) => error!(error = ?err, "Failed to join both tasks: {err:#}"),
        }
        Ok(num_responses)
    }

    fn spawn_appender(
        path: impl AsRef<Path>,
        mut from_sink: mpsc::Receiver<B::Response>,
    ) -> Result<JoinHandle<Result<u64, Error>>, Error> {
        let appender_handle = ::tokio::task::spawn_blocking({
            let mut bw = BufWriter::with_capacity(
                1 << 16,
                File::options()
                    .create_new(true)
                    .write(true)
                    .open(&path)
                    .map_err(|err| Error::Io {
                        msg: format!("failed to open output file '{}'", path.as_ref().display())
                            .into_boxed_str(),
                        source: Some(err),
                    })?,
            );

            move || {
                let span = warn_span!("sink-file-appender");
                let _span_guard = span.enter();

                // Receive responses until Sink's channel has been closed & drained
                let mut num_resps = 0;
                while let Some(resp) = from_sink.blocking_recv() {
                    if let Err(err) = ::serde_json::to_writer(&mut bw, &resp) {
                        error!(error = ?err, "Failed to append to file JSON-encoded '{resp:?}': {err:#}");
                    }
                    num_resps += 1;
                    if let Err(err) = bw.write_all(b"\n") {
                        error!(error = ?err, "Failed to append newline after a Response: {err:#}");
                    }
                    // TODO
                }

                info!("Sink's channel has been closed & drained; flushing buffer and exiting...");
                if let Err(err) = bw.flush() {
                    error!(error = ?err, "Failed to flush buffer to file: {err:#}");
                }
                Ok(num_resps)
            }
        });
        Ok(appender_handle)
    }
}
