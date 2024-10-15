use std::{
    fmt::Debug,
    fs::File,
    io::{BufRead, BufReader, BufWriter, Write},
    path::Path,
    sync::{atomic::AtomicU64, Arc},
    time::Duration,
};

use compact_str::CompactString;
use rand_chacha::rand_core::{RngCore, SeedableRng};
use tokio::{
    sync::{broadcast, mpsc, Barrier},
    task::{JoinHandle, JoinSet},
};
use tracing::{debug, error, info, info_span, instrument, warn, Level};

use crate::source::{
    backend::Backend,
    iat::IatGenerator,
    worker::{Error as WorkerError, FunctionWorker, WorkerSync},
    Error, FunctionRow, MinuteRange,
};

type SecureRng = ::rand_chacha::ChaCha12Rng;

#[derive(Debug)]
pub struct SourceClient {
    workers: JoinSet<Result<u64, WorkerError>>,
    quit_tx: broadcast::Sender<()>,
    inv_log_h: Option<JoinHandle<Result<(), Error>>>,
}

impl SourceClient {
    pub const DEFAULT_FIXED_SEED: u64 = 0x0f0f_0f0f_0f0f_0f0fu64;

    #[allow(clippy::too_many_arguments)]
    pub fn new<G: IatGenerator, B: Backend>(
        csv_path: impl AsRef<Path>,
        inv_log_path: Option<impl AsRef<Path>>,
        seed: Option<u64>,
        iat_gen: G,
        invoc_id_start: u64,
        minute_range: MinuteRange,
        backend: B,
        minio_address: &str,
        bucket_name: &str,
    ) -> Result<Self, Error> {
        let mut rng = match seed {
            Some(0) => SecureRng::seed_from_u64(Self::DEFAULT_FIXED_SEED),
            Some(seed) => SecureRng::seed_from_u64(seed),
            None => SecureRng::from_entropy(),
        };

        // Initialize the InvocationLogger, if configured
        let inv_log = if let Some(path) = inv_log_path {
            let (to_inv_log, from_workers) = mpsc::channel(1 << 15);
            Some((to_inv_log, InvocationLogger::new(from_workers, path)?))
        } else {
            None
        };

        let rows = Self::parse_csv(&csv_path)?;
        // NOTE: Handling the parsing as a stream would prevent us from initializing the Barrier:
        let sync = Arc::new(WorkerSync {
            barrier: Barrier::new(rows.len()),
            invoc_id: AtomicU64::new(invoc_id_start),
        });
        let (quit_tx, _) = broadcast::channel(1);
        let mut workers = JoinSet::new();
        rows.into_iter()
            .try_for_each(|row| {
                FunctionWorker::new(
                    row,
                    rng.next_u64(),
                    iat_gen.clone(),
                    Arc::clone(&sync),
                    minute_range,
                    backend.clone(),
                    minio_address,
                    bucket_name,
                    inv_log.as_ref().map(|(to_inv_log, _)| InvocationLoggerRef {
                        tx: to_inv_log.clone(),
                    }),
                    quit_tx.subscribe(),
                )
                .map(|worker| {
                    workers.spawn(async move { worker.run().await });
                })
            })
            .map_err(Error::Worker)?;

        // Spawn the InvocationLogger, if configured
        let inv_log_h =
            inv_log.map(|(_, mut inv_log)| ::tokio::task::spawn_blocking(move || inv_log.run()));

        Ok(Self {
            workers,
            quit_tx,
            inv_log_h,
        })
    }

    #[instrument(level = Level::INFO, skip_all)]
    pub async fn run(&mut self, mut quit_rx: broadcast::Receiver<()>) -> Result<u64, Error> {
        let num_suscribers = self.quit_tx.send(()).expect("TODO"); // FIXME: error handling?
        assert_eq!(
            num_suscribers,
            self.workers.len(),
            "All Workers should be subscribed to quit_tx",
        );

        let mut num_requests = 0;
        loop {
            ::tokio::select! {
                res = quit_rx.recv() => {
                    warn!(received = ?res, "Received shutdown signal");
                    match self.quit_tx.send(()) {
                        Ok(num_suscribers) if num_suscribers == self.workers.len() => continue,
                        // NOTE: When we receive >1 shutdown signals, some Workers might have
                        // already dropped their Receiver, which currently leads to forceful
                        // abortion of all the rest Worker tasks:
                        Ok(num_suscribers) => warn!(
                            "subscribed = {num_suscribers}; expected = {}", self.workers.len(),
                        ),
                        Err(err) => error!(error = ?err, "Failed to broadcast quit signal: {err:#}"),
                    }
                    warn!("Forcefully aborting all Worker tasks...");
                    self.workers.abort_all();
                }
                wrk_res = self.workers.join_next() => {
                    match wrk_res {
                        Some(Ok(Ok(worker_requests))) => {
                            num_requests += worker_requests;
                            info!(?worker_requests, "Worker task joined successfully");
                        },
                        Some(Ok(Err(err))) => warn!(error = ?err, "Joined failed Worker task: {err:#}"),
                        Some(Err(jerr)) if jerr.is_cancelled() => warn!("Joined aborted Worker task"),
                        Some(Err(jerr)) => error!(error = ?jerr, "Failed to join Worker task: {jerr:#}"),
                        None => {
                            info!(?num_requests, "No more Worker tasks to join");
                            break;
                        }
                    }
                }
            }
        }

        // Reap the InvocationLogger (if configured), who must be waiting for all Workers to finish
        if let Some(inv_log_h) = self.inv_log_h.take() {
            match inv_log_h.await {
                Ok(Ok(())) => debug!("InvocationLogger task joined successfully"),
                Ok(Err(err)) => warn!(error = ?err, "Joined failed InvocationLogger: {err:#}"),
                Err(jerr) => error!(error = ?jerr, "Failed to join InvocationLogger: {jerr:#}"),
            }
        }
        Ok(num_requests)
    }

    pub fn parse_csv(csv_path: impl AsRef<Path>) -> Result<Vec<FunctionRow>, Error> {
        let mut br = BufReader::new(File::options().read(true).open(&csv_path).map_err(|err| {
            Error::Io {
                msg: format!("failed to open CSV file {:?}", csv_path.as_ref()).into_boxed_str(),
                source: err,
            }
        })?);

        // Discard the headers' line
        {
            let _nr = br.read_line(&mut String::new()).map_err(|err| Error::Io {
                msg: "failed to read headers line in CSV file".into(),
                source: err,
            })?;
        }

        let mut rdr = ::csv::ReaderBuilder::new()
            .has_headers(false)
            .from_reader(br);
        rdr.deserialize()
            .collect::<Result<_, _>>()
            .map_err(Error::CsvDeserialization)
    }
}

#[derive(Debug)]
struct InvocationLogger {
    bw: BufWriter<File>,
    rx: mpsc::Receiver<(CompactString, CompactString)>,
}

impl InvocationLogger {
    fn new(
        rx: mpsc::Receiver<(CompactString, CompactString)>,
        path: impl AsRef<Path>,
    ) -> Result<Self, Error> {
        let bw = BufWriter::new(
            File::options()
                .create_new(true)
                .write(true)
                .open(&path)
                .map_err(|err| Error::Io {
                    msg: format!(
                        "failed to create and open for writing file '{}'",
                        path.as_ref().display()
                    )
                    .into_boxed_str(),
                    source: err,
                })?,
        );
        Ok(Self { bw, rx })
    }

    fn run(&mut self) -> Result<(), Error> {
        let span = info_span!("invocation-logger");
        let _guard = span.entered();

        while let Some((function_id, invocation_id)) = self.rx.blocking_recv() {
            let line = ::serde_json::json!({ invocation_id: function_id });
            ::serde_json::to_writer(&mut self.bw, &line).map_err(Error::JsonSerialization)?;
            self.bw.write_all(b"\n").map_err(|err| Error::Io {
                msg: "failed to append newline".into(),
                source: err,
            })?;
        }
        info!("Flushing buffer and exiting...");
        self.bw.flush().map_err(|err| Error::Io {
            msg: "failed to flush buffer to file".into(),
            source: err,
        })
    }
}

#[derive(Debug, Clone)]
pub(super) struct InvocationLoggerRef {
    tx: mpsc::Sender<(CompactString, CompactString)>,
}

impl InvocationLoggerRef {
    #[inline]
    pub(crate) async fn log(&self, function_id: CompactString, invocation_id: CompactString) {
        use mpsc::error::SendTimeoutError;
        match self
            .tx
            .send_timeout((function_id, invocation_id), Duration::from_millis(50))
            .await
        {
            Ok(()) => {}
            Err(
                ref err @ SendTimeoutError::Timeout((ref f, ref i))
                | ref err @ SendTimeoutError::Closed((ref f, ref i)),
            ) => warn!(
                error = format!("{err:#}"), invocation_id = ?i, function_id = ?f,
                "Failed to log (FunctionID, InvocationID) pair",
            ),
        }
    }
}
