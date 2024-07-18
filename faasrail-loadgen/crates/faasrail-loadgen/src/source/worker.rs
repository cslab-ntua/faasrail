use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use compact_str::format_compact;
use rand_xoshiro::rand_core::SeedableRng;
use tokio::{
    sync::{broadcast, Barrier},
    time::{sleep, Instant},
};
use tracing::{debug, error, info, instrument, trace, warn, Level};

use crate::{
    source::{
        backend::Backend, client::InvocationLoggerRef, iat::IatGenerator, FunctionRow, MinuteRange,
    },
    WorkloadRequest,
};

#[derive(Debug, ::thiserror::Error)]
pub enum Error {
    #[error("JSON deserialization error: {msg}")]
    Deserialization {
        msg: Box<str>,
        #[source]
        source: ::serde_json::Error,
    },

    #[error("failed to adjust the payload of {wreq:?}")]
    FbpmlPayloadFix {
        wreq: Box<WorkloadRequest>,
        #[source]
        source: crate::fixer::Error,
    },

    #[error("failed to generate IATs")]
    IatGen {
        #[source]
        source: Box<dyn ::std::error::Error + Send + Sync + 'static>,
    },
}

/// Type alias to easily swap algorithms (e.g., to change it to [`rand_xoshiro::Xoshiro256Plus`]).
type FastRng = ::rand_xoshiro::Xoshiro256PlusPlus;

#[derive(Debug)]
pub(crate) struct WorkerSync {
    pub(crate) barrier: Barrier,
    pub(crate) invoc_id: AtomicU64,
}

#[derive(Debug)]
pub(crate) struct FunctionWorker<G: IatGenerator, B: Backend> {
    sync: Arc<WorkerSync>,
    minute_range: MinuteRange,

    _pavg: f64,
    rpm: Vec<u32>,
    wreq: WorkloadRequest,
    backend: B,
    rng: FastRng,
    iat_gen: G,

    inv_log: Option<InvocationLoggerRef>,
    quit_rx: broadcast::Receiver<()>,
}

impl<G: IatGenerator, B: Backend> FunctionWorker<G, B> {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        row: FunctionRow,
        seed: u64,
        iat_gen: G,
        sync: Arc<WorkerSync>,
        minute_range: MinuteRange,
        backend: B,
        minio_address: &str,
        bucket_name: &str,
        inv_log: Option<InvocationLoggerRef>,
        quit_rx: broadcast::Receiver<()>,
    ) -> Result<Self, Error> {
        let mut wreq =
            ::serde_json::from_str::<WorkloadRequest>(&row.mapped_wreq).map_err(|err| {
                Error::Deserialization {
                    msg: format!("mapped WorkloadRequest: {:?}", row.mapped_wreq).into_boxed_str(),
                    source: err,
                }
            })?;
        crate::fixer::fix_fbpml_payload(&mut wreq, minio_address, bucket_name).map_err(|err| {
            Error::FbpmlPayloadFix {
                wreq: Box::new(wreq.clone()),
                source: err,
            }
        })?;

        Ok(Self {
            sync,
            minute_range,

            _pavg: row.pavg,
            rpm: row.rpm,
            wreq,
            backend,

            iat_gen,
            rng: FastRng::seed_from_u64(seed),

            inv_log,
            quit_rx,
        })
    }

    #[instrument(level = Level::INFO, skip(self), fields(function_id = %self.wreq.bench, self._pavg))] // FIXME?
    pub async fn run(mut self) -> Result<u64, Error> {
        const ONE_MINUTE: Duration = Duration::from_secs(60);

        // Workers won't start until kicked
        self.quit_rx.recv().await.expect("TODO"); // FIXME: error handling
        let mut num_requests = 0;
        let t_start = Instant::now();

        let minute_end = sleep(ONE_MINUTE);
        // The actual duration is irrelevant here; we just need to initialize & pin it for now.
        ::tokio::pin!(minute_end);

        'minutes: for (minute, rpm) in (1..).zip(&self.rpm) {
            if minute < self.minute_range.start() {
                info!("Skipping minute {minute} < {}", self.minute_range.start());
                continue;
            }
            if minute > self.minute_range.end() {
                info!("Skipping remaining minutes > {}", self.minute_range.end());
                break;
            }

            let mut iats =
                self.iat_gen
                    .gen(*rpm, self.rng.clone())
                    .map_err(|err| Error::IatGen {
                        source: Box::new(err),
                    })?;

            self.sync.barrier.wait().await;
            info!(minute, rpm, "alive.for" = %::humantime::format_duration(t_start.elapsed()));

            minute_end.as_mut().reset(Instant::now() + ONE_MINUTE);
            loop {
                // NOTE: Keep the loop like this (rather than, e.g., `while let`) to make sure we
                // always await on `quit_rx` too.
                let iat = iats.next().map(Duration::from_micros).unwrap_or(ONE_MINUTE);

                ::tokio::select! {
                    biased;
                    // NOTE: Making this selection biased can severely affect the total number of
                    // requests produced. For instance, by moving the check for elapsed minute as
                    // the last choice, *all* requests produced by the IatGenerator end up being
                    // actually issued. Theoretically, these should stay within the minute, but
                    // realistically they don't, presumably because of overheads, which depend on
                    //  (1) the total number of Worker tasks (i.e., on the input number of
                    //      Functions), and
                    //  (2) the underlying environment (i.e., CPU cores, # of pthreads).
                    // By keeping this selection biased, while also checking whether the minute
                    // has elapsed _before_ sleeping on the produced IAT, we make sure that all
                    // Workers do their best to honor the minute limit, possibly at the cost of
                    // producing fewer requests than expected. Let's just do this for now.

                    quit_res = self.quit_rx.recv() => {
                        match quit_res {
                            Ok(()) => {
                                // TODO?
                                warn!("Received quit notification!");
                                //self.client_handle.abort();
                            }
                            Err(err) => error!("Quit channel unexpectedly emitted: {err:#}"),
                        }
                        // We should probably break out of the (outer) loop in any case
                        break 'minutes;
                    }

                    () = &mut minute_end => {
                        debug!(minute, rpm, "Minute elapsed");
                        break;
                    }

                    () = sleep(iat) => {
                        let invocation_id = format_compact!(
                            "{:024}",
                            self.sync.invoc_id.fetch_add(1, Ordering::AcqRel)
                        );
                        debug_assert!(!invocation_id.is_heap_allocated());
                        if let Err(err) = self
                            .backend
                            .issue(
                                invocation_id.clone(),
                                &self.wreq,
                                minute,
                                minute_end.deadline().duration_since(Instant::now()),
                            )
                            .await
                        {
                            error!(error = ?err, %invocation_id, "Failed to issue request");
                        } else {
                            num_requests += 1;
                            trace!(%invocation_id, ?num_requests, "Request issued successfully");
                            if let Some(ref inv_log) = self.inv_log {
                                inv_log.log(self.wreq.bench.clone(), invocation_id).await;
                            }
                        }
                    }
                }
            }
        }

        Ok(num_requests)
    }
}
