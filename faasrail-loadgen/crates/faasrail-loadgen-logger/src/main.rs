mod source;
use source::Logger;

use std::{
    fs::File,
    io::{self, BufWriter},
    path::PathBuf,
};

use anyhow::{anyhow, Context, Result};
use argh::FromArgs;
use futures::{stream::SelectAll, StreamExt};
use tokio::{
    signal::unix::{signal, SignalKind},
    sync::broadcast,
};
use tokio_stream::wrappers::SignalStream;
use tracing::{error, info, trace, warn};
use tracing_subscriber::{fmt::format::FmtSpan, EnvFilter};

use faasrail_loadgen::{
    sink::{
        backend::{NoOp as NoOpSink, NoResponse},
        SinkClient,
    },
    source::{MinuteRange, Poisson, SourceClient},
};

const DEFAULT_MINIO_HOSTPORT: &str = "localhost:59000";
const DEFAULT_MINIO_BUCKET_NAME: &str = "snaplace-fbpml";

/// rgv3-logger - generator and logger of WorkloadRequests
#[derive(Debug, FromArgs)]
struct Cli {
    /// path to input CSV file
    #[argh(option)]
    csv: PathBuf,

    /// path to file where WorkloadRequests will be logged
    #[argh(option, short = 'r')]
    requests: PathBuf,

    /// path to SinkClient's output file
    #[argh(option, short = 'o')]
    outfile: String,

    /// u64 to seed PRNGs (default: system entropy)
    #[argh(option)]
    seed: Option<u64>,

    /// start of invocation ID range (default: 0)
    #[argh(option, default = "0")]
    invoc_id: u64,

    /// subset of input minutes to execute (default: all)
    #[argh(option, default = "MinuteRange::default()")]
    minutes: MinuteRange,

    /// HOST:PORT formatted address of MinIO server
    #[argh(option, default = "String::from(DEFAULT_MINIO_HOSTPORT)")]
    minio_address: String,
    /// name of the MinIO bucket
    #[argh(option, default = "String::from(DEFAULT_MINIO_BUCKET_NAME)")]
    minio_bucket: String,
}

fn setup_signals_handler(shutdown: broadcast::Sender<()>) -> Result<()> {
    let mut signals = [
        ("ALRM", signal(SignalKind::alarm())),
        ("HUP", signal(SignalKind::hangup())),
        ("INT", signal(SignalKind::interrupt())),
        ("QUIT", signal(SignalKind::quit())),
        ("TERM", signal(SignalKind::terminate())),
        ("USR1", signal(SignalKind::user_defined1())),
        ("USR2", signal(SignalKind::user_defined2())),
        ("PIPE", signal(SignalKind::pipe())),
    ]
    .into_iter()
    .try_fold(SelectAll::new(), |mut sig_stream, (sig, s)| {
        sig_stream.push(SignalStream::new(
            s.with_context(|| format!("failed to setup listener for SIG{sig}"))?,
        ));
        Ok::<_, ::anyhow::Error>(sig_stream)
    })
    .context("failed to setup signal listeners")?;

    let _h = ::tokio::spawn(async move {
        while signals.next().await.is_some() {
            warn!("Signal received; sending shutdown notification");
            if let Err(err) = shutdown.send(()) {
                error!(error = ?err, "Failed to send shutdown notification!");
                panic!("failed to send shutdown notification: {err:#}");
            }
        }
    });

    Ok(())
}

#[::tokio::main]
async fn main() -> Result<()> {
    ::tracing_subscriber::fmt()
        .with_writer(io::stderr)
        .with_env_filter(EnvFilter::from_default_env())
        .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
        .with_thread_ids(true)
        .with_line_number(true)
        //.with_thread_names(true)
        .try_init()
        .map_err(|err| anyhow!("failed to initialize tracing subscriber: {err:#}"))?;

    let cli = ::argh::from_env::<Cli>();
    trace!("{cli:?}");

    let (shutdown, _) = broadcast::channel(1);
    setup_signals_handler(shutdown.clone())?;

    let sink_backend = NoOpSink::<NoResponse>::default();
    let sink_client =
        SinkClient::new(&cli.outfile, sink_backend).context("failed to create Sink client")?;
    let sink = ::tokio::spawn({
        let shutdown = shutdown.subscribe();
        async move { sink_client.run(shutdown).await }
    });

    let source_backend = Logger::new(BufWriter::new(
        File::options()
            .create_new(true)
            .write(true)
            .open(&cli.requests)
            .with_context(|| {
                format!(
                    "failed to open requests log file '{}'",
                    cli.requests.display()
                )
            })?,
    ));
    let mut source_client = SourceClient::new(
        &cli.csv,
        None::<&str>,
        cli.seed,
        Poisson,
        cli.invoc_id,
        cli.minutes,
        source_backend
            .new_ref()
            .expect("Logger has not been run yet"),
        &cli.minio_address,
        &cli.minio_bucket,
    )
    .context("failed to create Source client")?;
    let logger = ::tokio::task::spawn_blocking(move || source_backend.run());
    let source = ::tokio::spawn({
        let shutdown = shutdown.subscribe();
        async move { source_client.run(shutdown).await }
    });

    match ::tokio::try_join!(source, logger, sink) {
        Ok((source, logger, sink)) => {
            match source {
                Ok(num_requests) => info!(?num_requests, "Source task joined"),
                Err(err) => error!(error = ?err, "Joined failed Source task: {err:#}"),
            }
            match logger {
                Ok(num_requests) => info!(?num_requests, "Logger task joined"),
                Err(err) => error!(error = ?err, "Joined failed Logger task: {err:#}"),
            }
            match sink {
                Ok(num_responses) => info!(?num_responses, "Sink task joined"),
                Err(err) => error!(error = ?err, "Joined failed Sink task: {err:#}"),
            }
        }
        Err(jerr) => {
            error!(error = ?jerr, "Failed to join Source, Logger and Sink tasks: {jerr:#}");
            return Err(jerr).context("Failed to join Source, Logger and Sink tasks");
        }
    }
    Ok(())
}
