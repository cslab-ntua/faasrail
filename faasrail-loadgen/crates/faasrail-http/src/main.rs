mod sink;
mod source;

use std::{
    collections::HashMap,
    fs::File,
    io::{self, BufReader},
    path::{Path, PathBuf},
    str::FromStr,
};

use anyhow::{anyhow, bail, Context, Result};
use argh::FromArgs;
use compact_str::ToCompactString;
use futures::{stream::SelectAll, StreamExt};
use tokio::{
    signal::unix::{signal, SignalKind},
    sync::{broadcast, mpsc},
};
use tokio_stream::wrappers::SignalStream;
use tracing::{error, info, trace, warn};
use tracing_subscriber::{fmt::format::FmtSpan, EnvFilter};

use faasrail_loadgen::{
    sink::SinkClient,
    source::{MinuteRange, Poisson, SourceClient},
};

use crate::{
    sink::HttpSink,
    source::{FunctionId, HttpSource, TargetUris},
};

//const DEFAULT_SOURCE_ADDR: &str = "localhost:60051";
//const DEFAULT_SINK_ADDR: &str = "localhost:60052";
const DEFAULT_MINIO_HOSTPORT: &str = "localhost:59000";
const DEFAULT_MINIO_BUCKET_NAME: &str = "snaplace-fbpml";

/// rgv3 - request generator for FaaSCell
#[derive(Debug, FromArgs)]
struct Cli {
    /// path to input CSV file
    #[argh(option)]
    csv: PathBuf,

    /// path to log file for InvocationID-to-FunctionID mappings
    #[argh(option)]
    inv_log: Option<PathBuf>,

    ///// HOST:PORT formatted address of faascell's request source
    //#[argh(option, default = "String::from(DEFAULT_SOURCE_ADDR)")]
    //source_address: String,
    ///// HOST:PORT formatted address of faascell's response sink
    //#[argh(option, default = "String::from(DEFAULT_SINK_ADDR)")]
    //sink_address: String,
    //
    /// path to JSON file that contains Functions' target URIs
    #[argh(option)]
    target_uris: PathBuf,

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

fn parse_json_target_uris(path: impl AsRef<Path>) -> Result<Vec<TargetUris>> {
    let br = BufReader::with_capacity(
        1 << 14,
        File::options()
            .read(true)
            .open(&path)
            .with_context(|| format!("failed to open file '{}'", path.as_ref().display()))?,
    );
    let ::serde_json::Value::Object(doc) =
        ::serde_json::from_reader(br).context("failure during JSON deserialization from reader")?
    else {
        bail!(r#"failed to parse the top-level JSON object of the JSON document"#)
    };

    //
    // Exact URIs
    //
    let ::serde_json::Value::Object(functions) = doc
        .get("functions")
        .ok_or_else(|| anyhow!(r#"key "functions" not found in target URIs' JSON file"#))?
    else {
        bail!(r#"malformed target URIs' JSON object under the "functions" key"#)
    };
    //let uris = functions
    //    .into_iter()
    //    .map(|(fid, json_val)| {
    //        let ::serde_json::Value::String(uri) = json_val else {
    //            error!(r#"FunctionID "{fid}" is associated with JSON object {json_val:?}"#);
    //            bail!(r#"FunctionID "{fid}" is not associated with a URI"#)
    //        };
    //        let uri = ::hyper::Uri::from_str(uri)
    //            .with_context(|| format!(r#"failed to parse ::hyper::Uri from "{uri}""#))?;
    //        let function_id = FunctionId::from_str(fid)
    //            .with_context(|| format!(r#"failed to parse FunctionID from "{fid}""#))?;
    //        Ok((function_id, uri))
    //    })
    //    .collect::<Result<HashMap<_, _>, _>>()
    //    .context("failed to parse exact (FunctionID, URI) pairs")?;
    let uris = functions
        .into_iter()
        .map(|(fid, json_val)| match json_val {
            ::serde_json::Value::String(uri) => Ok((
                FunctionId::from_str(fid)
                    .with_context(|| format!(r#"failed to parse FunctionID from "{fid}""#))?,
                ::hyper::Uri::from_str(uri)
                    .with_context(|| format!(r#"failed to parse ::hyper::Uri from "{uri}""#))?,
            )),
            _ => {
                error!(r#"FunctionID "{fid}" is associated with JSON object {json_val:?}"#);
                bail!(r#"FunctionID "{fid}" is not associated with a URI"#)
            }
        })
        .collect::<Result<HashMap<_, _>, _>>()
        .context("failed to parse exact (FunctionID, URI) pairs")?;

    //
    // Rules
    //
    let ::serde_json::Value::Array(rules) = doc
        .get("rules")
        .ok_or_else(|| anyhow!(r#"key "rules" not found in target URIs' JSON file"#))?
    else {
        bail!(r#"malformed target URIs' JSON file under the "rules" key"#)
    };
    let rules = rules
        .iter()
        .map(|json_val| {
            json_val
                .as_object()
                .map(|rule| {
                    let fid_pat = rule
                        .get("pattern")
                        .and_then(|pat| pat.as_str())
                        .ok_or_else(|| {
                            anyhow!(r#"failed to get "pattern" in rule object {rule:?}"#)
                        })?
                        .to_compact_string();
                    let template = rule
                        .get("template")
                        .and_then(|tmpl| tmpl.as_str())
                        .ok_or_else(|| {
                            anyhow!(r#"failed to get "template" in rule object {rule:?}"#)
                        })?
                        .to_string();
                    Ok::<_, ::anyhow::Error>(TargetUris::Rule { fid_pat, template })
                })
                .ok_or_else(|| {
                    anyhow!(r#"failed to interpret this as a Rule object: "{json_val:?}""#)
                })?
        })
        .collect::<Result<Vec<_>, _>>()
        .context("failed to parse target URIs' rules from JSON")?;

    let mut ret = Vec::with_capacity(1 + rules.len());
    ret.push(TargetUris::Exact { uris });
    ret.extend(rules);
    Ok(ret)
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

    let target_uris = parse_json_target_uris(&cli.target_uris)
        .context("failed to parse Functions' target URIs")?;
    trace!("{target_uris:?}");

    let (shutdown, _) = broadcast::channel(1);
    setup_signals_handler(shutdown.clone())?;

    let (to_sink, from_source) = mpsc::channel(1 << 14); // FIXME(ckatsak): chan cap?

    let sink_backend = HttpSink::new(from_source).context("failed to spawn HttpSink")?;
    let sink_client =
        SinkClient::new(&cli.outfile, sink_backend).context("failed to create Sink client")?;
    let sink = ::tokio::spawn({
        let shutdown = shutdown.subscribe();
        async move { sink_client.run(shutdown).await }
    });

    let source_backend =
        HttpSource::spawn(&target_uris, to_sink).context("failed to spawn HttpSource")?;
    let mut source_client = SourceClient::new(
        &cli.csv,
        cli.inv_log.as_ref(),
        cli.seed,
        Poisson,
        cli.invoc_id,
        cli.minutes,
        source_backend.new_ref(),
        &cli.minio_address,
        &cli.minio_bucket,
    )
    .context("failed to create Source client")?;
    let source = ::tokio::spawn({
        let shutdown = shutdown.subscribe();
        async move { source_client.run(shutdown).await }
    });

    match ::tokio::try_join!(source, sink) {
        Ok((source, sink)) => {
            match source {
                Ok(num_requests) => info!(?num_requests, "Source task joined"),
                Err(err) => error!(error = ?err, "Joined failed Source task: {err:#}"),
            }
            match sink {
                Ok(num_responses) => info!(?num_responses, "Sink task joined"),
                Err(err) => error!(error = ?err, "Joined failed Sink task: {err:#}"),
            }
        }
        Err(err) => error!(error = ?err, "Failed to join Source and Sink tasks: {err:#}"),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use anyhow::{Context, Result};

    use crate::parse_json_target_uris;

    #[test]
    fn target_uris_01() -> Result<()> {
        let target_uris = parse_json_target_uris("test_target_uris.json5")
            .context("failed to parse TargetUris")?;
        eprintln!("{target_uris:#?}");
        Ok(())
    }
}
