use std::collections::HashMap;

use bytes::Bytes;
use compact_str::{CompactString, ToCompactString};
use http_body_util::BodyExt;
use hyper::{body::Incoming, Response};
use serde::Serialize;
use tokio::{
    sync::{broadcast, mpsc},
    task::JoinSet,
};
use tracing::{debug, error, info, instrument, trace, warn, Level};

use faasrail_loadgen::sink::SinkBackend;

#[derive(Debug, ::thiserror::Error)]
pub enum Error {
    #[error("failed to collect HTTP body")]
    ResponseBodyCollect(#[source] ::hyper::Error),
}

#[derive(Debug, Clone, Serialize)]
pub struct HttpResponse {
    status_code: u16,
    headers: HashMap<CompactString, CompactString>,
    body: Bytes,
}

#[derive(Debug)]
pub struct HttpSink {
    from_source: mpsc::Receiver<Response<Incoming>>,
}

impl HttpSink {
    pub fn new(from_source: mpsc::Receiver<Response<Incoming>>) -> Result<Self, Error> {
        Ok(Self { from_source })
    }

    #[instrument(level = Level::TRACE, skip_all, fields(_id))]
    async fn sink_worker(
        _id: usize,
        from_sink: ::flume::Receiver<Response<Incoming>>,
        to_appender: mpsc::Sender<HttpResponse>,
    ) -> Result<u64, Error> {
        let mut num_resp = 0;
        while let Ok(resp) = from_sink.recv_async().await {
            let (parts, inc) = resp.into_parts();
            let body = inc
                .collect()
                .await
                .map_err(Error::ResponseBodyCollect)?
                .to_bytes();
            trace!(?body);

            let resp = HttpResponse {
                status_code: parts.status.as_u16(),
                headers: parts
                    .headers
                    .into_iter()
                    .filter_map(|(k, v)| match (k, v.to_str()) {
                        (Some(k), Ok(v)) => Some((k.to_compact_string(), v.to_compact_string())),
                        (None, v) => {
                            warn!(value = ?v, "No header key associated with this value?!");
                            None
                        }
                        (k, Err(err)) => {
                            warn!(
                                error = ?err, key = ?k, value = ?v,
                                "Failed to convert HeaderValue to String: {err:#}",
                            );
                            None
                        }
                    })
                    .collect(),
                body,
            };
            num_resp += 1;
            if let Err(err) = to_appender.send(resp).await {
                // TODO(ckatsak):  ^^  `send_timeout()` ?
                error!(error = ?err, "Failed to send to appender: {err:#}");
            }
        }
        debug!(?num_resp, "Sink has dropped its sender; exiting...");
        Ok(num_resp)
    }
}

impl SinkBackend for HttpSink {
    type Error = Error;
    type Response = HttpResponse;

    #[instrument(level = Level::TRACE, skip_all)]
    async fn run(
        mut self,
        to_appender: mpsc::Sender<Self::Response>,
        mut quit_rx: broadcast::Receiver<()>,
    ) -> Result<u64, Self::Error> {
        let (to_sink_workers, from_sink) = ::flume::bounded(1 << 14);
        let mut sink_workers = (0..::num_cpus::get())
            .map(|id| {
                ::tokio::spawn({
                    let from_sink = from_sink.clone();
                    let to_appender = to_appender.clone();
                    async move { Self::sink_worker(id, from_sink, to_appender).await }
                })
            })
            .collect::<JoinSet<_>>();
        // NOTE(ckatsak): Drop the mpsc::Sender to allow FileAppender to exit once
        // all sink workers are done? Or maybe wait for `HttpSink` to exit?
        //drop(to_appender);

        loop {
            ::tokio::select! {
                res = quit_rx.recv() => {
                    warn!(received = ?res, "Notification from quit channel");
                    break;
                }

                opt_resp = self.from_source.recv() => {
                    match opt_resp {
                        Some(resp) => {
                            if let Err(err) = to_sink_workers.send_async(resp).await {
                                error!(
                                    error = ?err, "Failed to send to sink workers: {err:#}",
                                );
                            }
                        }
                        None => {
                            // Reaching this means that all `HttpSource`'s Senders have
                            // been closed, and thus there is no point in staying alive.
                            info!("All HttpSource's senders are closed; quitting...");
                            break;
                        }
                    }
                }
            }
        }

        let mut ret = 0;
        while let Some(jres) = sink_workers.join_next().await {
            match jres {
                Ok(Ok(Ok(num_resp))) => {
                    debug!(worker.responses = %num_resp, "Joined sink worker");
                    ret += num_resp;
                }
                Ok(Ok(Err(err))) => warn!(error = ?err, "Joined failed sink worker: {err:#}"),
                Ok(Err(jerr)) => warn!(error = ?jerr, "Failed to join sink worker: {jerr:#}"),
                Err(jer) => warn!(error = ?jer, "Failed to join next task from JoinSet: {jer:#}"),
            }
        }
        info!("All sink worker tasks have been joined; exiting...");

        Ok(ret)
    }
}
