use std::{
    borrow::Cow,
    collections::HashMap,
    convert::Infallible,
    io,
    sync::{Arc, RwLock},
    time::Duration,
};

use compact_str::CompactString;
use http_body_util::combinators::BoxBody;
use hyper::{
    body::{Bytes, Incoming},
    Request, Response,
};
use hyper_util::rt::TokioIo;
use tokio::{
    net::TcpStream,
    sync::{mpsc, oneshot},
    task::JoinSet,
};
use tracing::{debug, error, info, instrument, trace, warn, Level};

use faasrail_loadgen::{source::SourceBackend, InvocationId, WorkloadRequest};

pub type FunctionId = CompactString;

#[derive(Debug, ::thiserror::Error)]
pub enum Error {
    #[error("FunctionId '{0}' not found")]
    FunctionNotFound(FunctionId),

    #[error("failed to parse '{uri}' as a valid URI")]
    InvalidUri {
        uri: Box<str>,
        #[source]
        err: Option<::hyper::http::uri::InvalidUri>,
    },

    #[error("failed to connect(2)")]
    TcpConnection(#[source] io::Error),

    #[error("failed to build HTTP request")]
    RequestBuild(#[source] ::hyper::http::Error),

    #[error("failed to send HTTP request")]
    RequestSend(#[source] ::hyper::Error),

    #[error("failed to apply rule to create new Function")]
    RuleApplication,

    #[error("failed to forward request: {0}")]
    Channel(Box<str>),

    #[error("HTTP error")]
    Hyper(#[source] ::hyper::Error),
}

#[derive(Debug, Clone)]
pub enum TargetUris {
    Exact {
        uris: HashMap<FunctionId, ::hyper::Uri>,
    },
    Rule {
        /// Pattern to be replaced with the actual [`FunctionId`] in the `template`.
        fid_pat: CompactString,
        template: String,
    },
}

impl TargetUris {
    pub fn get(&self, function_id: &str) -> Result<Cow<::hyper::Uri>, Error> {
        match self {
            Self::Exact { uris } => uris
                .get(function_id)
                .map(Cow::Borrowed)
                .ok_or_else(|| Error::FunctionNotFound(FunctionId::from(function_id))),
            Self::Rule { fid_pat, template } => {
                let str_uri = template.replace(fid_pat.as_str(), function_id);
                str_uri.parse::<::hyper::Uri>().map_or_else(
                    |err| {
                        Err(Error::InvalidUri {
                            uri: str_uri.into_boxed_str(),
                            err: Some(err),
                        })
                    },
                    |uri| Ok(Cow::Owned(uri)),
                )
            }
        }
    }
}

#[derive(Debug, Clone)]
struct Function {
    uri: ::hyper::Uri,
    to_agent: mpsc::Sender<WorkloadRequest>,
}

#[derive(Debug)]
pub struct HttpSource {
    functions: Arc<RwLock<HashMap<FunctionId, Function>>>,
    func_agents: JoinSet<Result<(), Error>>,

    rules: Vec<TargetUris>,
    from_refs: mpsc::Receiver<RuleJob>,
    to_source: mpsc::Sender<RuleJob>,
    to_sink: mpsc::Sender<Response<Incoming>>,
}

impl HttpSource {
    /// TODO: Documentation
    ///
    /// # Note
    ///
    /// For now, `HttpSource` can work either with a set of exact URLs or with a single rule to
    /// construct URLs by replacing a pattern in a given template.
    ///
    /// In the future, we might want to extend this to support a combination of [`TargetUris`]
    /// that allows using exact URLs but also falling back to a (set of?) rule(s) that construct
    /// URLs for Functions that are not associated with any exact URL.
    #[instrument(level = Level::TRACE, skip_all)]
    pub fn spawn(
        targets: &[TargetUris],
        to_sink: mpsc::Sender<Response<Incoming>>,
    ) -> Result<Self, Error> {
        let (exact, mut rules) = targets.iter().fold(
            (
                HashMap::<FunctionId, ::hyper::Uri>::default(),
                Vec::with_capacity(targets.len()),
            ),
            |(mut exact, mut rules), target_uris| {
                match target_uris {
                    TargetUris::Exact { uris } => {
                        exact.extend(uris.clone().into_iter());
                    }
                    rule @ TargetUris::Rule { .. } => {
                        rules.push(rule.clone());
                    }
                }
                (exact, rules)
            },
        );
        rules.shrink_to_fit();
        //ret_uris.shrink_to_fit();

        let mut functions = HashMap::with_capacity(exact.len());
        // Spawn per-Function agents (tasks) for each existing Function:
        let func_agents = exact
            .into_iter()
            .map(|(function_id, uri)| {
                let (to_agent, from_source) = mpsc::channel(1 << 14); // FIXME: chan size
                functions.insert(
                    function_id,
                    Function {
                        uri: uri.clone(),
                        to_agent,
                    },
                );
                let to_sink = to_sink.clone();
                async move { Self::function_agent(from_source, to_sink, uri).await }
            })
            .collect::<JoinSet<_>>();

        let (to_source, from_refs) = mpsc::channel(1 << 14); // FIXME: chan size
        Ok(Self {
            functions: Arc::new(RwLock::new(functions)),
            func_agents,

            rules,
            to_source,
            from_refs,
            to_sink,
        })
    }

    #[instrument(level = Level::TRACE, skip_all)]
    pub async fn run(mut self) -> Result<(), Error> {
        //
        // TODO(ckatsak): ?
        //
        while let Some(rulejob) = self.from_refs.recv().await {
            for rule in &self.rules {
                match rule.get(&rulejob.function_id) {
                    Ok(uri) => {
                        let (to_agent, from_source) = mpsc::channel(1 << 14); // FIXME: chan size
                        let function = Function {
                            uri: uri.clone().into_owned(),
                            to_agent,
                        };
                        {
                            assert!(
                                self.functions
                                    .write()
                                    .expect("I hold the sole write lock, and I have not panicked")
                                    .insert(rulejob.function_id, function.clone())
                                    .is_none(),
                                "Function should not already exist",
                            );
                        }
                        self.func_agents.spawn({
                            let to_sink = self.to_sink.clone();
                            let uri = uri.into_owned();
                            async move { Self::function_agent(from_source, to_sink, uri).await }
                        });
                        rulejob.respond.send(function).map_err(|function| {
                            error!(?function, "Failed to forward response to HttpSourceRef");
                            Error::Channel(
                                String::from("HttpSource -> HttpSourceRef").into_boxed_str(),
                            )
                        })?;
                        break; // wait for next RuleJob
                    }
                    Err(err) => warn!(
                        error = ?err, ?rulejob.function_id, ?rule, "Failed to apply rule: {err:#}",
                    ),
                }
            }
        }
        info!("All HttpSourceRefs appear to have been dropped; exiting...");
        Ok(())
    }

    pub fn new_ref(&self) -> HttpSourceRef {
        HttpSourceRef {
            source_functions: self.functions.clone(),
            cached_functions: Default::default(),
            to_source: self.to_source.clone(),
        }
    }

    #[instrument(level = Level::TRACE, skip_all, fields(uri))]
    async fn function_agent(
        mut from_source: mpsc::Receiver<WorkloadRequest>,
        to_sink: mpsc::Sender<Response<Incoming>>,
        uri: ::hyper::Uri,
    ) -> Result<(), Error> {
        let host = uri.host().ok_or_else(|| Error::InvalidUri {
            uri: uri.to_string().into_boxed_str(),
            err: None,
        })?;
        let port = uri.port_u16().ok_or_else(|| Error::InvalidUri {
            uri: uri.to_string().into_boxed_str(),
            err: None,
        })?;
        let stream = TcpStream::connect(format!("{host}:{port}"))
            .await
            .map_err(Error::TcpConnection)?;
        let io = TokioIo::new(stream);

        let (mut sender, conn) =
            ::hyper::client::conn::http1::handshake::<_, BoxBody<Bytes, Infallible>>(io)
                .await
                .map_err(Error::Hyper)?;
        let conn_task = ::tokio::spawn(async move {
            if let Err(err) = conn.await {
                error!("Connection failed: {err:#}");
            }
        });

        // Each Function agent waits for new requests from an `HttpSourceRef` forever, until its
        // channel is closed and emptied. When this happens, it shuts down gracefully, dropping
        // its inbound channel (which allows `HttpSource` to know when all Function agents are
        // done), as well as its outbound channel (which allows `HttpSink` to know it too).
        while let Some(wreq) = from_source.recv().await {
            let req = Request::builder()
                //.version(::hyper::Version::HTTP_11)
                .uri(&uri)
                .header(
                    ::hyper::header::HOST,
                    uri.authority()
                        .expect("authority is checked to be present in the URI by now")
                        .as_str(),
                )
                .method(::hyper::Method::POST)
                .body(BoxBody::new(wreq.payload))
                .map_err(Error::RequestBuild)?;
            trace!(?req);

            let resp = sender.send_request(req).await.map_err(Error::RequestSend)?;
            trace!(?resp);
            if let Err(err) = to_sink.send(resp).await {
                // TODO(ckatsak):  ^^  `send_timeout()` ?
                error!(error = ?err, "Failed to forward HTTP response to HttpSink: {err:#}");
            }
        }
        debug!("Closing connection");
        conn_task.abort();

        Ok(())
    }
}

#[derive(Debug)]
struct RuleJob {
    function_id: FunctionId,
    respond: oneshot::Sender<Function>,
}

#[derive(Debug, Clone)]
pub struct HttpSourceRef {
    source_functions: Arc<RwLock<HashMap<FunctionId, Function>>>,
    // TODO(ckatsak): Since each `HttpSourceRef` ends up being used by a single `FunctionWorker`
    // (in crate `faasrail-loadgen`), it does **not** need to cache information about all
    // Functions; it can only cache information about its own Function instead.
    cached_functions: HashMap<FunctionId, Function>,
    to_source: mpsc::Sender<RuleJob>,
}

impl HttpSourceRef {
    /// # Panics
    ///
    /// - If [`HttpSource`]'s internal RWLock is poisoned (which can only mean that
    ///   [`HttpSource`] itself has panicked, since it holds the sole write lock).
    /// - If the provided `function_id` is already cached.
    #[inline]
    fn update_cache_from_source(&mut self, function_id: &str) {
        if let Some((function_id, function)) = self
            .source_functions
            .read()
            .expect("RWLock can be poisoned only if HttpSource has panicked")
            .get_key_value(function_id)
        {
            assert!(
                self.cached_functions
                    .insert(function_id.clone(), function.clone())
                    .is_none(),
                "should not be here if Function was already cached",
            )
        };
    }
}

impl SourceBackend for HttpSourceRef {
    type Error = Error;

    #[instrument(level = Level::TRACE, skip(self, wreq), fields(wreq.bench))]
    #[inline]
    async fn issue(
        &mut self,
        invocation_id: InvocationId,
        wreq: &WorkloadRequest,
        minute: u16,
        timeout: Duration,
    ) -> Result<(), Self::Error> {
        // If the Function is already cached, get its mpsc::Sender from the cache.
        match self.cached_functions.get(&wreq.bench) {
            Some(function) => &function.to_agent,
            None => {
                // If the Function is *not* already cached, we might be able to get its
                // mpsc::Sender from `HttpSource`'s shared HashMap and cache it for future use.
                self.update_cache_from_source(&wreq.bench);
                match self.cached_functions.get(&wreq.bench) {
                    Some(function) => &function.to_agent,
                    None => {
                        // If the Function was not present in `HttpSource`'s HashMap either,
                        // then we delegate the creation of a new Function agent to HttpSource
                        // and await it, to cache the result for future use.
                        let (tx, rx) = oneshot::channel();
                        if let Err(err) = self
                            .to_source
                            .send(RuleJob {
                                function_id: wreq.bench.clone(),
                                respond: tx,
                            })
                            .await
                        {
                            error!(error = ?err, "Failed to send RuleJob to HttpSource: {err:#}");
                            return Err(Error::RuleApplication);
                        }
                        match rx.await {
                            Ok(function) => {
                                assert!(
                                    self.cached_functions
                                        .insert(wreq.bench.clone(), function)
                                        .is_none(),
                                    "should not be here if Function was already cached",
                                );
                                &self
                                    .cached_functions
                                    .get(&wreq.bench)
                                    .expect("Function must have just been inserted")
                                    .to_agent
                            }
                            Err(err) => {
                                error!(error = ?err, "Failed to receive Function from HttpSource");
                                return Err(Error::RuleApplication);
                            }
                        }
                    }
                }
            }
        }
        .send(wreq.clone())
        .await
        .map_err(|err| {
            error!(error = ?err, "Failed to forward request to Function agent: {err:#}");
            Error::Channel(String::from("HttpSourceRef -> Function agent").into_boxed_str())
        })
    }
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;

    use compact_str::CompactString;

    use super::TargetUris;

    #[test]
    fn target_uris_parsing() {
        let turi = TargetUris::Rule {
            fid_pat: CompactString::const_new("@@FUNCTION_ID@@"),
            template: String::from("http://@@FUNCTION_ID@@-function.knative.com:12345/invoke"),
        };
        const CASES: &[(&str, &str)] =
            &[("skata", "http://skata-function.knative.com:12345/invoke")];

        for (function_id, uri) in CASES {
            assert_eq!(
                turi.get(function_id).unwrap(),
                Cow::<::hyper::Uri>::Owned(uri.parse().unwrap()),
            );
        }
    }
}
