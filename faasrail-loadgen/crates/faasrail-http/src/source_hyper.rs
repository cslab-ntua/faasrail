use std::{borrow::Cow, collections::HashMap, io, time::Duration};

use compact_str::CompactString;
use faasrail_loadgen::{source::SourceBackend, InvocationId, WorkloadRequest};
use hyper::client::conn::http1::handshake;
use hyper_util::rt::TokioIo;
use tokio::{
    net::TcpStream,
    select,
    sync::mpsc,
    task::{JoinError, JoinSet},
};
use tracing::{error, warn};
//use tracing::warn;

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

    #[error("HTTP error")]
    Hyper(#[source] ::hyper::Error),
    // TODO
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
    //pub fn get(&self, function_id: &str) -> Option<Cow<::hyper::Uri>> {
    //    match self {
    //        Self::Exact { uris } => uris.get(function_id).map(Cow::Borrowed),
    //        Self::Rule { fid_pat, template } => template
    //            .replace(fid_pat.as_str(), function_id)
    //            .parse::<::hyper::Uri>()
    //            .inspect_err(|err| {
    //                warn!(
    //                    "Failed to parse '{}' as URI: {err:#}",
    //                    template.replace(fid_pat.as_str(), function_id),
    //                )
    //            })
    //            .map(Cow::Owned)
    //            .ok(),
    //    }
    //}
    pub fn get(&self, function_id: &str) -> Result<Cow<::hyper::Uri>, Error> {
        match self {
            Self::Exact { uris } => uris
                .get(function_id)
                .map(Cow::Borrowed)
                .ok_or_else(|| Error::FunctionNotFound(FunctionId::from(function_id))),
            //Self::Rule { fid_pat, template } => template
            //    .replace(fid_pat.as_str(), function_id)
            //    .parse::<::hyper::Uri>()
            //    .map_or_else(|err| Err(Error::InvalidUri(err)), |uri| Ok(Cow::Owned(uri))),
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
    to_agent: mpsc::Sender<RequestInfo>,
}

#[derive(Debug, Clone)]
struct RequestInfo {
    invocation_id: InvocationId,
    wreq: WorkloadRequest,
    minute: u16,
    timeout: Duration,
}

#[derive(Debug)]
pub struct HttpSource {
    functions: HashMap<FunctionId, Function>,
    func_agents: JoinSet<Result<Result<(), Error>, JoinError>>,

    //exact: HashMap<FunctionId, ::hyper::Uri>,
    rules: Vec<TargetUris>,
    //
    // TODO
    //
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
    pub fn spawn(targets: &[TargetUris]) -> Result<Self, Error> {
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
        let func_agents = exact
            .into_iter()
            .map(|(function_id, uri)| {
                let (to_agent, from_source) = mpsc::channel(1 << 14); // FIXME: chan size
                let uri2 = uri.clone();
                functions.insert(function_id, Function { uri, to_agent });
                ::tokio::spawn(async move { Self::function_agent(from_source, uri2).await })
            })
            .collect::<JoinSet<_>>();
        //
        // TODO
        //
        Ok(Self {
            functions,
            func_agents,

            //exact: todo!(),
            rules,
        })
    }

    async fn function_agent(
        mut from_source: mpsc::Receiver<RequestInfo>,
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

        let (mut sender, conn) = handshake(io).await.map_err(Error::Hyper)?;
        let mut conn_task = ::tokio::spawn(async move {
            if let Err(err) = conn.await {
                error!("Connection failed: {err:#}");
            }
        });
        // TODO(ckatsak): Initialize ::hyper::Client

        loop {
            select! {
                opt_reqinfo = from_source.recv() => {
                    match opt_reqinfo {
                        Some(reqinfo) => {
                            // TODO(ckatsak): Send request
                        }
                        None => {
                            conn_task.abort();
                            break;
                        }
                    }
                }

                // TODO
            }
        }

        while let Some(reqinfo) = from_source.recv().await {
            // TODO
            todo!()
        }
        conn_task.abort();

        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct HttpSourceRef {
    // TODO
}

impl SourceBackend for HttpSourceRef {
    type Error = Error;

    async fn issue(
        &mut self,
        invocation_id: InvocationId,
        wreq: &WorkloadRequest,
        minute: u16,
        timeout: Duration,
    ) -> Result<(), Self::Error> {
        let req_info = RequestInfo {
            invocation_id,
            wreq: wreq.clone(),
            minute,
            timeout,
        };
        //
        // TODO
        //
        todo!()
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
