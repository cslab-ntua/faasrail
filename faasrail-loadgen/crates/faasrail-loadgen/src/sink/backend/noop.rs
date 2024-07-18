use std::{convert::Infallible, fmt::Debug, marker::PhantomData};

use serde::Serialize;
use tokio::sync::{broadcast, mpsc};
use tracing::{info, instrument, Level};

#[derive(Debug, Default)]
pub struct NoOp<Resp> {
    waiting: bool,
    _phantom: PhantomData<fn() -> Resp>,
}

impl<Resp> NoOp<Resp> {
    pub fn new_waiting() -> Self {
        Self {
            waiting: true,
            _phantom: PhantomData,
        }
    }
}

impl<Resp> super::Backend for NoOp<Resp>
where
    Resp: Serialize + Send + Debug + 'static,
{
    type Error = Infallible;
    type Response = Resp;

    #[instrument(level = Level::INFO, skip_all)]
    async fn run(
        self,
        to_appender: mpsc::Sender<Self::Response>,
        mut quit_rx: broadcast::Receiver<()>,
    ) -> Result<u64, Self::Error> {
        if !self.waiting {
            return Ok(0);
        }

        // Let FileAppender die early...
        drop(to_appender);
        // ...and wait on the quit channel before exiting.
        let received = quit_rx.recv().await;
        info!(?received, "Received notification from the quit channel");
        Ok(0)
    }
}

#[derive(Debug, Default, Serialize)]
pub struct NoResponse;

::static_assertions::assert_impl_all!(crate::sink::client::SinkClient<NoOp<NoResponse>>: Send);
