mod noop;
pub use noop::NoOp;
pub use noop::NoResponse;

use std::{error::Error as stdError, fmt::Debug, future::Future};

use serde::Serialize;
use tokio::sync::{broadcast, mpsc};

pub trait Backend: Debug + Send + 'static {
    type Error: stdError + Send + Sync + 'static;
    type Response: Serialize + Send + Debug;

    fn run(
        self,
        to_appender: mpsc::Sender<Self::Response>,
        quit_rx: broadcast::Receiver<()>,
    ) -> impl Future<Output = Result<u64, Self::Error>> + Send;
}
