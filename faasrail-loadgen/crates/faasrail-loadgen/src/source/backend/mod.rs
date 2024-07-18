mod noop;
pub use noop::NoOp;

use std::{error::Error as stdError, fmt::Debug, future::Future, time::Duration};

use crate::{InvocationId, WorkloadRequest};

pub trait Backend: Clone + Debug + Send + 'static {
    type Error: stdError + Send + Sync + 'static;

    fn issue(
        &mut self,
        invocation_id: InvocationId,
        wreq: &WorkloadRequest,
        minute: u16,
        timeout: Duration,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}
