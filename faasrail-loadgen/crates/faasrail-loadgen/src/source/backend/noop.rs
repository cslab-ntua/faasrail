use std::{convert::Infallible, time::Duration};

use tracing::{instrument, Level};

use crate::{source::backend::Backend, InvocationId, WorkloadRequest};

#[derive(Debug, Default, Clone, Copy)]
pub struct NoOp;

impl Backend for NoOp {
    type Error = Infallible;

    #[instrument(level = Level::INFO)]
    #[inline]
    async fn issue(
        &mut self,
        invocation_id: InvocationId,
        wreq: &WorkloadRequest,
        minute: u16,
        timeout: Duration,
    ) -> Result<(), Self::Error> {
        Ok(())
    }
}
