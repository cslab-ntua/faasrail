use compact_str::CompactString;

use crate::WorkloadRequest;

const MINIO_ADDRESS_KEY: &str = "minio_address";
const BUCKET_NAME_KEY: &str = "bucket_name";

#[derive(Debug, ::thiserror::Error)]
pub enum Error {
    #[error("error during JSON serialization")]
    JsonSerialization(#[source] ::serde_json::Error),

    #[error("error during JSON deserialization")]
    JsonDeserialization(#[source] ::serde_json::Error),
}

#[derive(Debug, Clone)]
pub struct FbpmlPayloadFixer {
    minio_address: CompactString,
    bucket_name: CompactString,
}

impl FbpmlPayloadFixer {
    pub fn new(minio_address: &str, bucket_name: &str) -> Self {
        Self {
            minio_address: minio_address.into(),
            bucket_name: bucket_name.into(),
        }
    }

    #[inline]
    pub fn fix_payload(&self, req: &mut WorkloadRequest) -> Result<(), Error> {
        fix_fbpml_payload(req, &self.minio_address, &self.bucket_name)
    }
}

pub fn fix_fbpml_payload(
    req: &mut WorkloadRequest,
    minio_address: &str,
    bucket_name: &str,
) -> Result<(), Error> {
    let mut payload: ::serde_json::Map<String, ::serde_json::Value> =
        ::serde_json::from_str(&req.payload).map_err(Error::JsonDeserialization)?;

    if let Some(req_addr) = payload.get_mut(MINIO_ADDRESS_KEY) {
        match req_addr.as_str() {
            Some(req_addr) if req_addr == minio_address => (/* do nothing */),
            Some(_) | None => *req_addr = ::serde_json::Value::String(minio_address.into()),
        }
    }

    if let Some(req_bucket) = payload.get_mut(BUCKET_NAME_KEY) {
        match req_bucket.as_str() {
            Some(req_bucket) if req_bucket == bucket_name => (/* do nothing */),
            Some(_) | None => *req_bucket = ::serde_json::Value::String(bucket_name.into()),
        }
    }

    req.payload = ::serde_json::to_string(&payload).map_err(Error::JsonSerialization)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{
        fs::OpenOptions,
        io::BufReader,
        time::{Duration, Instant},
    };

    use anyhow::{Context, Result};
    use const_format::concatcp;
    use serde::Deserialize;
    use tracing::{debug, info};
    use tracing_test::traced_test;

    use super::FbpmlPayloadFixer;
    use crate::WorkloadRequest;

    const TEST_OUTPUT_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/artifacts/tests/");

    #[derive(Debug, Deserialize)]
    struct ImportedWorkload {
        #[allow(dead_code)]
        trace_exec_time_ms: Option<f64>,
        workload_request: WorkloadRequest,
    }

    #[test]
    #[traced_test]
    fn payload_fixer() -> Result<()> {
        const EXPORTED_PRETTY_FILEPATH: &str =
            concatcp!(TEST_OUTPUT_DIR, "exported_pretty_1000.json");
        const BUCK_NAME: &str = "THE-YOLO-BUCKET";
        const MINIO_ADDR: &str = "YOLO.CSLAB.ECE.NTUA.GR:59000";

        let br = BufReader::with_capacity(
            1 << 14,
            OpenOptions::new()
                .read(true)
                .open(EXPORTED_PRETTY_FILEPATH)
                .context("failed to open file w/ exported workload requests")?,
        );
        let wreqs: Vec<ImportedWorkload> =
            ::serde_json::from_reader(br).context("failed to deserialize ImportedWorkloads")?;

        let fixer = FbpmlPayloadFixer::new(MINIO_ADDR, BUCK_NAME);

        let (mut mean_dur, num_wreqs) = (Duration::ZERO, wreqs.len());
        wreqs.into_iter().for_each(|mut wreq| {
            debug!("orig: {:?}", wreq.workload_request);
            let start = Instant::now();
            match fixer.fix_payload(&mut wreq.workload_request) {
                Ok(()) => debug!("fixed: {:?}\n", wreq.workload_request),
                Err(err) => debug!("FAILED to fix payload: {err:#}\n"),
            }
            mean_dur += start.elapsed();
        });
        mean_dur /= u32::try_from(num_wreqs).expect("wreqs are fewer");
        info!("mean duration = {}", ::humantime::format_duration(mean_dur));

        Ok(())
    }
}
