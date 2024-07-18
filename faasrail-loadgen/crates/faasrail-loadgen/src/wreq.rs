use compact_str::CompactString;
use serde::{Deserialize, Serialize};

#[inline(always)]
fn nan() -> f64 {
    f64::NAN
}

#[inline]
fn not_finite(float: &f64) -> bool {
    !float.is_finite()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkloadRequest {
    /// The average execution time of this workload, based on prior measurements.
    #[serde(skip_serializing_if = "not_finite")]
    #[serde(default = "nan")]
    mean: f64,

    /// The standard deviation of the execution time of this workload, based on prior measurements.
    #[serde(skip_serializing_if = "not_finite")]
    #[serde(default = "nan")]
    stdev: f64,

    /// The name of the benchmark.
    ///
    /// Maybe possible to use as `FunctionId`.
    pub bench: CompactString,

    /// The payload of the invocation request.
    pub payload: String,
}

impl PartialEq for WorkloadRequest {
    fn eq(&self, other: &Self) -> bool {
        self.bench.eq(&other.bench) && self.payload.eq(&other.payload)
    }
}

impl Eq for WorkloadRequest {}

impl PartialOrd for WorkloadRequest {
    fn partial_cmp(&self, other: &Self) -> Option<::std::cmp::Ordering> {
        Some(match self.bench.cmp(&other.bench) {
            ::std::cmp::Ordering::Equal => self.payload.cmp(&other.payload),
            lt_or_gt => lt_or_gt,
        })
    }
}

#[cfg(test)]
mod tests {
    use anyhow::{Context, Result};
    use compact_str::format_compact;
    use tracing::info;
    use tracing_test::traced_test;

    use super::{nan, WorkloadRequest};

    #[test]
    #[traced_test]
    fn serde_nan() -> Result<()> {
        let req = WorkloadRequest {
            mean: nan(),
            stdev: nan(),
            bench: format_compact!("test-workload"),
            payload: "test-payload".to_string(),
        };
        let ser = ::serde_json::to_string(&req).context("failed to serialize WorkloadRequest")?;
        info!("{req:#?}\n\t--> {ser:#?}");

        const SER: &str = "{\"bench\":\"test-workload\",\"payload\":\"test-payload\"}";
        let des = ::serde_json::from_str(SER).context("failed to deserialize WorkloadRequest")?;
        info!("{des:#?}");
        assert_eq!(req, des);

        Ok(())
    }
}
