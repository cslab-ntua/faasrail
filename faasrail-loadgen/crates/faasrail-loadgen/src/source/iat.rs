use std::{convert::Infallible, error::Error as stdError, iter::FusedIterator};

use rand::Rng;
use rand_distr::Distribution;

pub(crate) type MicroSeconds = u64;
const MICROSECONDS_PER_SECOND: f64 = 1e6;
const MICROSECONDS_PER_MINUTE: f64 = 60. * MICROSECONDS_PER_SECOND;

pub trait IatGenerator: Clone + Send + Sync + 'static {
    type Error: stdError + Send + Sync + 'static;

    fn gen<R: Rng + Send + Sync + 'static>(
        &self,
        rpm: u32,
        rng: R,
    ) -> Result<impl FusedIterator<Item = MicroSeconds> + Send + Sync + 'static, Self::Error>;
}

/// Exponential IATs (i.e., Poisson process)
#[derive(Debug, Clone, Copy)]
pub struct Poisson;
impl IatGenerator for Poisson {
    type Error = ::rand_distr::ExpError;

    fn gen<R: Rng + Send + Sync + 'static>(
        &self,
        rpm: u32,
        rng: R,
    ) -> Result<impl FusedIterator<Item = MicroSeconds> + Send + Sync + 'static, Self::Error> {
        // Divide RPM by 6e7 to calculate the rate of requests per μs, and use it as λ
        let exp = ::rand_distr::Exp::new(rpm as f64 / MICROSECONDS_PER_MINUTE)?.sample_iter(rng);

        let mut iats_sum = 0.;
        Ok(exp
            .take_while(move |&iat| {
                iats_sum += iat;
                iats_sum < MICROSECONDS_PER_MINUTE
            })
            .map(|iat| iat as u64))
    }
}

/// Uniformly random IATs
#[derive(Debug, Clone, Copy)]
pub struct Uniform;
impl IatGenerator for Uniform {
    type Error = Infallible;

    fn gen<R: Rng + Send + Sync + 'static>(
        &self,
        rpm: u32,
        rng: R,
    ) -> Result<impl FusedIterator<Item = MicroSeconds> + Send + Sync + 'static, Self::Error> {
        let uni = ::rand_distr::Uniform::new(0., 1.);
        let mut iat_sum = 0.;
        let iats = uni
            .sample_iter(rng)
            .take(rpm as usize)
            .inspect(|&iat| {
                iat_sum += iat;
            })
            .collect::<Vec<_>>();
        // FIXME: So far, Uniform is the slowest to run. Maybe reduce allocations?
        let ret = iats
            .into_iter()
            .map(move |iat| (iat * MICROSECONDS_PER_MINUTE / iat_sum) as u64);
        Ok(ret)
    }
}

/// Equidistant IATs
#[derive(Debug, Clone, Copy)]
pub struct Equidistant;
impl IatGenerator for Equidistant {
    type Error = Infallible;

    fn gen<R: Rng + Send + Sync + 'static>(
        &self,
        rpm: u32,
        _rng: R,
    ) -> Result<impl FusedIterator<Item = MicroSeconds> + Send + 'static, Self::Error> {
        // FIXME: The last one(s) may race with `minute_end` (let alone the intermediate overheads,
        // which probably manifest themselves towards the end)
        Ok(::std::iter::repeat((MICROSECONDS_PER_MINUTE / rpm as f64) as u64).take(rpm as usize))
    }
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use anyhow::{Context, Result};
    use rand::{rngs::SmallRng, SeedableRng};
    use tracing::debug;
    use tracing_test::traced_test;

    use super::{Equidistant, IatGenerator, Poisson, Uniform, MICROSECONDS_PER_SECOND};

    #[test]
    #[traced_test]
    fn poisson0() -> Result<()> {
        //let mut rng = SmallRng::seed_from_u64(crate::source::client::DEFAULT_FIXED_SEED);
        let rng = SmallRng::from_entropy();

        let p = Poisson;
        for lambda in &[3, 25, 50, 100, 200] {
            debug!("λ = {lambda}");
            let t_start = Instant::now();
            let iats_iter = p
                .gen(*lambda, rng.clone())
                .context("failed to generate IATs")?;
            let dur = t_start.elapsed();
            let iats = iats_iter.collect::<Vec<_>>();
            debug!("\t- Generated {} IATs in {dur:?}", iats.len());
            debug!("\t- Raw (μs) IATs: {iats:?}");
            debug!(
                "\t- IATs: {:?} (Σ = {:.3}s)",
                iats.iter()
                    .map(|&iat| format!("{:.6}s", iat as f64 / MICROSECONDS_PER_SECOND))
                    .collect::<Vec<_>>(),
                iats.iter()
                    .map(|&iat| iat as f64 / MICROSECONDS_PER_SECOND)
                    .sum::<f64>(),
            );
        }

        Ok(())
    }

    #[test]
    #[traced_test]
    fn uniform0() -> Result<()> {
        //let mut rng = SmallRng::seed_from_u64(crate::source::client::DEFAULT_FIXED_SEED);
        let rng = SmallRng::from_entropy();

        let u = Uniform;
        for rpm in &[3, 25, 50, 100, 200] {
            debug!("RPM = {rpm}");
            let t_start = Instant::now();
            let iats_iter = u
                .gen(*rpm, rng.clone())
                .context("failed to generate IATs")?;
            let dur = t_start.elapsed();
            let iats = iats_iter.collect::<Vec<_>>();
            debug!("\t- Generated {} IATs in {dur:?}", iats.len());
            debug!("\t- Raw (μs) IATs: {iats:?}");
            debug!(
                "\t- IATs: {:?} (Σ = {:.3}s)",
                iats.iter()
                    .map(|&iat| format!("{:.6}s", iat as f64 / MICROSECONDS_PER_SECOND))
                    .collect::<Vec<_>>(),
                iats.iter()
                    .map(|&iat| iat as f64 / MICROSECONDS_PER_SECOND)
                    .sum::<f64>(),
            );
        }

        Ok(())
    }

    #[test]
    #[traced_test]
    fn equidistant0() -> Result<()> {
        //let mut rng = SmallRng::seed_from_u64(crate::source::client::DEFAULT_FIXED_SEED);
        let rng = SmallRng::from_entropy();

        let e = Equidistant;
        for rpm in &[3, 25, 50, 100, 200] {
            debug!("RPM = {rpm}");
            let t_start = Instant::now();
            let iats_iter = e
                .gen(*rpm, rng.clone())
                .context("failed to generate IATs")?;
            let dur = t_start.elapsed();
            let iats = iats_iter.collect::<Vec<_>>();
            debug!("\t- Generated {} IATs in {dur:?}", iats.len());
            debug!("\t- Raw (μs) IATs: {iats:?}");
            debug!(
                "\t- IATs: {:?} (Σ = {:.3}s)",
                iats.iter()
                    .map(|&iat| format!("{:.6}s", iat as f64 / MICROSECONDS_PER_SECOND))
                    .collect::<Vec<_>>(),
                iats.iter()
                    .map(|&iat| iat as f64 / MICROSECONDS_PER_SECOND)
                    .sum::<f64>(),
            );
        }

        Ok(())
    }
}
