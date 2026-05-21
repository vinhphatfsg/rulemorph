use std::str::FromStr;
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub(in crate::endpoint_engine) struct NetworkRetry {
    #[serde(default)]
    max: Option<u32>,
    #[serde(default)]
    backoff: Option<String>,
    #[serde(default)]
    initial_delay: Option<String>,
}

#[derive(Debug, Clone)]
pub(in crate::endpoint_engine) struct RetryConfig {
    pub(in crate::endpoint_engine) max: u32,
    pub(in crate::endpoint_engine) backoff: RetryBackoff,
    pub(in crate::endpoint_engine) initial_delay: Duration,
}

#[derive(Debug, Clone, Copy)]
pub(in crate::endpoint_engine) enum RetryBackoff {
    Fixed,
    Linear,
    Exponential,
}

pub(in crate::endpoint_engine) fn parse_duration(value: &str) -> Result<Duration> {
    let trimmed = value.trim();
    if let Some(ms) = trimmed.strip_suffix("ms") {
        let amount = u64::from_str(ms.trim()).context("invalid ms")?;
        return Ok(Duration::from_millis(amount));
    }
    if let Some(sec) = trimmed.strip_suffix('s') {
        let amount = u64::from_str(sec.trim()).context("invalid s")?;
        return Ok(Duration::from_secs(amount));
    }
    Err(anyhow!("invalid duration: {}", value))
}

pub(in crate::endpoint_engine) fn compile_retry(
    raw: Option<&NetworkRetry>,
) -> Result<Option<RetryConfig>> {
    let Some(raw) = raw else {
        return Ok(None);
    };
    let max = raw.max.unwrap_or(0);
    if max == 0 {
        return Ok(None);
    }
    let backoff = match raw.backoff.as_deref().unwrap_or("fixed") {
        "fixed" => RetryBackoff::Fixed,
        "linear" => RetryBackoff::Linear,
        "exponential" => RetryBackoff::Exponential,
        other => return Err(anyhow!("invalid retry backoff: {}", other)),
    };
    let initial_delay = match raw.initial_delay.as_deref() {
        Some(value) => parse_duration(value)?,
        None => Duration::from_millis(100),
    };
    Ok(Some(RetryConfig {
        max,
        backoff,
        initial_delay,
    }))
}

impl RetryConfig {
    pub(in crate::endpoint_engine) fn delay_for(&self, attempt: u32) -> Duration {
        let factor = attempt.saturating_add(1);
        match self.backoff {
            RetryBackoff::Fixed => self.initial_delay,
            RetryBackoff::Linear => self
                .initial_delay
                .checked_mul(factor)
                .unwrap_or(Duration::MAX),
            RetryBackoff::Exponential => {
                let exp = 2u32.saturating_pow(attempt);
                self.initial_delay.checked_mul(exp).unwrap_or(Duration::MAX)
            }
        }
    }
}
