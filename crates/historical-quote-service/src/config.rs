use std::collections::BTreeSet;
use std::env;
use std::net::SocketAddr;
use std::str::FromStr;
use std::time::Duration;

use historical_quote::api::{Backend, ServiceLimits, API_REVISION};

use crate::jobs::JobLimits;

const DEFAULT_BIND_ADDRESS: &str = "0.0.0.0:3100";
const DEFAULT_MAX_RUNNING_JOBS: usize = 4;
const DEFAULT_MAX_WAITING_JOBS: usize = 16;
const DEFAULT_MAX_TERMINAL_JOBS: usize = 20;
const DEFAULT_TERMINAL_TTL_SECONDS: u64 = 3_600;
pub const DEFAULT_MAX_COMBINATION_COUNT: u64 = 20_000;
pub const DEFAULT_MAX_BLOCK_SPAN: u64 = 1_800;
pub const DEFAULT_TIMEOUT_MS: u64 = 600_000;
pub const DEFAULT_MAX_TIMEOUT_MS: u64 = 900_000;
pub const DEFAULT_JOB_DECODED_BYTE_RESERVATION: u64 = 1024 * 1024 * 1024;
pub const DEFAULT_DECODED_BYTE_BUDGET: u64 = 4 * 1024 * 1024 * 1024;
pub const DEFAULT_MAX_CONSISTENCY_PAIRS: usize = 100;
pub const DEFAULT_MAX_QUOTE_SELECTORS: usize = 20;
pub const DEFAULT_MAX_AMOUNTS_PER_QUOTE: usize = 20;
pub const DEFAULT_MAX_STRUCTURED_DIFFERENCES: usize = 100;
const DEFAULT_DATABASE_PORT: u16 = 5_432;
const DEFAULT_DATABASE_CONNECTIONS: usize = 5;
const DEFAULT_DATABASE_CONNECTION_LIFETIME_SECONDS: u64 = 3_600;
const DEFAULT_SHUTDOWN_TIMEOUT_SECONDS: u64 = 25;

#[derive(Clone)]
pub struct ServiceConfig {
    pub bind_address: SocketAddr,
    pub supported_chain_id: u64,
    pub service_revision: String,
    pub enabled_backends: Vec<Backend>,
    pub expected_bearer_digest: [u8; 32],
    pub scheduler: JobLimits,
    pub max_combination_count: u64,
    pub max_block_span: u64,
    pub default_timeout_ms: u64,
    pub max_timeout_ms: u64,
    pub job_decoded_byte_reservation: u64,
    pub max_consistency_pairs: usize,
    pub max_quote_selectors: usize,
    pub max_amounts_per_quote: usize,
    pub max_structured_differences: usize,
    pub database: ReplicaDatabaseConfig,
    pub object_store: ObjectStoreConfig,
    pub shutdown_timeout: Duration,
}

#[derive(Clone)]
pub struct ReplicaDatabaseConfig {
    pub host: String,
    pub port: u16,
    pub database: String,
    pub username: String,
    pub max_connections: usize,
    pub max_connection_lifetime: Duration,
}

#[derive(Clone)]
pub struct ObjectStoreConfig {
    pub bucket: String,
    pub prefix: String,
    pub region: String,
    pub endpoint_url: Option<String>,
    pub force_path_style: bool,
}

impl ServiceConfig {
    /// Loads the strict production configuration from the process environment.
    ///
    /// Workload limits default to the checked benchmark envelope. Deployment
    /// configuration can override them explicitly.
    ///
    /// # Errors
    ///
    /// Returns an error when a required value is absent, malformed, or unsafe.
    pub fn from_env() -> Result<Self, ConfigError> {
        let bind_address = match env::var("DSOLVER_HISTORY_BIND_ADDRESS") {
            Ok(value) => parse_value("DSOLVER_HISTORY_BIND_ADDRESS", &value)?,
            Err(_) => parse_value("DSOLVER_HISTORY_BIND_ADDRESS", DEFAULT_BIND_ADDRESS)?,
        };
        let service_revision = required("DSOLVER_HISTORY_SERVICE_REVISION")?;
        let api_revision: u32 = parse_required("DSOLVER_HISTORY_API_REVISION")?;
        if api_revision != API_REVISION {
            return Err(ConfigError::Invalid(
                "configured API revision must match the compiled revision".to_owned(),
            ));
        }
        let supported_chain_id: u64 = parse_required("DSOLVER_HISTORY_CHAIN_ID")?;
        if supported_chain_id != 8_453 {
            return Err(ConfigError::Invalid(
                "historical quote service supports Base chain ID 8453 only".to_owned(),
            ));
        }
        let enabled_backends = parse_backends(&required("DSOLVER_HISTORY_ENABLED_BACKENDS")?)?;
        let expected_bearer_digest = parse_digest(&required("DSOLVER_HISTORY_TOKEN_SHA256")?)?;
        let workload = workload_config()?;

        let username = required("DSOLVER_HISTORY_REPLICA_DB_USER")?;
        if username != "state_history_observer" {
            return Err(ConfigError::Invalid(
                "replica database user must be state_history_observer".to_owned(),
            ));
        }

        let config = Self {
            bind_address,
            supported_chain_id,
            service_revision,
            enabled_backends,
            expected_bearer_digest,
            scheduler: workload.scheduler,
            max_combination_count: workload.max_combination_count,
            max_block_span: workload.max_block_span,
            default_timeout_ms: workload.default_timeout_ms,
            max_timeout_ms: workload.max_timeout_ms,
            job_decoded_byte_reservation: workload.job_decoded_byte_reservation,
            max_consistency_pairs: workload.max_consistency_pairs,
            max_quote_selectors: workload.max_quote_selectors,
            max_amounts_per_quote: workload.max_amounts_per_quote,
            max_structured_differences: workload.max_structured_differences,
            database: database_config(username)?,
            object_store: object_store_config()?,
            shutdown_timeout: Duration::from_secs(parse_optional(
                "DSOLVER_HISTORY_SHUTDOWN_TIMEOUT_SECONDS",
                DEFAULT_SHUTDOWN_TIMEOUT_SECONDS,
            )?),
        };
        config.validate()?;
        Ok(config)
    }

    #[must_use]
    pub fn public_limits(&self) -> ServiceLimits {
        ServiceLimits {
            max_combination_count: self.max_combination_count,
            max_block_span: self.max_block_span,
            default_timeout_ms: self.default_timeout_ms,
            max_timeout_ms: self.max_timeout_ms,
            max_running_jobs: usize_to_u64(self.scheduler.max_running),
            max_waiting_jobs: usize_to_u64(self.scheduler.max_waiting),
            decoded_byte_budget: self.scheduler.decoded_byte_budget,
            max_terminal_jobs: usize_to_u64(self.scheduler.max_terminal_jobs),
            terminal_retention_seconds: self.scheduler.terminal_ttl.as_secs(),
            max_consistency_pairs: usize_to_u64(self.max_consistency_pairs),
            max_quote_selectors: usize_to_u64(self.max_quote_selectors),
            max_amounts_per_quote: usize_to_u64(self.max_amounts_per_quote),
            max_structured_differences: usize_to_u64(self.max_structured_differences),
        }
    }

    #[must_use]
    pub const fn api_revision(&self) -> u32 {
        API_REVISION
    }

    fn validate(&self) -> Result<(), ConfigError> {
        let positive = self.scheduler.max_running > 0
            && self.scheduler.max_waiting > 0
            && self.scheduler.decoded_byte_budget > 0
            && self.scheduler.max_terminal_jobs > 0
            && !self.scheduler.terminal_ttl.is_zero()
            && self.max_combination_count > 0
            && self.max_block_span > 0
            && self.default_timeout_ms > 0
            && self.max_timeout_ms > 0
            && self.job_decoded_byte_reservation > 0
            && self.max_consistency_pairs > 0
            && self.max_quote_selectors > 0
            && self.max_amounts_per_quote > 0
            && self.max_structured_differences > 0
            && self.database.max_connections > 0
            && !self.database.max_connection_lifetime.is_zero()
            && !self.shutdown_timeout.is_zero();
        if !positive {
            return Err(ConfigError::Invalid(
                "service limits must be positive".to_owned(),
            ));
        }
        Ok(())
    }
}

struct WorkloadConfig {
    scheduler: JobLimits,
    max_combination_count: u64,
    max_block_span: u64,
    default_timeout_ms: u64,
    max_timeout_ms: u64,
    job_decoded_byte_reservation: u64,
    max_consistency_pairs: usize,
    max_quote_selectors: usize,
    max_amounts_per_quote: usize,
    max_structured_differences: usize,
}

fn workload_config() -> Result<WorkloadConfig, ConfigError> {
    let decoded_byte_budget = parse_optional(
        "DSOLVER_HISTORY_DECODED_BYTE_BUDGET",
        DEFAULT_DECODED_BYTE_BUDGET,
    )?;
    let default_timeout_ms =
        parse_optional("DSOLVER_HISTORY_DEFAULT_TIMEOUT_MS", DEFAULT_TIMEOUT_MS)?;
    let max_timeout_ms = parse_optional("DSOLVER_HISTORY_MAX_TIMEOUT_MS", DEFAULT_MAX_TIMEOUT_MS)?;
    if default_timeout_ms > max_timeout_ms {
        return Err(ConfigError::Invalid(
            "default timeout must not exceed maximum timeout".to_owned(),
        ));
    }
    let job_decoded_byte_reservation = parse_optional(
        "DSOLVER_HISTORY_JOB_DECODED_BYTE_RESERVATION",
        DEFAULT_JOB_DECODED_BYTE_RESERVATION,
    )?;
    if job_decoded_byte_reservation > decoded_byte_budget {
        return Err(ConfigError::Invalid(
            "job decoded-byte reservation must not exceed the global budget".to_owned(),
        ));
    }

    Ok(WorkloadConfig {
        scheduler: JobLimits {
            max_running: parse_optional(
                "DSOLVER_HISTORY_MAX_RUNNING_JOBS",
                DEFAULT_MAX_RUNNING_JOBS,
            )?,
            max_waiting: parse_optional(
                "DSOLVER_HISTORY_MAX_WAITING_JOBS",
                DEFAULT_MAX_WAITING_JOBS,
            )?,
            decoded_byte_budget,
            max_terminal_jobs: parse_optional(
                "DSOLVER_HISTORY_MAX_TERMINAL_JOBS",
                DEFAULT_MAX_TERMINAL_JOBS,
            )?,
            terminal_ttl: Duration::from_secs(parse_optional(
                "DSOLVER_HISTORY_TERMINAL_TTL_SECONDS",
                DEFAULT_TERMINAL_TTL_SECONDS,
            )?),
        },
        max_combination_count: parse_optional(
            "DSOLVER_HISTORY_MAX_COMBINATION_COUNT",
            DEFAULT_MAX_COMBINATION_COUNT,
        )?,
        max_block_span: parse_optional("DSOLVER_HISTORY_MAX_BLOCK_SPAN", DEFAULT_MAX_BLOCK_SPAN)?,
        default_timeout_ms,
        max_timeout_ms,
        job_decoded_byte_reservation,
        max_consistency_pairs: parse_optional(
            "DSOLVER_HISTORY_MAX_CONSISTENCY_PAIRS",
            DEFAULT_MAX_CONSISTENCY_PAIRS,
        )?,
        max_quote_selectors: parse_optional(
            "DSOLVER_HISTORY_MAX_QUOTE_SELECTORS",
            DEFAULT_MAX_QUOTE_SELECTORS,
        )?,
        max_amounts_per_quote: parse_optional(
            "DSOLVER_HISTORY_MAX_AMOUNTS_PER_QUOTE",
            DEFAULT_MAX_AMOUNTS_PER_QUOTE,
        )?,
        max_structured_differences: parse_optional(
            "DSOLVER_HISTORY_MAX_STRUCTURED_DIFFERENCES",
            DEFAULT_MAX_STRUCTURED_DIFFERENCES,
        )?,
    })
}

fn usize_to_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

fn database_config(username: String) -> Result<ReplicaDatabaseConfig, ConfigError> {
    Ok(ReplicaDatabaseConfig {
        host: required("DSOLVER_HISTORY_REPLICA_DB_HOST")?,
        port: parse_optional("DSOLVER_HISTORY_REPLICA_DB_PORT", DEFAULT_DATABASE_PORT)?,
        database: required("DSOLVER_HISTORY_REPLICA_DB_NAME")?,
        username,
        max_connections: parse_optional(
            "DSOLVER_HISTORY_REPLICA_DB_MAX_CONNECTIONS",
            DEFAULT_DATABASE_CONNECTIONS,
        )?,
        max_connection_lifetime: Duration::from_secs(parse_optional(
            "DSOLVER_HISTORY_REPLICA_DB_CONNECTION_LIFETIME_SECONDS",
            DEFAULT_DATABASE_CONNECTION_LIFETIME_SECONDS,
        )?),
    })
}

fn object_store_config() -> Result<ObjectStoreConfig, ConfigError> {
    Ok(ObjectStoreConfig {
        bucket: required("DSOLVER_HISTORY_S3_BUCKET")?,
        prefix: required("DSOLVER_HISTORY_S3_PREFIX")?,
        region: required("AWS_REGION")?,
        endpoint_url: env::var("DSOLVER_HISTORY_S3_ENDPOINT_URL").ok(),
        force_path_style: parse_bool_optional("DSOLVER_HISTORY_S3_FORCE_PATH_STYLE", false)?,
    })
}

fn required(name: &'static str) -> Result<String, ConfigError> {
    let value = env::var(name).map_err(|_| ConfigError::Missing(name))?;
    if value.is_empty() {
        return Err(ConfigError::Malformed(name));
    }
    Ok(value)
}

fn parse_required<T>(name: &'static str) -> Result<T, ConfigError>
where
    T: FromStr,
{
    parse_value(name, &required(name)?)
}

fn parse_optional<T>(name: &'static str, default: T) -> Result<T, ConfigError>
where
    T: FromStr,
{
    match env::var(name) {
        Ok(value) => parse_value(name, &value),
        Err(_) => Ok(default),
    }
}

fn parse_value<T>(name: &'static str, value: &str) -> Result<T, ConfigError>
where
    T: FromStr,
{
    value.parse().map_err(|_| ConfigError::Malformed(name))
}

fn parse_bool_optional(name: &'static str, default: bool) -> Result<bool, ConfigError> {
    parse_optional(name, default)
}

fn parse_backends(value: &str) -> Result<Vec<Backend>, ConfigError> {
    let mut backends = Vec::new();
    let mut distinct = BTreeSet::new();
    for raw in value.split(',') {
        let backend = match raw.trim() {
            "native" => Backend::Native,
            "vm" => Backend::Vm,
            "rfq" => Backend::Rfq,
            _ => return Err(ConfigError::Malformed("DSOLVER_HISTORY_ENABLED_BACKENDS")),
        };
        if !distinct.insert(backend) {
            return Err(ConfigError::Invalid(
                "enabled backends must be duplicate-free".to_owned(),
            ));
        }
        backends.push(backend);
    }
    if backends.is_empty() {
        return Err(ConfigError::Invalid(
            "at least one backend must be enabled".to_owned(),
        ));
    }
    Ok(backends)
}

fn parse_digest(value: &str) -> Result<[u8; 32], ConfigError> {
    let decoded =
        hex::decode(value).map_err(|_| ConfigError::Malformed("DSOLVER_HISTORY_TOKEN_SHA256"))?;
    decoded
        .try_into()
        .map_err(|_| ConfigError::Malformed("DSOLVER_HISTORY_TOKEN_SHA256"))
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("required configuration is missing: {0}")]
    Missing(&'static str),
    #[error("configuration value is malformed: {0}")]
    Malformed(&'static str),
    #[error("configuration is invalid: {0}")]
    Invalid(String),
}

#[cfg(test)]
mod tests {
    use super::{
        DEFAULT_DATABASE_CONNECTIONS, DEFAULT_DECODED_BYTE_BUDGET,
        DEFAULT_JOB_DECODED_BYTE_RESERVATION, DEFAULT_MAX_AMOUNTS_PER_QUOTE,
        DEFAULT_MAX_BLOCK_SPAN, DEFAULT_MAX_COMBINATION_COUNT, DEFAULT_MAX_CONSISTENCY_PAIRS,
        DEFAULT_MAX_QUOTE_SELECTORS, DEFAULT_MAX_STRUCTURED_DIFFERENCES, DEFAULT_MAX_TIMEOUT_MS,
        DEFAULT_TIMEOUT_MS,
    };

    #[test]
    fn measured_workload_defaults_cover_the_release_benchmark_envelope() {
        assert_eq!(DEFAULT_MAX_COMBINATION_COUNT, 20_000);
        assert_eq!(DEFAULT_MAX_BLOCK_SPAN, 1_800);
        assert_eq!(DEFAULT_TIMEOUT_MS, 600_000);
        assert_eq!(DEFAULT_MAX_TIMEOUT_MS, 900_000);
        assert_eq!(DEFAULT_JOB_DECODED_BYTE_RESERVATION, 1024 * 1024 * 1024);
        assert_eq!(DEFAULT_DECODED_BYTE_BUDGET, 4 * 1024 * 1024 * 1024);
        assert_eq!(DEFAULT_DATABASE_CONNECTIONS, 5);
        assert_eq!(DEFAULT_MAX_CONSISTENCY_PAIRS, 100);
        assert_eq!(DEFAULT_MAX_QUOTE_SELECTORS, 20);
        assert_eq!(DEFAULT_MAX_AMOUNTS_PER_QUOTE, 20);
        assert_eq!(DEFAULT_MAX_STRUCTURED_DIFFERENCES, 100);
    }
}
