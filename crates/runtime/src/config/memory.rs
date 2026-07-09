use std::env;

#[derive(Debug, Clone, Copy)]
pub struct MemoryConfig {
    pub purge_enabled: bool,
    pub snapshots_enabled: bool,
    pub snapshots_min_interval_secs: u64,
    pub snapshots_min_new_pairs: usize,
    pub snapshots_emit_emf: bool,
}

impl MemoryConfig {
    pub fn from_env() -> Self {
        let snapshots_min_interval_secs = parse_env_u64("STREAM_MEM_LOG_MIN_INTERVAL_SECS", 60);
        assert!(
            snapshots_min_interval_secs > 0,
            "STREAM_MEM_LOG_MIN_INTERVAL_SECS must be > 0"
        );

        Self {
            purge_enabled: parse_env_bool_strict("STREAM_MEM_PURGE", true),
            snapshots_enabled: parse_env_bool_strict("STREAM_MEM_LOG", true),
            snapshots_min_interval_secs,
            snapshots_min_new_pairs: parse_env_usize("STREAM_MEM_LOG_MIN_NEW_PAIRS", 1000),
            snapshots_emit_emf: parse_env_bool_strict("STREAM_MEM_LOG_EMF", true),
        }
    }
}

fn parse_env_u64(key: &str, default: u64) -> u64 {
    match env::var(key) {
        Ok(value) => parse_or_panic(key, &value),
        Err(env::VarError::NotPresent) => default,
        Err(err) => read_env_or_panic(key, err),
    }
}

fn parse_env_usize(key: &str, default: usize) -> usize {
    match env::var(key) {
        Ok(value) => parse_or_panic(key, &value),
        Err(env::VarError::NotPresent) => default,
        Err(err) => read_env_or_panic(key, err),
    }
}

fn parse_env_bool_strict(key: &str, default: bool) -> bool {
    let value = match env::var(key) {
        Ok(value) => value,
        Err(env::VarError::NotPresent) => return default,
        Err(err) => read_env_or_panic(key, err),
    };

    match value.as_str() {
        "1" => return true,
        "0" => return false,
        _ => {}
    }

    match value.to_ascii_lowercase().as_str() {
        "true" | "yes" => true,
        "false" | "no" => false,
        _ => invalid_bool_value(key, &value),
    }
}

#[expect(
    clippy::panic,
    reason = "invalid startup env should still fail fast during boot"
)]
fn parse_or_panic<T>(key: &str, value: &str) -> T
where
    T: std::str::FromStr,
{
    match value.parse() {
        Ok(parsed) => parsed,
        Err(_) => panic!("Invalid {key}"),
    }
}

#[expect(
    clippy::panic,
    reason = "invalid startup env should still fail fast during boot"
)]
fn read_env_or_panic<T>(key: &str, err: env::VarError) -> T {
    panic!("Failed reading {key}: {err}");
}

#[expect(
    clippy::panic,
    reason = "invalid startup env should still fail fast during boot"
)]
fn invalid_bool_value(key: &str, value: &str) -> ! {
    panic!("Invalid {key}: expected one of 1/0/true/false/yes/no, got {value}");
}
