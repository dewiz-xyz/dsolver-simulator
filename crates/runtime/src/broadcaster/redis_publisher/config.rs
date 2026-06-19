use std::time::Duration;

use crate::config::BroadcasterRedisConfig;

#[derive(Debug, Clone)]
pub struct BroadcasterRedisPublisherConfig {
    pub stream_key: String,
    pub chain_id: u64,
    pub append_retry_window: Duration,
    pub maxlen: Option<u64>,
}

impl BroadcasterRedisPublisherConfig {
    pub fn from_redis_config(redis_config: &BroadcasterRedisConfig, chain_id: u64) -> Self {
        Self {
            stream_key: redis_config.stream_key.clone(),
            chain_id,
            append_retry_window: Duration::from_millis(redis_config.append_retry_window_ms),
            maxlen: redis_config.maxlen,
        }
    }
}
