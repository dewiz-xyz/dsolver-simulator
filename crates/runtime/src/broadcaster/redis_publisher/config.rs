use std::time::Duration;

use crate::config::BroadcasterRedisConfig;

#[derive(Debug, Clone)]
pub struct BroadcasterRedisPublisherConfig {
    pub stream_key: String,
    pub snapshot_key: String,
    pub chain_id: u64,
    pub snapshot_max_payload_bytes: usize,
    pub append_retry_window: Duration,
}

impl BroadcasterRedisPublisherConfig {
    pub fn from_redis_config(
        redis_config: &BroadcasterRedisConfig,
        chain_id: u64,
        snapshot_max_payload_bytes: usize,
    ) -> Self {
        Self {
            stream_key: redis_config.stream_key.clone(),
            snapshot_key: redis_config.snapshot_key.clone(),
            chain_id,
            snapshot_max_payload_bytes,
            append_retry_window: Duration::from_millis(redis_config.append_retry_window_ms),
        }
    }
}
