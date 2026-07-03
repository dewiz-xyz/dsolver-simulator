use std::sync::Arc;
use std::time::Duration;

use tokio::sync::RwLock;
use tokio::time::Instant;

#[derive(Debug, Clone)]
pub struct StreamHealth {
    inner: Arc<RwLock<StreamHealthData>>,
}

#[derive(Debug, Default)]
struct StreamHealthData {
    started_at: Option<Instant>,
    last_update_at: Option<Instant>,
    last_block: u64,
    missing_block_burst: u64,
    missing_block_total: u64,
    missing_block_window_start: Option<Instant>,
    error_burst: u64,
    error_total: u64,
    error_window_start: Option<Instant>,
    advanced_burst: u64,
    advanced_total: u64,
    advanced_window_start: Option<Instant>,
    restart_count: u64,
    last_error: Option<String>,
}

#[derive(Debug, Clone, Copy)]
pub struct BurstUpdate {
    pub burst_count: u64,
    pub total_count: u64,
    pub window_started: bool,
}

#[derive(Debug, Clone, Copy)]
pub struct AdvancedUpdate {
    pub burst_count: u64,
    pub total_count: u64,
    pub window_started: bool,
    pub elapsed: Duration,
}

impl StreamHealth {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(StreamHealthData::default())),
        }
    }

    pub async fn mark_started(&self) {
        let mut guard = self.inner.write().await;
        let now = Instant::now();
        guard.started_at = Some(now);
        guard.last_update_at = None;
        guard.last_block = 0;
    }

    pub async fn record_update(&self, block: u64) {
        let mut guard = self.inner.write().await;
        guard.last_update_at = Some(Instant::now());
        guard.last_block = block;
    }

    pub async fn record_missing_block(&self, now: Instant, window: Duration) -> BurstUpdate {
        let mut guard = self.inner.write().await;
        guard.missing_block_total = guard.missing_block_total.saturating_add(1);
        let window_started = {
            if let Some(start) = guard.missing_block_window_start {
                if now.saturating_duration_since(start) > window {
                    guard.missing_block_burst = 0;
                    guard.missing_block_window_start = None;
                }
            }

            let started = guard.missing_block_window_start.is_none();
            if started {
                guard.missing_block_window_start = Some(now);
            }
            guard.missing_block_burst = guard.missing_block_burst.saturating_add(1);
            started
        };
        BurstUpdate {
            burst_count: guard.missing_block_burst,
            total_count: guard.missing_block_total,
            window_started,
        }
    }

    pub async fn record_error(&self, now: Instant, window: Duration) -> BurstUpdate {
        let mut guard = self.inner.write().await;
        guard.error_total = guard.error_total.saturating_add(1);
        let window_started = {
            if let Some(start) = guard.error_window_start {
                if now.saturating_duration_since(start) > window {
                    guard.error_burst = 0;
                    guard.error_window_start = None;
                }
            }

            let started = guard.error_window_start.is_none();
            if started {
                guard.error_window_start = Some(now);
            }
            guard.error_burst = guard.error_burst.saturating_add(1);
            started
        };
        BurstUpdate {
            burst_count: guard.error_burst,
            total_count: guard.error_total,
            window_started,
        }
    }

    pub async fn record_advanced(&self, now: Instant) -> AdvancedUpdate {
        let mut guard = self.inner.write().await;
        guard.advanced_total = guard.advanced_total.saturating_add(1);
        let window_started = guard.advanced_window_start.is_none();
        if window_started {
            guard.advanced_window_start = Some(now);
            guard.advanced_burst = 0;
        }
        guard.advanced_burst = guard.advanced_burst.saturating_add(1);
        let elapsed = guard
            .advanced_window_start
            .map(|start| now.saturating_duration_since(start))
            .unwrap_or(Duration::from_secs(0));
        AdvancedUpdate {
            burst_count: guard.advanced_burst,
            total_count: guard.advanced_total,
            window_started,
            elapsed,
        }
    }

    pub async fn clear_advanced(&self) {
        let mut guard = self.inner.write().await;
        guard.advanced_burst = 0;
        guard.advanced_window_start = None;
    }

    pub async fn increment_restart(&self) -> u64 {
        let mut guard = self.inner.write().await;
        guard.restart_count = guard.restart_count.saturating_add(1);
        guard.restart_count
    }

    pub async fn reset_bursts(&self) {
        let mut guard = self.inner.write().await;
        guard.missing_block_burst = 0;
        guard.missing_block_window_start = None;
        guard.error_burst = 0;
        guard.error_window_start = None;
        guard.advanced_burst = 0;
        guard.advanced_window_start = None;
    }

    pub async fn set_last_error(&self, message: Option<String>) {
        let mut guard = self.inner.write().await;
        guard.last_error = message;
    }

    pub async fn started_age_ms(&self) -> Option<u64> {
        let guard = self.inner.read().await;
        guard.started_at.map(|instant| {
            Instant::now()
                .saturating_duration_since(instant)
                .as_millis() as u64
        })
    }

    pub async fn has_received_update(&self) -> bool {
        let guard = self.inner.read().await;
        guard.last_update_at.is_some()
    }

    pub async fn last_update_age_ms(&self) -> Option<u64> {
        let guard = self.inner.read().await;
        guard.last_update_at.map(|instant| {
            Instant::now()
                .saturating_duration_since(instant)
                .as_millis() as u64
        })
    }

    pub async fn last_block(&self) -> u64 {
        let guard = self.inner.read().await;
        guard.last_block
    }

    pub async fn restart_count(&self) -> u64 {
        let guard = self.inner.read().await;
        guard.restart_count
    }

    pub async fn last_error(&self) -> Option<String> {
        let guard = self.inner.read().await;
        guard.last_error.clone()
    }
}

impl Default for StreamHealth {
    fn default() -> Self {
        Self::new()
    }
}
