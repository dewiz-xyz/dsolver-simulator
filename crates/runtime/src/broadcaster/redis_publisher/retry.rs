use std::time::Duration;

use rand::Rng;
use tokio::time::Instant;

const RETRY_BACKOFF_BASE: Duration = Duration::from_millis(5);
const RETRY_BACKOFF_CAP: Duration = Duration::from_millis(200);

pub(super) fn remaining_retry_window(
    started_at: Instant,
    retry_window: Duration,
) -> Option<Duration> {
    let remaining = retry_window.saturating_sub(started_at.elapsed());
    (!remaining.is_zero()).then_some(remaining)
}

pub(super) async fn sleep_before_retry(started_at: Instant, retry_window: Duration, attempts: u64) {
    let elapsed = started_at.elapsed();
    let remaining = retry_window.saturating_sub(elapsed);
    if remaining.is_zero() {
        return;
    }
    let backoff = retry_backoff(attempts).min(remaining);
    let max_delay_ms = backoff.as_millis().max(1) as u64;
    let delay_ms = rand::thread_rng().gen_range(1..=max_delay_ms);
    tokio::time::sleep(Duration::from_millis(delay_ms)).await;
}

fn retry_backoff(attempts: u64) -> Duration {
    let multiplier = 1u32 << attempts.saturating_sub(1).min(5);
    RETRY_BACKOFF_BASE
        .saturating_mul(multiplier)
        .min(RETRY_BACKOFF_CAP)
}
