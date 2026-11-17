use std::time::Duration;

use rand::Rng;
use tokio::time::Instant;

const BASE_DELAY: Duration = Duration::from_millis(200);
const MAX_DELAY: Duration = Duration::from_secs(2);

pub struct RetryState {
    attempt: u32,
}

impl RetryState {
    #[must_use]
    pub const fn new() -> Self {
        Self { attempt: 0 }
    }

    #[must_use]
    pub fn next_delay(&mut self, retry_after: Option<Duration>) -> Duration {
        self.attempt = self.attempt.saturating_add(1);
        if let Some(delay) = retry_after {
            return delay;
        }
        let factor = 1_u32
            .checked_shl(self.attempt.saturating_sub(1))
            .unwrap_or(u32::MAX);
        let base = BASE_DELAY.saturating_mul(factor).min(MAX_DELAY);
        let jitter_max = u64::try_from(base.as_millis() / 2).unwrap_or(u64::MAX);
        let jitter = rand::thread_rng().gen_range(0..=jitter_max);
        base.saturating_add(Duration::from_millis(jitter))
            .min(MAX_DELAY)
    }
}

pub async fn sleep_before(deadline: Instant, delay: Duration) -> bool {
    let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
        return false;
    };
    if delay > remaining {
        return false;
    }
    tokio::time::sleep(delay).await;
    Instant::now() < deadline
}
