use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use aws_sdk_secretsmanager::Client;
use tokio::sync::watch;
use tokio::time::{interval_at, timeout, Instant, MissedTickBehavior};
use tokio_util::sync::CancellationToken;

use super::{BearerKeySetError, BearerTokenVerifier};

const READ_TIMEOUT: Duration = Duration::from_secs(10);
const REFRESH_INTERVAL: Duration = Duration::from_secs(30);
const STALE_AFTER: Duration = Duration::from_secs(60);

/// One version and its document, returned by a single source read.
pub struct KeySetVersion {
    pub version_id: String,
    pub document: String,
}

/// Reads the current document without exposing provider errors or secret values.
pub trait KeySetSource: Send + Sync + 'static {
    fn read_current(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<KeySetVersion, BearerKeySetError>> + Send + '_>>;
}

/// Reads the version carrying the AWSCURRENT label from Secrets Manager.
pub struct SecretsManagerKeySetSource {
    client: Client,
    secret_arn: String,
}

impl SecretsManagerKeySetSource {
    #[must_use]
    pub fn new(client: Client, secret_arn: String) -> Self {
        Self { client, secret_arn }
    }
}

impl KeySetSource for SecretsManagerKeySetSource {
    fn read_current(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<KeySetVersion, BearerKeySetError>> + Send + '_>> {
        Box::pin(async move {
            let response = self
                .client
                .get_secret_value()
                .secret_id(&self.secret_arn)
                .version_stage("AWSCURRENT")
                .send()
                .await
                .map_err(|_| BearerKeySetError::SourceUnavailable)?;
            Ok(KeySetVersion {
                version_id: response
                    .version_id
                    .ok_or(BearerKeySetError::InvalidSourceResponse)?,
                document: response
                    .secret_string
                    .ok_or(BearerKeySetError::InvalidSourceResponse)?,
            })
        })
    }
}

/// A cheap handle for taking one coherent authentication snapshot.
#[derive(Clone)]
pub struct BearerKeySetHandle {
    receiver: watch::Receiver<Arc<BearerKeySetSnapshot>>,
}

impl BearerKeySetHandle {
    /// Clones the current snapshot without retaining the watch borrow.
    #[must_use]
    pub fn snapshot(&self) -> Arc<BearerKeySetSnapshot> {
        Arc::clone(&self.receiver.borrow())
    }
}

/// The validated keys and refresh state used by one authenticated request.
pub struct BearerKeySetSnapshot {
    verifier: Arc<BearerTokenVerifier>,
    version_id: String,
    last_success: Instant,
    last_refresh_failed: bool,
}

impl BearerKeySetSnapshot {
    /// Returns the audit ID associated with this token, if one matches.
    #[must_use]
    pub fn verify(&self, token: &str) -> Option<&str> {
        self.verifier.verify(token)
    }

    /// Returns non-secret metadata, including staleness at the time of this call.
    #[must_use]
    pub fn metadata(&self) -> BearerKeySetMetadata {
        let refresh_age = self.last_success.elapsed();
        BearerKeySetMetadata {
            version_id: self.version_id.clone(),
            refresh_age_seconds: refresh_age.as_secs(),
            degraded: self.last_refresh_failed || refresh_age >= STALE_AFTER,
        }
    }
}

/// Safe response metadata for an authenticated status request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BearerKeySetMetadata {
    pub version_id: String,
    pub refresh_age_seconds: u64,
    pub degraded: bool,
}

/// Owns the serial refresh loop and publishes complete snapshots atomically.
pub struct BearerKeySetReloader {
    source: Arc<dyn KeySetSource>,
    sender: watch::Sender<Arc<BearerKeySetSnapshot>>,
}

impl BearerKeySetReloader {
    /// Loads and validates keys before a handle can be made available to HTTP.
    ///
    /// # Errors
    ///
    /// Returns an error when the initial read times out, fails, or is invalid.
    pub async fn load(
        source: Arc<dyn KeySetSource>,
    ) -> Result<(BearerKeySetHandle, Self), BearerKeySetError> {
        let version = read_current(source.as_ref()).await?;
        let verifier = BearerTokenVerifier::from_json(&version.document)?;
        let snapshot = Arc::new(BearerKeySetSnapshot {
            verifier: Arc::new(verifier),
            version_id: version.version_id,
            last_success: Instant::now(),
            last_refresh_failed: false,
        });
        tracing::info!(
            auth_version = %snapshot.version_id,
            "historical quote API keys initialized"
        );
        let (sender, receiver) = watch::channel(snapshot);
        Ok((BearerKeySetHandle { receiver }, Self { source, sender }))
    }

    /// Reads once and either publishes valid keys or retains the last valid keys.
    ///
    /// # Errors
    ///
    /// Returns the sanitized read or validation error after publishing degraded
    /// refresh state. The active verifier is unchanged on failure.
    pub async fn refresh_once(&mut self) -> Result<(), BearerKeySetError> {
        let current = Arc::clone(&self.sender.borrow());
        let next = self.read_next(&current).await.inspect_err(|error| {
            if !current.last_refresh_failed {
                tracing::warn!(
                    auth_version = %current.version_id,
                    reason = %error,
                    "historical quote API key refresh degraded"
                );
            }
            self.sender.send_replace(Arc::new(BearerKeySetSnapshot {
                verifier: Arc::clone(&current.verifier),
                version_id: current.version_id.clone(),
                last_success: current.last_success,
                last_refresh_failed: true,
            }));
        })?;
        if current.version_id != next.version_id {
            tracing::info!(
                auth_version = %next.version_id,
                "historical quote API keys updated"
            );
        }
        if current.metadata().degraded {
            tracing::info!(
                auth_version = %next.version_id,
                "historical quote API key refresh recovered"
            );
        }
        self.sender.send_replace(Arc::new(next));
        Ok(())
    }

    /// Refreshes on a delayed interval until cancellation, including during reads.
    pub async fn run(mut self, cancellation: CancellationToken) {
        let mut ticks = interval_at(Instant::now() + REFRESH_INTERVAL, REFRESH_INTERVAL);
        ticks.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                biased;
                () = cancellation.cancelled() => return,
                _ = ticks.tick() => {
                    tokio::select! {
                        biased;
                        () = cancellation.cancelled() => return,
                        _ = self.refresh_once() => {}
                    }
                }
            }
        }
    }

    async fn read_next(
        &self,
        current: &BearerKeySetSnapshot,
    ) -> Result<BearerKeySetSnapshot, BearerKeySetError> {
        let version = read_current(self.source.as_ref()).await?;
        // Secret versions are immutable, so successful unchanged reads reuse the verifier.
        let verifier = if version.version_id == current.version_id {
            Arc::clone(&current.verifier)
        } else {
            Arc::new(BearerTokenVerifier::from_json(&version.document)?)
        };
        Ok(BearerKeySetSnapshot {
            verifier,
            version_id: version.version_id,
            last_success: Instant::now(),
            last_refresh_failed: false,
        })
    }
}

async fn read_current(source: &dyn KeySetSource) -> Result<KeySetVersion, BearerKeySetError> {
    let version = timeout(READ_TIMEOUT, source.read_current())
        .await
        .map_err(|_| BearerKeySetError::ReadTimeout)??;
    if !(32..=64).contains(&version.version_id.len())
        || !version
            .version_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
    {
        return Err(BearerKeySetError::InvalidSourceResponse);
    }
    Ok(version)
}

#[cfg(test)]
mod tests;
