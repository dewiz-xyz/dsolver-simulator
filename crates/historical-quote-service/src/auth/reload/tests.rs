use std::collections::VecDeque;
use std::error::Error;
use std::future::{pending, Future};
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::json;
use tokio::sync::Notify;
use tokio::time::{advance, Instant};
use tokio_util::sync::CancellationToken;

use super::{BearerKeySetReloader, KeySetSource, KeySetVersion};
use crate::auth::tests::document;
use crate::auth::BearerKeySetError;

enum ReadStep {
    Value(KeySetVersion),
    Failure(BearerKeySetError),
    Pending {
        started: Arc<Notify>,
        dropped: CancellationToken,
    },
}

struct SequenceSource {
    steps: Mutex<VecDeque<ReadStep>>,
    reads: AtomicUsize,
}

impl SequenceSource {
    fn new(steps: impl IntoIterator<Item = ReadStep>) -> Arc<Self> {
        Arc::new(Self {
            steps: Mutex::new(steps.into_iter().collect()),
            reads: AtomicUsize::new(0),
        })
    }

    fn reads(&self) -> usize {
        self.reads.load(Ordering::Relaxed)
    }
}

impl KeySetSource for SequenceSource {
    fn read_current(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<KeySetVersion, BearerKeySetError>> + Send + '_>> {
        Box::pin(async move {
            self.reads.fetch_add(1, Ordering::Relaxed);
            let step = self
                .steps
                .lock()
                .map_err(|_| BearerKeySetError::SourceUnavailable)?
                .pop_front()
                .ok_or(BearerKeySetError::SourceUnavailable)?;
            match step {
                ReadStep::Value(version) => Ok(version),
                ReadStep::Failure(error) => Err(error),
                ReadStep::Pending { started, dropped } => {
                    let _guard = CancelOnDrop(dropped);
                    started.notify_one();
                    pending().await
                }
            }
        })
    }
}

struct CancelOnDrop(CancellationToken);

impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

fn version(index: u32, entries: &[(&str, &str)]) -> KeySetVersion {
    KeySetVersion {
        version_id: format!("00000000-0000-4000-a000-{index:012}"),
        document: document(entries),
    }
}

fn ready(index: u32, entries: &[(&str, &str)]) -> ReadStep {
    ReadStep::Value(version(index, entries))
}

#[tokio::test]
async fn initial_load_requires_a_valid_document_and_source() {
    for step in [
        ReadStep::Failure(BearerKeySetError::SourceUnavailable),
        ReadStep::Value(KeySetVersion {
            document: "invalid".to_owned(),
            ..version(1, &[])
        }),
        ReadStep::Value(KeySetVersion {
            document: String::new(),
            ..version(1, &[])
        }),
    ] {
        assert!(BearerKeySetReloader::load(SequenceSource::new([step]))
            .await
            .is_err());
    }
}

#[tokio::test]
async fn rejects_missing_or_unsafe_version_ids() {
    for id in [
        String::new(),
        "a".repeat(31),
        "a".repeat(65),
        format!("{}\r\n", "a".repeat(32)),
        "é".repeat(32),
        "a_b".repeat(12),
    ] {
        let source = SequenceSource::new([ReadStep::Value(KeySetVersion {
            version_id: id,
            ..version(1, &[("existing", "existing-token")])
        })]);
        assert_eq!(
            BearerKeySetReloader::load(source).await.err(),
            Some(BearerKeySetError::InvalidSourceResponse)
        );
    }
}

#[tokio::test]
async fn accepts_the_version_id_length_boundaries() -> Result<(), BearerKeySetError> {
    for id in ["a".repeat(32), "A-".repeat(32)] {
        let source = SequenceSource::new([ReadStep::Value(KeySetVersion {
            version_id: id.clone(),
            ..version(1, &[("existing", "existing-token")])
        })]);
        let (handle, _) = BearerKeySetReloader::load(source).await?;
        assert_eq!(handle.snapshot().metadata().version_id, id);
    }
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn initial_load_times_out_and_drops_the_read() {
    let dropped = CancellationToken::new();
    let source = SequenceSource::new([ReadStep::Pending {
        started: Arc::new(Notify::new()),
        dropped: dropped.clone(),
    }]);
    let start = Instant::now();
    assert_eq!(
        BearerKeySetReloader::load(source).await.err(),
        Some(BearerKeySetError::ReadTimeout)
    );
    assert_eq!(start.elapsed(), Duration::from_secs(10));
    assert!(dropped.is_cancelled());
}

#[tokio::test]
async fn adds_rotates_and_revokes_with_atomic_snapshots() -> Result<(), BearerKeySetError> {
    let source = SequenceSource::new([
        ready(1, &[("existing", "existing-token")]),
        ready(
            2,
            &[
                ("existing", "existing-token"),
                ("client", "old-client-token"),
            ],
        ),
        ready(
            3,
            &[
                ("existing", "existing-token"),
                ("client", "new-client-token"),
            ],
        ),
        ready(4, &[("existing", "existing-token")]),
    ]);
    let (handle, mut reloader) = BearerKeySetReloader::load(source.clone()).await?;
    let initial = handle.snapshot();
    assert_eq!(initial.verify("old-client-token"), None);

    reloader.refresh_once().await?;
    let added = handle.snapshot();
    assert_eq!(added.verify("old-client-token"), Some("client"));
    assert_eq!(initial.verify("old-client-token"), None);

    reloader.refresh_once().await?;
    let rotated = handle.snapshot();
    assert_eq!(rotated.verify("old-client-token"), None);
    assert_eq!(rotated.verify("new-client-token"), Some("client"));
    assert_eq!(added.verify("old-client-token"), Some("client"));

    reloader.refresh_once().await?;
    let revoked = handle.snapshot();
    assert_eq!(revoked.verify("old-client-token"), None);
    assert_eq!(revoked.verify("new-client-token"), None);
    assert_eq!(revoked.verify("existing-token"), Some("existing"));
    assert_eq!(revoked.metadata().version_id, version(4, &[]).version_id);
    assert_eq!(source.reads(), 4);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn failed_reads_retain_keys_indefinitely_and_recover_on_the_same_version(
) -> Result<(), BearerKeySetError> {
    let source = SequenceSource::new([
        ready(1, &[("existing", "existing-token")]),
        ReadStep::Failure(BearerKeySetError::SourceUnavailable),
        ReadStep::Value(KeySetVersion {
            version_id: String::new(),
            ..version(2, &[("client", "client-token")])
        }),
        ready(1, &[("existing", "existing-token")]),
    ]);
    let (handle, mut reloader) = BearerKeySetReloader::load(source).await?;
    let initial = handle.snapshot();
    advance(Duration::from_secs(30)).await;
    assert_eq!(
        reloader.refresh_once().await,
        Err(BearerKeySetError::SourceUnavailable)
    );
    assert!(handle.snapshot().metadata().degraded);
    assert_eq!(handle.snapshot().metadata().refresh_age_seconds, 30);

    advance(Duration::from_secs(86_400)).await;
    assert_eq!(
        reloader.refresh_once().await,
        Err(BearerKeySetError::InvalidSourceResponse)
    );
    let retained = handle.snapshot();
    assert!(Arc::ptr_eq(&initial.verifier, &retained.verifier));
    assert_eq!(retained.verify("existing-token"), Some("existing"));
    assert_eq!(retained.metadata().refresh_age_seconds, 86_430);

    reloader.refresh_once().await?;
    let recovered = handle.snapshot();
    assert!(Arc::ptr_eq(&initial.verifier, &recovered.verifier));
    assert!(!recovered.metadata().degraded);
    assert_eq!(recovered.metadata().refresh_age_seconds, 0);
    Ok(())
}

#[tokio::test]
async fn invalid_new_documents_never_publish_partial_keys() -> Result<(), BearerKeySetError> {
    let invalid_document = json!({
        "schemaVersion": 1,
        "keys": [
            {"id": "client", "sha256": "0".repeat(64)},
            {"id": "bad!", "sha256": "1".repeat(64)},
        ],
    })
    .to_string();
    let source = SequenceSource::new([
        ready(1, &[("existing", "existing-token")]),
        ReadStep::Value(KeySetVersion {
            document: invalid_document,
            ..version(2, &[])
        }),
        ready(2, &[("client", "client-token")]),
    ]);
    let (handle, mut reloader) = BearerKeySetReloader::load(source).await?;
    let initial = handle.snapshot();
    assert_eq!(
        reloader.refresh_once().await,
        Err(BearerKeySetError::InvalidDocument)
    );
    let retained = handle.snapshot();
    assert!(Arc::ptr_eq(&initial.verifier, &retained.verifier));
    assert_eq!(
        retained.metadata().version_id,
        initial.metadata().version_id
    );
    assert!(retained.metadata().degraded);

    reloader.refresh_once().await?;
    assert_eq!(handle.snapshot().verify("existing-token"), None);
    assert_eq!(handle.snapshot().verify("client-token"), Some("client"));
    assert!(!handle.snapshot().metadata().degraded);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn staleness_is_visible_without_a_refresh_and_success_resets_it(
) -> Result<(), BearerKeySetError> {
    let source = SequenceSource::new([
        ready(1, &[("existing", "existing-token")]),
        ready(1, &[("existing", "existing-token")]),
    ]);
    let (handle, mut reloader) = BearerKeySetReloader::load(source).await?;
    let initial = handle.snapshot();
    advance(Duration::from_secs(59)).await;
    assert!(!handle.snapshot().metadata().degraded);
    advance(Duration::from_secs(1)).await;
    assert!(handle.snapshot().metadata().degraded);
    assert_eq!(handle.snapshot().metadata().refresh_age_seconds, 60);
    reloader.refresh_once().await?;
    let refreshed = handle.snapshot();
    assert!(Arc::ptr_eq(&initial.verifier, &refreshed.verifier));
    assert!(!refreshed.metadata().degraded);
    assert_eq!(refreshed.metadata().refresh_age_seconds, 0);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn refresh_timeout_keeps_the_last_verifier() -> Result<(), BearerKeySetError> {
    let dropped = CancellationToken::new();
    let source = SequenceSource::new([
        ready(1, &[("existing", "existing-token")]),
        ReadStep::Pending {
            started: Arc::new(Notify::new()),
            dropped: dropped.clone(),
        },
    ]);
    let (handle, mut reloader) = BearerKeySetReloader::load(source).await?;
    let initial = handle.snapshot();
    assert_eq!(
        reloader.refresh_once().await,
        Err(BearerKeySetError::ReadTimeout)
    );
    let retained = handle.snapshot();
    assert!(Arc::ptr_eq(&initial.verifier, &retained.verifier));
    assert_eq!(retained.verify("existing-token"), Some("existing"));
    assert_eq!(retained.metadata().refresh_age_seconds, 10);
    assert!(retained.metadata().degraded);
    assert!(dropped.is_cancelled());
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn loop_delays_the_first_read_and_skips_missed_ticks() -> Result<(), Box<dyn Error>> {
    let source = SequenceSource::new([
        ready(1, &[("existing", "existing-token")]),
        ready(2, &[("existing", "existing-token")]),
        ready(3, &[("existing", "existing-token")]),
    ]);
    let (handle, reloader) = BearerKeySetReloader::load(source.clone()).await?;
    let cancellation = CancellationToken::new();
    let task = tokio::spawn(reloader.run(cancellation.clone()));
    tokio::task::yield_now().await;
    advance(Duration::from_secs(29)).await;
    tokio::task::yield_now().await;
    assert_eq!(source.reads(), 1);
    advance(Duration::from_secs(1)).await;
    tokio::task::yield_now().await;
    assert_eq!(source.reads(), 2);
    assert_eq!(
        handle.snapshot().metadata().version_id,
        version(2, &[]).version_id
    );

    advance(Duration::from_secs(120)).await;
    tokio::task::yield_now().await;
    assert_eq!(source.reads(), 3);
    assert_eq!(
        handle.snapshot().metadata().version_id,
        version(3, &[]).version_id
    );
    tokio::task::yield_now().await;
    assert_eq!(source.reads(), 3);
    cancellation.cancel();
    task.await?;
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn cancellation_drops_an_in_flight_read_without_replacing_the_snapshot(
) -> Result<(), Box<dyn Error>> {
    let started = Arc::new(Notify::new());
    let dropped = CancellationToken::new();
    let source = SequenceSource::new([
        ready(1, &[("existing", "existing-token")]),
        ReadStep::Pending {
            started: Arc::clone(&started),
            dropped: dropped.clone(),
        },
    ]);
    let (handle, reloader) = BearerKeySetReloader::load(source.clone()).await?;
    let initial = handle.snapshot();
    let cancellation = CancellationToken::new();
    let task = tokio::spawn(reloader.run(cancellation.clone()));
    started.notified().await;
    cancellation.cancel();
    task.await?;
    assert!(dropped.is_cancelled());
    assert_eq!(source.reads(), 2);
    assert!(Arc::ptr_eq(&initial, &handle.snapshot()));
    assert_eq!(handle.snapshot().verify("existing-token"), Some("existing"));
    Ok(())
}
