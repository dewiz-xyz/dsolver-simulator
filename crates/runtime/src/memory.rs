use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use jemalloc_ctl::{arenas, epoch, raw, stats};
use tracing::{info, warn};

use crate::config::MemoryConfig;
use crate::metrics::emit_jemalloc_snapshot;

static LAST_SNAPSHOT_TS: AtomicU64 = AtomicU64::new(0);

pub fn maybe_purge_allocator(context: &'static str, cfg: MemoryConfig) {
    if !cfg.purge_enabled {
        return;
    }

    purge_jemalloc(context);
}

pub fn maybe_log_memory_snapshot(
    source: &'static str,
    label: &'static str,
    new_pairs: Option<usize>,
    cfg: MemoryConfig,
    bypass_throttle: bool,
) {
    if !cfg.snapshots_enabled {
        return;
    }

    if let Some(count) = new_pairs {
        if count < cfg.snapshots_min_new_pairs {
            return;
        }
    }

    let now_secs = match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_secs(),
        Err(_) => return,
    };

    if bypass_throttle {
        // Forced snapshots should always log, but still suppress nearby periodic/event snapshots
        // to keep the overall log volume predictable.
        LAST_SNAPSHOT_TS.store(now_secs, Ordering::Relaxed);
    } else if !should_log_snapshot(now_secs, cfg.snapshots_min_interval_secs) {
        return;
    }

    if let Err(err) = epoch::advance() {
        warn!(error = %err, "Failed advancing jemalloc epoch");
        return;
    }

    let allocated = match stats::allocated::read() {
        Ok(value) => value,
        Err(err) => {
            warn!(error = %err, "Failed reading jemalloc allocated bytes");
            return;
        }
    };
    let resident = match stats::resident::read() {
        Ok(value) => value,
        Err(err) => {
            warn!(error = %err, "Failed reading jemalloc resident bytes");
            return;
        }
    };

    info!(
        event = "stream_mem",
        source,
        label,
        new_pairs = new_pairs.unwrap_or(0),
        jemalloc_allocated_bytes = allocated,
        jemalloc_resident_bytes = resident,
        "Memory snapshot"
    );

    if cfg.snapshots_emit_emf {
        emit_jemalloc_snapshot(label, allocated, resident);
    }
}

fn purge_jemalloc(context: &'static str) {
    let arenas = match arenas::narenas::read() {
        Ok(count) => count as usize,
        Err(err) => {
            warn!(
                event = "memory_purge",
                context,
                method = "jemalloc",
                error = %err,
                "Failed reading jemalloc arena count"
            );
            return;
        }
    };

    let mut purged = 0usize;
    let mut failed = 0usize;
    for arena in 0..arenas {
        let name = format!("arena.{}.purge\0", arena);
        let result = unsafe { raw::write(name.as_bytes(), ()) };
        match result {
            Ok(()) => purged += 1,
            Err(err) => {
                failed += 1;
                warn!(
                    event = "memory_purge",
                    context,
                    method = "jemalloc",
                    arena,
                    error = %err,
                    "Failed to purge jemalloc arena"
                );
            }
        }
    }

    info!(
        event = "memory_purge",
        context,
        method = "jemalloc",
        arenas,
        purged,
        failed,
        "jemalloc purge complete"
    );
}

fn should_log_snapshot(now_secs: u64, min_interval_secs: u64) -> bool {
    loop {
        let last = LAST_SNAPSHOT_TS.load(Ordering::Relaxed);
        if now_secs.saturating_sub(last) < min_interval_secs {
            return false;
        }

        if LAST_SNAPSHOT_TS
            .compare_exchange(last, now_secs, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
        {
            return true;
        }
    }
}
