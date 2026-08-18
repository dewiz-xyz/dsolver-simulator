#![expect(
    clippy::expect_used,
    reason = "benchmark fixtures must fail immediately when they are invalid"
)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use alloy_primitives::U256;
use criterion::{black_box, BenchmarkId, Criterion, Throughput};
use nix::sys::resource::{getrusage, UsageWho};
use serde::Deserialize;
use simulator_core::broadcaster::{
    BroadcasterBackend, BroadcasterSnapshotPartition, BroadcasterTokenDto, BroadcasterUpdateMessage,
};
use simulator_replay::{
    DecoderConfig, ExactPoolQuote, ReplayBackend, ReplayDecoder, ReplayWorld, StatePoint, TokenMap,
};
use tokio::runtime::Runtime;
use tycho_simulation::tycho_common::{models::token::Token, models::Chain, Bytes};

const NATIVE_CHECKPOINT: &str =
    include_str!("../../simulator-replay/tests/fixtures/native_checkpoint_v1.json");
const NATIVE_DELTA: &str =
    include_str!("../../simulator-replay/tests/fixtures/native_delta_v1.json");
const RFQ_CHECKPOINT: &str =
    include_str!("../../simulator-replay/tests/fixtures/rfq_checkpoint_v1.json");
const RFQ_DELTA: &str = include_str!("../../simulator-replay/tests/fixtures/rfq_delta_v1.json");

#[derive(Clone, Copy)]
struct QuoteShape {
    start_blocks: usize,
    lags: usize,
    amounts: usize,
}

impl QuoteShape {
    const fn quote_calls(self) -> usize {
        self.start_blocks * self.amounts * (self.lags + 1)
    }

    const fn state_points(self) -> usize {
        self.start_blocks * (self.lags + 1)
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct FixtureCheckpoint {
    chain_id: u64,
    tokens: Vec<BroadcasterTokenDto>,
    partition: BroadcasterSnapshotPartition,
    quote: FixtureQuote,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct FixtureDelta {
    applicable_backends: Vec<BroadcasterBackend>,
    update: BroadcasterUpdateMessage,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct FixtureQuote {
    backend: BroadcasterBackend,
    protocol: String,
    component_id: String,
    token_in: Bytes,
    token_out: Bytes,
    amount_in: String,
}

struct PreparedFixture {
    label: &'static str,
    decoder: ReplayDecoder,
    tokens: TokenMap,
    checkpoint: BroadcasterSnapshotPartition,
    delta: BroadcasterUpdateMessage,
    applicable_backends: Vec<ReplayBackend>,
    backend: ReplayBackend,
    protocol: String,
    component_id: String,
    token_in: Bytes,
    token_out: Bytes,
    amount_in: U256,
    checkpoint_bytes: usize,
    delta_bytes: usize,
}

impl PreparedFixture {
    async fn load(
        label: &'static str,
        checkpoint_json: &'static str,
        delta_json: &'static str,
    ) -> Self {
        let checkpoint: FixtureCheckpoint =
            serde_json::from_str(checkpoint_json).expect("checkpoint fixture must parse");
        let delta: FixtureDelta =
            serde_json::from_str(delta_json).expect("delta fixture must parse");
        assert_eq!(checkpoint.chain_id, Chain::Base.id());
        let tokens = fixture_tokens(checkpoint.tokens);
        let backend = ReplayBackend::from(checkpoint.quote.backend);
        let decoder = ReplayDecoder::new(
            DecoderConfig::for_backend(backend, vec![checkpoint.quote.protocol.clone()], 0),
            tokens.clone(),
        )
        .await
        .expect("replay decoder must initialize");
        Self {
            label,
            decoder,
            tokens,
            checkpoint: checkpoint.partition,
            delta: delta.update,
            applicable_backends: delta
                .applicable_backends
                .into_iter()
                .map(ReplayBackend::from)
                .collect(),
            backend,
            protocol: checkpoint.quote.protocol,
            component_id: checkpoint.quote.component_id,
            token_in: checkpoint.quote.token_in,
            token_out: checkpoint.quote.token_out,
            amount_in: U256::from_str_radix(&checkpoint.quote.amount_in, 10)
                .expect("fixture amount must be canonical"),
            checkpoint_bytes: checkpoint_json.len(),
            delta_bytes: delta_json.len(),
        }
    }

    async fn retained_points(&self, delta_rows: usize, point_count: usize) -> Vec<StatePoint> {
        assert!(point_count > 0);
        let decoded = self
            .decoder
            .decode_snapshot_partition(self.checkpoint.clone())
            .await
            .expect("checkpoint must decode");
        let mut world = ReplayWorld::new(self.tokens.clone(), None);
        let report = world.restore(decoded.update.expect("checkpoint must contain state"));
        assert!(!report.has_anomalies());

        let mut applied_rows = 0;
        let mut points = Vec::with_capacity(point_count);
        for point_index in 0..point_count {
            let target_rows = if point_count == 1 {
                delta_rows
            } else {
                point_index * delta_rows / (point_count - 1)
            };
            while applied_rows < target_rows {
                let decoded = self
                    .decoder
                    .decode_live_delta(self.delta.clone(), &self.applicable_backends)
                    .await
                    .expect("delta must decode");
                let report = world.apply(decoded.update.expect("delta must contain state"));
                assert!(!report.has_anomalies());
                applied_rows += 1;
            }
            points.push(world.pin());
        }
        points
    }

    fn run_quotes(&self, points: &[StatePoint], shape: QuoteShape) -> U256 {
        assert_eq!(points.len(), shape.state_points());
        let mut last = U256::ZERO;
        for (point_index, point) in points.iter().enumerate() {
            for amount_index in 0..shape.amounts {
                let input_offset = point_index * shape.amounts + amount_index;
                last = point
                    .quote(&ExactPoolQuote {
                        backend: self.backend,
                        protocol: self.protocol.clone(),
                        component_id: self.component_id.clone(),
                        token_in: self.token_in.clone(),
                        token_out: self.token_out.clone(),
                        amount_in: self.amount_in + U256::from(input_offset),
                    })
                    .expect("fixture quote must succeed");
            }
        }
        last
    }

    const fn fixture_input_bytes(&self, delta_rows: usize) -> usize {
        self.checkpoint_bytes + self.delta_bytes * delta_rows
    }
}

fn fixture_tokens(tokens: Vec<BroadcasterTokenDto>) -> HashMap<Bytes, Token> {
    tokens
        .into_iter()
        .map(|token| {
            let token = token
                .into_token(Chain::Base)
                .expect("fixture token must decode");
            (token.address.clone(), token)
        })
        .collect()
}

async fn replay_workload(
    fixture: &PreparedFixture,
    delta_rows: usize,
    shape: QuoteShape,
) -> WorkloadObservation {
    let points = fixture
        .retained_points(delta_rows, shape.state_points())
        .await;
    let amount_out = fixture.run_quotes(&points, shape);
    WorkloadObservation {
        amount_out,
        fixture_input_bytes: fixture.fixture_input_bytes(delta_rows),
        database_rows: delta_rows,
        s3_bytes: fixture.checkpoint_bytes,
        quote_calls: shape.quote_calls(),
        retained_points: points,
    }
}

struct WorkloadObservation {
    amount_out: U256,
    fixture_input_bytes: usize,
    database_rows: usize,
    s3_bytes: usize,
    quote_calls: usize,
    retained_points: Vec<StatePoint>,
}

impl WorkloadObservation {
    fn measurements(&self) -> (U256, usize, usize, usize, usize, usize) {
        (
            self.amount_out,
            self.fixture_input_bytes,
            self.database_rows,
            self.s3_bytes,
            self.quote_calls,
            self.retained_points.len(),
        )
    }
}

fn benchmark_replay(
    criterion: &mut Criterion,
    runtime: &Runtime,
    fixtures: &[Arc<PreparedFixture>],
) {
    let mut group = criterion.benchmark_group("replay_delta_span");
    for fixture in fixtures {
        for delta_rows in [10, 100, 1_800] {
            group.throughput(Throughput::Bytes(
                u64::try_from(fixture.fixture_input_bytes(delta_rows)).unwrap_or(u64::MAX),
            ));
            group.bench_with_input(
                BenchmarkId::new(fixture.label, delta_rows),
                &delta_rows,
                |bencher, rows| {
                    bencher.to_async(runtime).iter(|| async {
                        let observation = replay_workload(
                            fixture,
                            *rows,
                            QuoteShape {
                                start_blocks: 1,
                                lags: 1,
                                amounts: 1,
                            },
                        )
                        .await;
                        black_box(observation.measurements())
                    });
                },
            );
        }
    }
    group.finish();
}

fn benchmark_quote_matrix(
    criterion: &mut Criterion,
    runtime: &Runtime,
    fixtures: &[Arc<PreparedFixture>],
) {
    let mut group = criterion.benchmark_group("quote_matrix");
    for fixture in fixtures {
        for (label, shape) in quote_matrix_cases() {
            let points = runtime.block_on(fixture.retained_points(10, shape.state_points()));
            group.throughput(Throughput::Elements(
                u64::try_from(shape.quote_calls()).unwrap_or(u64::MAX),
            ));
            group.bench_with_input(
                BenchmarkId::new(fixture.label, label),
                &shape,
                |bencher, shape| {
                    bencher.iter(|| black_box(fixture.run_quotes(&points, *shape)));
                },
            );
        }
    }
    group.finish();
}

const fn quote_matrix_cases() -> [(&'static str, QuoteShape); 11] {
    [
        ("starts-1", quote_shape(1, 5, 5)),
        ("starts-10", quote_shape(10, 5, 5)),
        ("starts-100", quote_shape(100, 5, 5)),
        ("lags-1", quote_shape(10, 1, 5)),
        ("lags-3", quote_shape(10, 3, 5)),
        ("lags-5", quote_shape(10, 5, 5)),
        ("lags-10", quote_shape(10, 10, 5)),
        ("amounts-1", quote_shape(10, 5, 1)),
        ("amounts-5", quote_shape(10, 5, 5)),
        ("amounts-20", quote_shape(10, 5, 20)),
        ("max-100x10x20", quote_shape(100, 10, 20)),
    ]
}

const fn quote_shape(start_blocks: usize, lags: usize, amounts: usize) -> QuoteShape {
    QuoteShape {
        start_blocks,
        lags,
        amounts,
    }
}

fn benchmark_concurrent_jobs(
    criterion: &mut Criterion,
    runtime: &Runtime,
    fixtures: &[Arc<PreparedFixture>],
) {
    let mut group = criterion.benchmark_group("replay_concurrent_jobs");
    for fixture in fixtures {
        for jobs in [1, 2, 4] {
            group.bench_with_input(
                BenchmarkId::new(fixture.label, jobs),
                &jobs,
                |bencher, jobs| {
                    bencher.to_async(runtime).iter(|| async {
                        let mut tasks = Vec::with_capacity(*jobs);
                        for _ in 0..*jobs {
                            let fixture = Arc::clone(fixture);
                            tasks.push(tokio::spawn(async move {
                                replay_workload(fixture.as_ref(), 1_800, quote_shape(100, 10, 20))
                                    .await
                            }));
                        }
                        let mut observations = Vec::with_capacity(*jobs);
                        for task in tasks {
                            observations.push(task.await.expect("benchmark task must finish"));
                        }
                        black_box(
                            observations
                                .iter()
                                .map(WorkloadObservation::measurements)
                                .collect::<Vec<_>>(),
                        );
                        black_box(observations);
                    });
                },
            );
        }
    }
    group.finish();
}

fn peak_rss_bytes() -> Option<u64> {
    let usage = getrusage(UsageWho::RUSAGE_SELF).ok()?;
    let rss = u64::try_from(usage.max_rss()).ok()?;
    #[cfg(target_os = "macos")]
    {
        Some(rss)
    }
    #[cfg(not(target_os = "macos"))]
    {
        rss.checked_mul(1_024)
    }
}

fn main() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("benchmark runtime must build");
    let fixtures = runtime.block_on(async {
        vec![
            Arc::new(PreparedFixture::load("native", NATIVE_CHECKPOINT, NATIVE_DELTA).await),
            Arc::new(PreparedFixture::load("rfq", RFQ_CHECKPOINT, RFQ_DELTA).await),
        ]
    });
    let mut criterion = Criterion::default()
        .sample_size(10)
        .warm_up_time(Duration::from_millis(200))
        .measurement_time(Duration::from_secs(1))
        .without_plots();
    benchmark_replay(&mut criterion, &runtime, &fixtures);
    benchmark_quote_matrix(&mut criterion, &runtime, &fixtures);
    benchmark_concurrent_jobs(&mut criterion, &runtime, &fixtures);
    criterion.final_summary();
    let fixture_evidence = fixtures
        .iter()
        .map(|fixture| {
            serde_json::json!({
                "backend": fixture.label,
                "checkpointBytes": fixture.checkpoint_bytes,
                "deltaBytes": fixture.delta_bytes,
                "maxDeltaRows": 1800,
                "maxFixtureInputBytes": fixture.fixture_input_bytes(1800),
                "maxComparisonCount": 20000,
                "maxQuoteCallsIncludingStarts": QuoteShape {
                    start_blocks: 100,
                    lags: 10,
                    amounts: 20,
                }
                .quote_calls(),
            })
        })
        .collect::<Vec<_>>();
    eprintln!(
        "{}",
        serde_json::json!({
            "report": "historical_quote_replay_benchmark",
            "fixture": "sanitized",
            "deltaSpans": [10, 100, 1800],
            "startBlocks": [1, 10, 100],
            "lags": [1, 3, 5, 10],
            "amounts": [1, 5, 20],
            "concurrentJobs": [1, 2, 4],
            "retainedTimelineStatesPerJob": quote_shape(100, 10, 20).state_points(),
            "retainedTimelineDeltaRowsPerJob": 1800,
            "peakResidentBytes": peak_rss_bytes(),
            "fixtures": fixture_evidence,
        })
    );
}
