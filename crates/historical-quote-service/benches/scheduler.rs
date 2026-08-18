#![expect(
    clippy::expect_used,
    clippy::panic,
    reason = "benchmark requests and lifecycle invariants must fail immediately"
)]

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use criterion::{black_box, BenchmarkId, Criterion, Throughput};
use historical_quote::api::{HistoricalQuoteResult, JobResult, QuoteJobRequest};
use historical_quote_service::jobs::{
    ExecutionContext, JobExecutionError, JobExecutor, JobLimits, JobRegistry, JobRequest,
    JobRunner, PollOutcome, ScheduledJob, SubmitOutcome,
};
use tokio::runtime::Runtime;
use uuid::Uuid;

const MIB: u64 = 1024 * 1024;
const JOB_RESERVATION_BYTES: u64 = 1024 * MIB;
const GLOBAL_BUDGET_BYTES: u64 = 4 * 1024 * MIB;

fn quote_request(request_id: Uuid) -> QuoteJobRequest {
    serde_json::from_value(serde_json::json!({
        "requestId": request_id,
        "apiRevision": 1,
        "timeoutMs": 600000,
        "chainId": 8453,
        "pool": {
            "backend": "native",
            "protocol": "uniswap_v3",
            "componentId": "sanitized-pool",
            "tokenIn": "0x01",
            "tokenOut": "0x02"
        },
        "blocks": {"start": 100, "endInclusive": 100, "step": 1},
        "lags": [1],
        "amountsIn": ["1000000"]
    }))
    .expect("benchmark request must parse")
}

fn limits() -> JobLimits {
    JobLimits {
        max_running: 4,
        max_waiting: 16,
        decoded_byte_budget: GLOBAL_BUDGET_BYTES,
        max_terminal_jobs: 20,
        terminal_ttl: Duration::from_secs(3_600),
    }
}

async fn admit_jobs(count: usize) -> u64 {
    let registry = JobRegistry::new(limits());
    for _ in 0..count {
        let outcome = registry
            .submit(ScheduledJob::new(
                JobRequest::HistoricalQuote(quote_request(Uuid::new_v4())),
                JOB_RESERVATION_BYTES,
            ))
            .await;
        assert!(matches!(outcome, SubmitOutcome::Accepted(_)));
    }
    registry.snapshot().await.counts.queued
}

async fn admit_jobs_concurrently(count: usize) -> u64 {
    let registry = JobRegistry::new(limits());
    let submissions = (0..count).map(|_| {
        let registry = registry.clone();
        async move {
            registry
                .submit(ScheduledJob::new(
                    JobRequest::HistoricalQuote(quote_request(Uuid::new_v4())),
                    JOB_RESERVATION_BYTES,
                ))
                .await
        }
    });
    for outcome in futures::future::join_all(submissions).await {
        assert!(matches!(outcome, SubmitOutcome::Accepted(_)));
    }
    registry.snapshot().await.counts.queued
}

async fn reject_over_budget() -> SubmitOutcome {
    JobRegistry::new(limits())
        .submit(ScheduledJob::new(
            JobRequest::HistoricalQuote(quote_request(Uuid::new_v4())),
            GLOBAL_BUDGET_BYTES + 1,
        ))
        .await
}

#[derive(Clone, Copy)]
struct ImmediateExecutor;

impl JobExecutor for ImmediateExecutor {
    fn execute(
        &self,
        context: ExecutionContext,
    ) -> Pin<Box<dyn Future<Output = Result<JobResult, JobExecutionError>> + Send + 'static>> {
        Box::pin(async move {
            let JobRequest::HistoricalQuote(request) = context.request else {
                return Err(JobExecutionError::Cancelled);
            };
            let result: HistoricalQuoteResult = serde_json::from_value(serde_json::json!({
                "requestId": request.request_id,
                "chainId": 8453,
                "pool": request.pool,
                "visibleThroughBlock": 123,
                "gaps": [],
                "results": []
            }))
            .expect("benchmark result must parse");
            Ok(JobResult::HistoricalQuote(result))
        })
    }
}

async fn complete_job_lifecycles(count: usize) -> usize {
    let registry = JobRegistry::new(limits());
    let runner = tokio::spawn(JobRunner::new(registry.clone(), Arc::new(ImmediateExecutor)).run());
    let mut job_ids = Vec::with_capacity(count);
    for _ in 0..count {
        let outcome = registry
            .submit(ScheduledJob::new(
                JobRequest::HistoricalQuote(quote_request(Uuid::new_v4())),
                JOB_RESERVATION_BYTES,
            ))
            .await;
        let SubmitOutcome::Accepted(submission) = outcome else {
            panic!("benchmark job must be admitted");
        };
        job_ids.push(submission.job_id);
    }

    let mut delivered_bytes = 0;
    for job_id in job_ids {
        loop {
            match registry.poll(job_id).await {
                PollOutcome::Pending(_) => tokio::task::yield_now().await,
                PollOutcome::Terminal(delivery) => {
                    delivered_bytes += delivery.bytes().len();
                    delivery.commit().await;
                    break;
                }
                PollOutcome::NotFound => panic!("benchmark job disappeared before delivery"),
            }
        }
    }
    registry.begin_shutdown().await;
    runner.await.expect("benchmark runner must stop");
    delivered_bytes
}

fn benchmark_scheduler(criterion: &mut Criterion, runtime: &Runtime) {
    let mut group = criterion.benchmark_group("scheduler_admission");
    for count in [1, 2, 4, 16] {
        group.throughput(Throughput::Elements(
            u64::try_from(count).unwrap_or(u64::MAX),
        ));
        group.bench_with_input(
            BenchmarkId::new("sequential", count),
            &count,
            |bencher, count| {
                bencher
                    .to_async(runtime)
                    .iter(|| async { black_box(admit_jobs(*count).await) });
            },
        );
        group.bench_with_input(
            BenchmarkId::new("concurrent", count),
            &count,
            |bencher, count| {
                bencher
                    .to_async(runtime)
                    .iter(|| async { black_box(admit_jobs_concurrently(*count).await) });
            },
        );
    }
    group.finish();

    criterion.bench_function("scheduler_rejects_one_byte_over_budget", |bencher| {
        bencher.to_async(runtime).iter(|| async {
            let outcome = reject_over_budget().await;
            assert!(matches!(outcome, SubmitOutcome::ServiceUnavailable));
            black_box(outcome)
        });
    });

    let mut lifecycle = criterion.benchmark_group("scheduler_terminal_lifecycle");
    for count in [1, 4] {
        lifecycle.throughput(Throughput::Elements(
            u64::try_from(count).unwrap_or(u64::MAX),
        ));
        lifecycle.bench_with_input(
            BenchmarkId::new("completed_and_consumed", count),
            &count,
            |bencher, count| {
                bencher
                    .to_async(runtime)
                    .iter(|| async { black_box(complete_job_lifecycles(*count).await) });
            },
        );
    }
    lifecycle.finish();
}

fn main() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("benchmark runtime must build");
    let mut criterion = Criterion::default()
        .sample_size(10)
        .warm_up_time(Duration::from_millis(200))
        .measurement_time(Duration::from_secs(1))
        .without_plots();
    benchmark_scheduler(&mut criterion, &runtime);
    criterion.final_summary();
}
