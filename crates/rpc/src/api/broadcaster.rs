use axum::{
    routing::{get, post},
    Router,
};

use runtime::broadcaster::app::BroadcasterAppState;

use crate::handlers::broadcaster::{
    create_snapshot_session, deployment_ready, ready, snapshot_session_payload, status,
    token_lookup, token_snapshot,
};

pub fn create_broadcaster_router(app_state: BroadcasterAppState) -> Router {
    Router::new()
        .route("/status", get(status))
        .route("/ready", get(ready))
        .route("/deployment-ready", get(deployment_ready))
        .route("/snapshot-sessions", post(create_snapshot_session))
        .route(
            "/snapshot-sessions/:session_id/payloads/:index",
            get(snapshot_session_payload),
        )
        .route("/tokens/snapshot", get(token_snapshot))
        .route("/tokens/lookup", post(token_lookup))
        .with_state(app_state)
}

#[cfg(test)]
mod tests {
    use std::any::Any;
    use std::collections::HashMap;
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };
    use std::time::Duration;

    use anyhow::{anyhow, bail, Result};
    use axum::{
        body::{to_bytes, Body},
        http::{Request, StatusCode},
        routing::post,
        Json, Router,
    };
    use num_bigint::BigUint;
    use num_traits::Zero;
    use runtime::{
        broadcaster::app::BroadcasterAppState,
        broadcaster::redis_publisher::{
            BroadcasterRedisPublisher, BroadcasterRedisPublisherConfig, RedisStreamWriter,
        },
        broadcaster::service::BroadcasterServiceState,
        broadcaster::state::{
            BroadcasterReadiness, BroadcasterSnapshotCache, BroadcasterUpstreamState,
        },
        models::tokens::TokenStore,
    };
    use simulator_core::broadcaster::{BroadcasterBackend, BroadcasterBackendHead};
    use tokio::{
        sync::{Barrier, Mutex, Notify},
        task::JoinHandle,
        time::{sleep, timeout},
    };
    use tower::ServiceExt;
    use tycho_simulation::{
        protocol::models::{ProtocolComponent, Update},
        tycho_client::feed::{
            synchronizer::{Snapshot, StateSyncMessage},
            BlockHeader, FeedMessage, SynchronizerState,
        },
        tycho_common::{
            dto::{PaginationResponse, ProtocolStateDelta, TokensRequestResponse},
            models::{blockchain::BlockAggregatedChanges, token::Token, Chain},
            simulation::{
                errors::{SimulationError, TransitionError},
                protocol_sim::{Balances, GetAmountOutResult, ProtocolSim},
            },
            Bytes,
        },
    };

    use super::create_broadcaster_router;

    #[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
    struct DummySim(u8);

    #[typetag::serde]
    impl ProtocolSim for DummySim {
        fn fee(&self) -> f64 {
            0.0
        }

        fn spot_price(&self, _base: &Token, _quote: &Token) -> Result<f64, SimulationError> {
            Ok(0.0)
        }

        fn get_amount_out(
            &self,
            amount_in: BigUint,
            _token_in: &Token,
            _token_out: &Token,
        ) -> Result<GetAmountOutResult, SimulationError> {
            Ok(GetAmountOutResult::new(
                amount_in,
                BigUint::zero(),
                self.clone_box(),
            ))
        }

        fn get_limits(
            &self,
            _sell_token: Bytes,
            _buy_token: Bytes,
        ) -> Result<(BigUint, BigUint), SimulationError> {
            Ok((BigUint::zero(), BigUint::zero()))
        }

        fn delta_transition(
            &mut self,
            _delta: ProtocolStateDelta,
            _tokens: &HashMap<Bytes, Token>,
            _balances: &Balances,
        ) -> Result<(), TransitionError> {
            Ok(())
        }

        fn clone_box(&self) -> Box<dyn ProtocolSim> {
            Box::new(self.clone())
        }

        fn as_any(&self) -> &dyn Any {
            self
        }

        fn as_any_mut(&mut self) -> &mut dyn Any {
            self
        }

        fn eq(&self, other: &dyn ProtocolSim) -> bool {
            other
                .as_any()
                .downcast_ref::<DummySim>()
                .map(|value| value.0 == self.0)
                .unwrap_or(false)
        }
    }

    #[derive(Clone, Copy)]
    enum SeedMode {
        Disconnected,
        WarmingUp,
        Ready,
    }

    #[tokio::test]
    async fn status_reports_upstream_disconnected() -> Result<()> {
        let app = create_broadcaster_router(build_state(SeedMode::Disconnected).await?);
        let (status, body) = get_json(app, "/status").await?;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["status"], "upstream_disconnected");
        assert_eq!(body["upstream"]["connected"], false);
        assert_eq!(body["snapshot"]["ready"], false);
        assert_eq!(
            body["backends"]["native"]["block_number"],
            serde_json::Value::Null
        );
        Ok(())
    }

    #[tokio::test]
    async fn status_reports_snapshot_warming_up() -> Result<()> {
        let app = create_broadcaster_router(build_state(SeedMode::WarmingUp).await?);
        let (status, body) = get_json(app.clone(), "/status").await?;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["status"], "snapshot_warming_up");
        assert_eq!(body["upstream"]["connected"], true);
        assert_eq!(body["snapshot"]["ready"], false);

        let (status, body) = get_json(app.clone(), "/ready").await?;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body["status"], "snapshot_warming_up");

        let (status, body) = post_json(app, "/snapshot-sessions", serde_json::json!({})).await?;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body["status"], "snapshot_warming_up");
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn status_reports_ready_once_snapshot_is_bootstrapped() -> Result<()> {
        let app = create_broadcaster_router(build_state(SeedMode::Ready).await?);
        let (status, body) = get_json(app.clone(), "/status").await?;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["status"], "ready");
        assert_eq!(body["redis_publisher"]["mode"], "active");
        assert_eq!(body["recovery"]["active"], false);
        assert_eq!(body["recovery"]["phase"], "idle");
        assert_eq!(body["recovery"]["buffer_a_count"], 0);
        assert_eq!(body["recovery"]["buffer_b_count"], 0);
        assert_eq!(body["snapshot"]["ready"], true);
        assert_eq!(body["backends"]["native"]["block_number"], 10);
        assert_eq!(body["backends"]["native"]["pool_count"], 1);

        let (status, body) = get_json(app, "/ready").await?;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["status"], "ready");
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn deployment_ready_uses_explicit_admission_instead_of_readiness() -> Result<()> {
        let active = create_broadcaster_router(build_state(SeedMode::Ready).await?);
        let (status, body) = get_json(active, "/deployment-ready").await?;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["deployment_admission"]["admitted"], true);
        assert_eq!(body["deployment_admission"]["phase"], "admitted");

        let passive = create_broadcaster_router(build_passive_ready_state().await?);
        let (status, body) = get_json(passive, "/deployment-ready").await?;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body["deployment_admission"]["admitted"], false);
        assert_eq!(body["deployment_admission"]["phase"], "warming");
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn status_reports_passive_publisher_while_remaining_live() -> Result<()> {
        let app = create_broadcaster_router(build_passive_ready_state().await?);
        let (status, body) = get_json(app.clone(), "/status").await?;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["status"], "redis_publisher_passive");
        assert_eq!(body["snapshot"]["ready"], true);
        assert_eq!(body["redis_publisher"]["healthy"], true);
        assert_eq!(body["redis_publisher"]["mode"], "passive");
        assert!(body["redis_publisher"]["replay_boundary"].is_null());

        let (status, body) = post_json(app, "/snapshot-sessions", serde_json::json!({})).await?;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body["status"], "redis_publisher_passive");
        assert_eq!(body["redis_publisher"]["mode"], "passive");
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn status_includes_redis_publisher_when_attached() -> Result<()> {
        let app =
            create_broadcaster_router(build_state_with_redis(RpcFakeRedisWriter::healthy()).await?);

        let (status, body) = get_json(app, "/status").await?;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["status"], "ready");
        assert_eq!(body["redis_publisher"]["healthy"], true);
        assert_eq!(body["redis_publisher"]["mode"], "active");
        assert!(body["state_history"].is_null());
        let Some(stream_id) = body["redis_publisher"]["stream_id"].as_str() else {
            bail!("expected redis publisher stream_id");
        };
        assert!(stream_id.starts_with("chain-1-stream-"));
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn ready_fences_stale_active_publisher_before_reporting_ready() -> Result<()> {
        let writer = RpcFakeRedisWriter::healthy();
        let app = create_broadcaster_router(build_stale_active_state(writer).await?);

        let (status, body) = get_json(app, "/ready").await?;

        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body["status"], "redis_publisher_retired");
        assert_eq!(body["redis_publisher"]["healthy"], false);
        assert_eq!(body["redis_publisher"]["mode"], "retired");
        assert!(body["redis_publisher"]["replay_boundary"].is_null());
        assert_eq!(body["deployment_admission"]["admitted"], false);
        assert_eq!(body["deployment_admission"]["phase"], "retired");
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn deployment_ready_responds_while_publisher_mutex_is_held() -> Result<()> {
        let writer = RpcFakeRedisWriter::healthy();
        let (state, service) = build_state_with_redis_and_service(writer.clone()).await?;
        assert_eq!(state.status_snapshot().await.readiness.as_str(), "ready");
        writer.block_next_append().await;
        let heartbeat = tokio::spawn(async move { service.broadcast_heartbeat().await });
        writer.wait_for_blocked_append().await;

        let (status, body) = timeout(
            Duration::from_millis(100),
            get_json(create_broadcaster_router(state), "/deployment-ready"),
        )
        .await
        .map_err(|_| anyhow!("deployment readiness blocked on the publisher mutex"))??;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["deployment_admission"]["admitted"], true);
        writer.release_blocked_append();
        heartbeat.await??;
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn retained_boundary_keeps_readiness_and_session_admission_in_agreement() -> Result<()> {
        let writer = RpcFakeRedisWriter::healthy();
        let state = build_state_with_redis(writer.clone()).await?;

        let first = state.readiness_snapshot().await;
        let second = state.readiness_snapshot().await;
        let session = state.create_snapshot_session().await?;

        assert_eq!(first.readiness, BroadcasterReadiness::Ready);
        assert!(first.snapshot.exportable);
        assert_eq!(second.readiness, BroadcasterReadiness::Ready);
        assert!(session.is_some());
        assert_eq!(
            writer.inspect_count().await,
            1,
            "the retained-boundary verdict should be reused within the cache window"
        );
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn trimmed_boundary_closes_readiness_and_session_admission() -> Result<()> {
        let writer = RpcFakeRedisWriter::healthy();
        let state = build_state_with_redis(writer.clone()).await?;
        writer.set_first_entry_sequence(3).await;

        let readiness = state.readiness_snapshot().await;
        let session = state.create_snapshot_session().await?;

        assert_eq!(
            readiness.readiness,
            BroadcasterReadiness::SnapshotUnexportable
        );
        assert!(!readiness.snapshot.exportable);
        assert!(session.is_none());
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn boundary_inspection_error_closes_readiness_and_session_admission() -> Result<()> {
        let writer = RpcFakeRedisWriter::healthy();
        let state = build_state_with_redis(writer.clone()).await?;
        writer.fail_next_inspect().await;

        let readiness = state.readiness_snapshot().await;
        let session = state.create_snapshot_session().await?;

        assert_eq!(
            readiness.readiness,
            BroadcasterReadiness::SnapshotUnexportable
        );
        assert!(!readiness.snapshot.exportable);
        assert!(session.is_none());
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn status_reports_unhealthy_publisher_while_remaining_live() -> Result<()> {
        let app = create_broadcaster_router(build_state_with_unhealthy_redis().await?);

        let (status, body) = get_json(app.clone(), "/status").await?;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["status"], "redis_publisher_unhealthy");
        assert_eq!(body["redis_publisher"]["healthy"], false);
        assert_eq!(body["redis_publisher"]["mode"], "unhealthy");
        assert_eq!(body["deployment_admission"]["admitted"], true);
        assert_eq!(body["deployment_admission"]["phase"], "admitted");

        let (status, body) = post_json(app, "/snapshot-sessions", serde_json::json!({})).await?;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body["status"], "redis_publisher_unhealthy");
        assert_eq!(body["redis_publisher"]["healthy"], false);
        Ok(())
    }

    #[tokio::test]
    async fn status_preserves_snapshot_warming_with_healthy_redis_publisher() -> Result<()> {
        let app = create_broadcaster_router(build_warming_state_with_redis().await?);

        let (status, body) = get_json(app, "/status").await?;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["status"], "snapshot_warming_up");
        assert_eq!(body["redis_publisher"]["healthy"], true);
        assert_eq!(body["redis_publisher"]["mode"], "passive");
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn snapshot_session_create_serves_payload_metadata_and_payloads() -> Result<()> {
        let app = create_broadcaster_router(build_state(SeedMode::Ready).await?);
        let (status, body) =
            post_json(app.clone(), "/snapshot-sessions", serde_json::json!({})).await?;

        assert_eq!(status, StatusCode::CREATED);
        assert_eq!(body["chainId"], Chain::Ethereum.id());
        assert_eq!(body["streamId"], "chain-1-stream-1");
        assert_eq!(body["snapshotId"], "chain-1-snapshot-1");
        assert!(body["redisReplayBoundary"].is_object());
        assert_eq!(body["payloadCount"], 3);
        assert_eq!(body["snapshotChunkCount"], 1);
        assert_eq!(body["expiresInMs"], 300_000);

        let session_id = body["sessionId"]
            .as_u64()
            .ok_or_else(|| anyhow!("expected numeric sessionId"))?;
        let (status, payload) =
            get_json(app, &format!("/snapshot-sessions/{session_id}/payloads/0")).await?;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(payload["stream_id"], "chain-1-stream-1");
        assert_eq!(payload["message_seq"], 1);
        assert_eq!(payload["kind"], "snapshot_start");
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn snapshot_session_stays_available_while_rfq_is_warming() -> Result<()> {
        let app = create_broadcaster_router(
            build_state_with_rfq(SeedMode::Ready, SeedMode::WarmingUp).await?,
        );
        let (status, body) = get_json(app.clone(), "/status").await?;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["status"], "ready");
        assert_eq!(
            body["snapshot"]["configured_backends"],
            serde_json::json!(["native", "rfq"])
        );
        assert!(body["backends"]["rfq"].is_object());
        assert!(body["upstream"]["connected"].as_bool().unwrap_or(false));
        assert!(body["snapshot"]["ready"].as_bool().unwrap_or(false));

        let (status, body) =
            post_json(app.clone(), "/snapshot-sessions", serde_json::json!({})).await?;
        assert_eq!(status, StatusCode::CREATED);
        let session_id = body["sessionId"]
            .as_u64()
            .ok_or_else(|| anyhow!("expected numeric sessionId"))?;
        let (status, payload) =
            get_json(app, &format!("/snapshot-sessions/{session_id}/payloads/0")).await?;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(payload["backends"], serde_json::json!(["native"]));
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn root_status_reports_rfq_update_timestamp_without_block_number() -> Result<()> {
        let app = create_broadcaster_router(
            build_state_with_rfq(SeedMode::Ready, SeedMode::Ready).await?,
        );

        let (status, body) = get_json(app, "/status").await?;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["status"], "ready");
        assert_eq!(body["backends"]["rfq"]["update_timestamp"], 12);
        assert!(body["backends"]["rfq"].get("block_number").is_none());
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn snapshot_session_create_serves_all_configured_backends() -> Result<()> {
        let app = create_broadcaster_router(
            build_state_with_rfq(SeedMode::Ready, SeedMode::Ready).await?,
        );

        let (status, body) =
            post_json(app.clone(), "/snapshot-sessions", serde_json::json!({})).await?;

        assert_eq!(status, StatusCode::CREATED);
        assert_eq!(body["chainId"], Chain::Ethereum.id());
        assert_eq!(body["streamId"], "chain-1-stream-1");
        assert_eq!(body["snapshotId"], "chain-1-snapshot-1");
        assert_eq!(body["payloadCount"], 4);
        assert_eq!(body["snapshotChunkCount"], 2);
        let session_id = body["sessionId"]
            .as_u64()
            .ok_or_else(|| anyhow!("expected numeric sessionId"))?;

        let (status, start) = get_json(
            app.clone(),
            &format!("/snapshot-sessions/{session_id}/payloads/0"),
        )
        .await?;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(start["kind"], "snapshot_start");
        assert_eq!(start["backends"], serde_json::json!(["native", "rfq"]));
        assert_eq!(start["totalChunks"], 2);

        let (_status, native_chunk) = get_json(
            app.clone(),
            &format!("/snapshot-sessions/{session_id}/payloads/1"),
        )
        .await?;
        let (_status, rfq_chunk) = get_json(
            app.clone(),
            &format!("/snapshot-sessions/{session_id}/payloads/2"),
        )
        .await?;
        assert_eq!(native_chunk["kind"], "snapshot_chunk");
        assert_eq!(native_chunk["partitions"][0]["backend"], "native");
        assert_eq!(rfq_chunk["kind"], "snapshot_chunk");
        assert_eq!(rfq_chunk["partitions"][0]["backend"], "rfq");

        let (status, body) = get_json(app, "/status").await?;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["snapshot_sessions"]["active"], 1);
        Ok(())
    }

    #[tokio::test]
    async fn snapshot_session_payload_reports_out_of_range() -> Result<()> {
        let app = create_broadcaster_router(build_state(SeedMode::Ready).await?);
        let (_status, body) =
            post_json(app.clone(), "/snapshot-sessions", serde_json::json!({})).await?;
        let session_id = body["sessionId"]
            .as_u64()
            .ok_or_else(|| anyhow!("expected numeric sessionId"))?;

        let (status, body) =
            get_json(app, &format!("/snapshot-sessions/{session_id}/payloads/99")).await?;

        assert_eq!(status, StatusCode::RANGE_NOT_SATISFIABLE);
        assert_eq!(body["error"], "snapshot payload index out of range");
        Ok(())
    }

    #[tokio::test]
    async fn token_lookup_serves_cache_hits_without_upstream_fetch() -> Result<()> {
        let cached = token(0x41, "CACHED", Chain::Ethereum);
        let (tycho_url, request_count, server_task) =
            spawn_tycho_token_server(None, Duration::ZERO).await?;
        let app = create_broadcaster_router(
            build_state_with_tokens(
                SeedMode::Disconnected,
                token_store(vec![cached.clone()], tycho_url, Chain::Ethereum),
                Chain::Ethereum,
            )
            .await?,
        );

        let (status, body) = post_json(
            app,
            "/tokens/lookup",
            serde_json::json!({
                "chainId": Chain::Ethereum.id(),
                "addresses": [cached.address],
            }),
        )
        .await?;
        server_task.abort();

        assert_eq!(status, StatusCode::OK);
        assert_eq!(request_count.load(Ordering::SeqCst), 0);
        assert_eq!(body["tokens"][0]["symbol"], "CACHED");
        assert_eq!(body["missing"], serde_json::json!([]));
        Ok(())
    }

    #[tokio::test]
    async fn token_snapshot_serves_broadcaster_token_cache() -> Result<()> {
        let cached = token(0x40, "SNAP", Chain::Ethereum);
        let (tycho_url, request_count, server_task) =
            spawn_tycho_token_server(None, Duration::ZERO).await?;
        let app = create_broadcaster_router(
            build_state_with_tokens(
                SeedMode::Disconnected,
                token_store(vec![cached.clone()], tycho_url, Chain::Ethereum),
                Chain::Ethereum,
            )
            .await?,
        );

        let (status, body) = get_json(app, "/tokens/snapshot").await?;
        server_task.abort();

        assert_eq!(status, StatusCode::OK);
        assert_eq!(request_count.load(Ordering::SeqCst), 0);
        assert_eq!(body["chainId"], Chain::Ethereum.id());
        assert_eq!(body["tokens"][0]["symbol"], "SNAP");
        Ok(())
    }

    #[tokio::test]
    async fn token_snapshot_includes_admitted_stream_token() -> Result<()> {
        let stream_token = token_with_metadata(0x51, "STREAM", 18, 75);
        let app = create_broadcaster_router(
            build_state_with_stream_token(Vec::new(), stream_token, 75).await?,
        );

        let (status, body) = get_json(app, "/tokens/snapshot").await?;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["tokens"].as_array().map(Vec::len), Some(1));
        assert_eq!(body["tokens"][0]["symbol"], "STREAM");
        assert_eq!(body["tokens"][0]["quality"], 75);
        Ok(())
    }

    #[tokio::test]
    async fn token_snapshot_includes_token_received_during_pending_replacement() -> Result<()> {
        let stream_token = token_with_metadata(0x54, "RECOVERY", 18, 75);
        let app = create_broadcaster_router(
            build_state_with_recovery_stream_token(stream_token, 75).await?,
        );

        let (status, body) = get_json(app, "/tokens/snapshot").await?;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["tokens"].as_array().map(Vec::len), Some(1));
        assert_eq!(body["tokens"][0]["symbol"], "RECOVERY");
        assert_eq!(body["tokens"][0]["quality"], 75);
        Ok(())
    }

    #[tokio::test]
    async fn token_snapshot_keeps_known_metadata_for_stream_duplicate() -> Result<()> {
        let stored = token_with_metadata(0x52, "STORED", 6, 80);
        let incoming = token_with_metadata(0x52, "INCOMING", 18, 100);
        let app = create_broadcaster_router(
            build_state_with_stream_token(vec![stored], incoming, 50).await?,
        );

        let (status, body) = get_json(app, "/tokens/snapshot").await?;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["tokens"].as_array().map(Vec::len), Some(1));
        assert_eq!(body["tokens"][0]["symbol"], "STORED");
        assert_eq!(body["tokens"][0]["decimals"], 6);
        assert_eq!(body["tokens"][0]["quality"], 80);
        Ok(())
    }

    #[tokio::test]
    async fn token_snapshot_excludes_stream_token_below_quality_floor() -> Result<()> {
        let stream_token = token_with_metadata(0x53, "LOW", 18, 49);
        let app = create_broadcaster_router(
            build_state_with_stream_token(Vec::new(), stream_token, 50).await?,
        );

        let (status, body) = get_json(app, "/tokens/snapshot").await?;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["tokens"], serde_json::json!([]));
        Ok(())
    }

    #[tokio::test]
    async fn token_lookup_fetches_cache_misses_from_broadcaster_store() -> Result<()> {
        let fetched = token(0x42, "FETCHED", Chain::Ethereum);
        let (tycho_url, request_count, server_task) =
            spawn_tycho_token_server(Some(fetched.clone()), Duration::ZERO).await?;
        let app = create_broadcaster_router(
            build_state_with_tokens(
                SeedMode::Disconnected,
                token_store(vec![], tycho_url, Chain::Ethereum),
                Chain::Ethereum,
            )
            .await?,
        );

        let (status, body) = post_json(
            app,
            "/tokens/lookup",
            serde_json::json!({
                "chainId": Chain::Ethereum.id(),
                "addresses": [fetched.address],
            }),
        )
        .await?;
        server_task.abort();

        assert_eq!(status, StatusCode::OK);
        assert_eq!(request_count.load(Ordering::SeqCst), 1);
        assert_eq!(body["tokens"][0]["symbol"], "FETCHED");
        assert_eq!(body["missing"], serde_json::json!([]));
        Ok(())
    }

    #[tokio::test]
    async fn token_lookup_reports_unresolved_tycho_tokens_as_missing() -> Result<()> {
        let missing = address(0x43);
        let (tycho_url, _request_count, server_task) =
            spawn_tycho_token_server(None, Duration::ZERO).await?;
        let app = create_broadcaster_router(
            build_state_with_tokens(
                SeedMode::Disconnected,
                token_store(vec![], tycho_url, Chain::Ethereum),
                Chain::Ethereum,
            )
            .await?,
        );

        let (status, body) = post_json(
            app,
            "/tokens/lookup",
            serde_json::json!({
                "chainId": Chain::Ethereum.id(),
                "addresses": [missing],
            }),
        )
        .await?;
        server_task.abort();

        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["tokens"], serde_json::json!([]));
        assert_eq!(body["missing"], serde_json::json!([missing]));
        Ok(())
    }

    #[tokio::test]
    async fn token_lookup_coalesces_concurrent_same_token_misses() -> Result<()> {
        const CONCURRENT_CALLS: usize = 6;

        let fetched = token(0x44, "SHARED", Chain::Ethereum);
        let (tycho_url, request_count, server_task) =
            spawn_tycho_token_server(Some(fetched.clone()), Duration::from_millis(100)).await?;
        let app = create_broadcaster_router(
            build_state_with_tokens(
                SeedMode::Disconnected,
                token_store(vec![], tycho_url, Chain::Ethereum),
                Chain::Ethereum,
            )
            .await?,
        );
        let barrier = Arc::new(Barrier::new(CONCURRENT_CALLS + 1));

        let handles: Vec<_> = (0..CONCURRENT_CALLS)
            .map(|_| {
                let app = app.clone();
                let barrier = Arc::clone(&barrier);
                let address = fetched.address.clone();
                tokio::spawn(async move {
                    barrier.wait().await;
                    post_json(
                        app,
                        "/tokens/lookup",
                        serde_json::json!({
                            "chainId": Chain::Ethereum.id(),
                            "addresses": [address],
                        }),
                    )
                    .await
                })
            })
            .collect();

        barrier.wait().await;

        for handle in handles {
            let (status, body) = handle.await??;
            assert_eq!(status, StatusCode::OK);
            assert_eq!(body["tokens"][0]["symbol"], "SHARED");
        }
        server_task.abort();

        assert_eq!(request_count.load(Ordering::SeqCst), 1);
        Ok(())
    }

    #[tokio::test]
    async fn token_lookup_rejects_mismatched_chain_id() -> Result<()> {
        let app = create_broadcaster_router(build_state(SeedMode::Disconnected).await?);

        let (status, body) = post_json(
            app,
            "/tokens/lookup",
            serde_json::json!({
                "chainId": Chain::Base.id(),
                "addresses": [address(0x45)],
            }),
        )
        .await?;

        assert_eq!(status, StatusCode::BAD_REQUEST);
        let Some(error) = body["error"].as_str() else {
            unreachable!("expected JSON error string");
        };
        assert!(error.contains("does not match"));
        Ok(())
    }

    #[tokio::test]
    async fn token_lookup_rejects_malformed_addresses() -> Result<()> {
        let app = create_broadcaster_router(build_state(SeedMode::Disconnected).await?);

        let (status, body) = post_json(
            app,
            "/tokens/lookup",
            serde_json::json!({
                "chainId": Chain::Ethereum.id(),
                "addresses": ["0x1234"],
            }),
        )
        .await?;

        assert_eq!(status, StatusCode::BAD_REQUEST);
        let Some(error) = body["error"].as_str() else {
            unreachable!("expected JSON error string");
        };
        assert!(error.contains("20-byte EVM address"));
        Ok(())
    }

    async fn build_state(mode: SeedMode) -> Result<BroadcasterAppState> {
        build_state_with_tokens(
            mode,
            token_store(vec![], "http://127.0.0.1:1".to_string(), Chain::Ethereum),
            Chain::Ethereum,
        )
        .await
    }

    async fn build_state_with_rfq(
        raw_mode: SeedMode,
        rfq_mode: SeedMode,
    ) -> Result<BroadcasterAppState> {
        let tokens = token_store(vec![], "http://127.0.0.1:1".to_string(), Chain::Ethereum);
        let (publisher, gate) = publisher_and_gate(RpcFakeRedisWriter::healthy());
        let raw_service = service_with_backend(
            raw_mode,
            BroadcasterBackend::Native,
            publisher.clone(),
            gate.clone(),
        )
        .await?;
        let rfq_service =
            service_with_backend(rfq_mode, BroadcasterBackend::Rfq, publisher.clone(), gate)
                .await?;
        if matches!(raw_mode, SeedMode::Ready) {
            BroadcasterServiceState::promote_when_ready(
                &[raw_service.clone(), rfq_service.clone()],
                "active_writer_promoted",
            )
            .await?;
        }
        Ok(BroadcasterAppState::with_snapshot_session_ttl(
            raw_service,
            Some(rfq_service),
            tokens,
            Chain::Ethereum.id(),
            Duration::from_secs(300),
            publisher,
        ))
    }

    async fn build_state_with_tokens(
        mode: SeedMode,
        tokens: Arc<TokenStore>,
        chain: Chain,
    ) -> Result<BroadcasterAppState> {
        let cache = BroadcasterSnapshotCache::new(1, vec![BroadcasterBackend::Native]);
        let upstream = BroadcasterUpstreamState::default();
        let (publisher, gate) = publisher_and_gate(RpcFakeRedisWriter::healthy());
        let service = BroadcasterServiceState::with_lifecycle_gate(
            8_388_608,
            cache,
            upstream,
            publisher.clone(),
            gate,
        );

        match mode {
            SeedMode::Disconnected => {}
            SeedMode::WarmingUp => service.mark_upstream_connected().await,
            SeedMode::Ready => {
                service.mark_upstream_connected().await;
                service.apply_update(&native_only_update()).await?;
                BroadcasterServiceState::promote_when_ready(
                    std::slice::from_ref(&service),
                    "active_writer_promoted",
                )
                .await?;
            }
        }

        Ok(BroadcasterAppState::with_snapshot_session_ttl(
            service,
            None,
            tokens,
            chain.id(),
            Duration::from_secs(300),
            publisher,
        ))
    }

    async fn build_state_with_stream_token(
        initial_tokens: Vec<Token>,
        stream_token: Token,
        token_min_quality: u32,
    ) -> Result<BroadcasterAppState> {
        let tokens = token_store(
            initial_tokens,
            "http://127.0.0.1:1".to_string(),
            Chain::Ethereum,
        );
        let cache = BroadcasterSnapshotCache::with_token_catalog(
            Chain::Ethereum.id(),
            vec![BroadcasterBackend::Native],
            Arc::clone(&tokens),
            token_min_quality,
        );
        let (publisher, gate) = publisher_and_gate(RpcFakeRedisWriter::healthy());
        publisher
            .promote(
                vec![BroadcasterBackendHead::new(BroadcasterBackend::Native, 0)],
                "active_writer_promoted",
            )
            .await?;
        let service = BroadcasterServiceState::with_lifecycle_gate(
            8_388_608,
            cache,
            BroadcasterUpstreamState::default(),
            Arc::clone(&publisher),
            gate,
        );
        service.mark_upstream_connected().await;
        service
            .apply_feed_message(&raw_feed_with_new_token(stream_token))
            .await?;

        Ok(BroadcasterAppState::with_snapshot_session_ttl(
            service,
            None,
            tokens,
            Chain::Ethereum.id(),
            Duration::from_secs(300),
            publisher,
        ))
    }

    async fn build_state_with_recovery_stream_token(
        stream_token: Token,
        token_min_quality: u32,
    ) -> Result<BroadcasterAppState> {
        let tokens = token_store(
            Vec::new(),
            "http://127.0.0.1:1".to_string(),
            Chain::Ethereum,
        );
        let cache = BroadcasterSnapshotCache::with_token_catalog(
            Chain::Ethereum.id(),
            vec![BroadcasterBackend::Native],
            Arc::clone(&tokens),
            token_min_quality,
        );
        let (publisher, gate) = publisher_and_gate(RpcFakeRedisWriter::healthy());
        publisher
            .promote(
                vec![BroadcasterBackendHead::new(BroadcasterBackend::Native, 0)],
                "active_writer_promoted",
            )
            .await?;
        let service = BroadcasterServiceState::with_lifecycle_gate(
            8_388_608,
            cache.clone(),
            BroadcasterUpstreamState::default(),
            Arc::clone(&publisher),
            gate,
        );
        service.mark_upstream_connected().await;
        service
            .apply_feed_message(&raw_feed(&["uniswap_v2", "sushiswap_v2"], 1, None))
            .await?;

        cache.begin_same_generation_recovery().await;
        let published = service
            .apply_feed_message(&raw_feed(&["uniswap_v2"], 2, Some(stream_token)))
            .await?;
        assert!(!published);
        assert!(service.status_snapshot().await.snapshot.recovery_pending);

        Ok(BroadcasterAppState::with_snapshot_session_ttl(
            service,
            None,
            tokens,
            Chain::Ethereum.id(),
            Duration::from_secs(300),
            publisher,
        ))
    }

    async fn build_state_with_redis(writer: RpcFakeRedisWriter) -> Result<BroadcasterAppState> {
        Ok(build_state_with_redis_and_service(writer).await?.0)
    }

    async fn build_state_with_redis_and_service(
        writer: RpcFakeRedisWriter,
    ) -> Result<(BroadcasterAppState, BroadcasterServiceState)> {
        let cache = BroadcasterSnapshotCache::new(1, vec![BroadcasterBackend::Native]);
        let upstream = BroadcasterUpstreamState::default();
        let (publisher, gate) = publisher_and_gate(writer);
        let service = BroadcasterServiceState::with_lifecycle_gate(
            8_388_608,
            cache,
            upstream,
            publisher.clone(),
            gate,
        );
        service.mark_upstream_connected().await;
        service.apply_update(&native_only_update()).await?;
        BroadcasterServiceState::promote_when_ready(
            std::slice::from_ref(&service),
            "active_writer_promoted",
        )
        .await?;
        let app_state = BroadcasterAppState::with_snapshot_session_ttl(
            service.clone(),
            None,
            token_store(vec![], "http://127.0.0.1:1".to_string(), Chain::Ethereum),
            Chain::Ethereum.id(),
            Duration::from_secs(300),
            publisher,
        );
        Ok((app_state, service))
    }

    async fn build_stale_active_state(writer: RpcFakeRedisWriter) -> Result<BroadcasterAppState> {
        let cache = BroadcasterSnapshotCache::new(1, vec![BroadcasterBackend::Native]);
        let upstream = BroadcasterUpstreamState::default();
        let (old_publisher, gate) = publisher_and_gate(writer.clone());
        let service = BroadcasterServiceState::with_lifecycle_gate(
            8_388_608,
            cache,
            upstream,
            old_publisher.clone(),
            gate,
        );
        service.mark_upstream_connected().await;
        service.apply_update(&native_only_update()).await?;
        BroadcasterServiceState::promote_when_ready(std::slice::from_ref(&service), "old_active")
            .await?;

        let new_publisher =
            BroadcasterRedisPublisher::new(redis_publisher_config(), Arc::new(writer));
        new_publisher
            .promote(
                vec![BroadcasterBackendHead::new(BroadcasterBackend::Native, 0)],
                "new_active",
            )
            .await?;

        Ok(BroadcasterAppState::with_snapshot_session_ttl(
            service,
            None,
            token_store(vec![], "http://127.0.0.1:1".to_string(), Chain::Ethereum),
            Chain::Ethereum.id(),
            Duration::from_secs(300),
            old_publisher,
        ))
    }

    async fn build_state_with_unhealthy_redis() -> Result<BroadcasterAppState> {
        let cache = BroadcasterSnapshotCache::new(1, vec![BroadcasterBackend::Native]);
        let upstream = BroadcasterUpstreamState::default();
        let (publisher, gate) =
            publisher_and_gate(RpcFakeRedisWriter::failing_after_first_append());
        let service = BroadcasterServiceState::with_lifecycle_gate(
            8_388_608,
            cache,
            upstream,
            publisher.clone(),
            gate,
        );
        service.mark_upstream_connected().await;
        service.apply_update(&native_only_update()).await?;
        BroadcasterServiceState::promote_when_ready(
            std::slice::from_ref(&service),
            "active_writer_promoted",
        )
        .await?;
        let Err(_error) = service.broadcast_heartbeat().await else {
            bail!("expected heartbeat to mark Redis publisher unhealthy");
        };
        Ok(BroadcasterAppState::with_snapshot_session_ttl(
            service,
            None,
            token_store(vec![], "http://127.0.0.1:1".to_string(), Chain::Ethereum),
            Chain::Ethereum.id(),
            Duration::from_secs(300),
            publisher,
        ))
    }

    async fn build_warming_state_with_redis() -> Result<BroadcasterAppState> {
        let cache = BroadcasterSnapshotCache::new(1, vec![BroadcasterBackend::Native]);
        let upstream = BroadcasterUpstreamState::default();
        let (publisher, gate) = publisher_and_gate(RpcFakeRedisWriter::healthy());
        let service = BroadcasterServiceState::with_lifecycle_gate(
            8_388_608,
            cache,
            upstream,
            publisher.clone(),
            gate,
        );
        service.mark_upstream_connected().await;
        Ok(BroadcasterAppState::with_snapshot_session_ttl(
            service,
            None,
            token_store(vec![], "http://127.0.0.1:1".to_string(), Chain::Ethereum),
            Chain::Ethereum.id(),
            Duration::from_secs(300),
            publisher,
        ))
    }

    async fn build_passive_ready_state() -> Result<BroadcasterAppState> {
        let cache = BroadcasterSnapshotCache::new(1, vec![BroadcasterBackend::Native]);
        let upstream = BroadcasterUpstreamState::default();
        let (publisher, gate) = publisher_and_gate(RpcFakeRedisWriter::healthy());
        let service = BroadcasterServiceState::with_lifecycle_gate(
            8_388_608,
            cache,
            upstream,
            publisher.clone(),
            gate,
        );
        service.mark_upstream_connected().await;
        service.apply_update(&native_only_update()).await?;
        Ok(BroadcasterAppState::with_snapshot_session_ttl(
            service,
            None,
            token_store(vec![], "http://127.0.0.1:1".to_string(), Chain::Ethereum),
            Chain::Ethereum.id(),
            Duration::from_secs(300),
            publisher,
        ))
    }

    async fn service_with_backend(
        mode: SeedMode,
        backend: BroadcasterBackend,
        publisher: Arc<BroadcasterRedisPublisher>,
        lifecycle_gate: Arc<Mutex<()>>,
    ) -> Result<BroadcasterServiceState> {
        let cache = BroadcasterSnapshotCache::new(1, vec![backend]);
        let upstream = BroadcasterUpstreamState::default();
        let service = BroadcasterServiceState::with_lifecycle_gate(
            8_388_608,
            cache,
            upstream,
            publisher,
            lifecycle_gate,
        );

        match mode {
            SeedMode::Disconnected => {}
            SeedMode::WarmingUp => service.mark_upstream_connected().await,
            SeedMode::Ready => {
                service.mark_upstream_connected().await;
                match backend {
                    BroadcasterBackend::Native => {
                        service.apply_update(&native_only_update()).await?
                    }
                    BroadcasterBackend::Rfq => service.apply_update(&rfq_only_update()).await?,
                    BroadcasterBackend::Vm => unreachable!("vm test service is not used"),
                }
            }
        }

        Ok(service)
    }

    fn publisher_and_gate(
        writer: RpcFakeRedisWriter,
    ) -> (Arc<BroadcasterRedisPublisher>, Arc<Mutex<()>>) {
        (
            Arc::new(BroadcasterRedisPublisher::new(
                redis_publisher_config(),
                Arc::new(writer),
            )),
            Arc::new(Mutex::new(())),
        )
    }

    #[derive(Debug, Clone)]
    struct RpcFakeRedisWriter {
        fail_after_successes: Option<usize>,
        state: Arc<Mutex<RpcFakeRedisWriterState>>,
        append_started: Arc<Notify>,
        append_release: Arc<Notify>,
    }

    #[derive(Debug, Default)]
    struct RpcFakeRedisWriterState {
        append_count: usize,
        active_token: Option<String>,
        active_generation: u64,
        first_entry_sequence: u64,
        fail_next_inspects: usize,
        inspect_count: usize,
        block_next_append: bool,
    }

    impl RpcFakeRedisWriter {
        fn healthy() -> Self {
            Self {
                fail_after_successes: None,
                state: Arc::new(Mutex::new(RpcFakeRedisWriterState {
                    first_entry_sequence: 1,
                    ..Default::default()
                })),
                append_started: Arc::new(Notify::new()),
                append_release: Arc::new(Notify::new()),
            }
        }

        fn failing_after_first_append() -> Self {
            Self {
                fail_after_successes: Some(1),
                state: Arc::new(Mutex::new(RpcFakeRedisWriterState {
                    first_entry_sequence: 1,
                    ..Default::default()
                })),
                append_started: Arc::new(Notify::new()),
                append_release: Arc::new(Notify::new()),
            }
        }

        async fn set_first_entry_sequence(&self, sequence: u64) {
            self.state.lock().await.first_entry_sequence = sequence;
        }

        async fn fail_next_inspect(&self) {
            self.state.lock().await.fail_next_inspects = 1;
        }

        async fn inspect_count(&self) -> usize {
            self.state.lock().await.inspect_count
        }

        async fn block_next_append(&self) {
            self.state.lock().await.block_next_append = true;
        }

        async fn wait_for_blocked_append(&self) {
            self.append_started.notified().await;
        }

        fn release_blocked_append(&self) {
            self.append_release.notify_one();
        }
    }

    impl RedisStreamWriter for RpcFakeRedisWriter {
        fn promote<'a>(
            &'a self,
            command: runtime::broadcaster::redis_publisher::RedisPromotionCommand<'a>,
        ) -> futures::future::BoxFuture<
            'a,
            Result<runtime::broadcaster::redis_publisher::RedisPromotionResult>,
        > {
            Box::pin(async move {
                let mut state = self.state.lock().await;
                if command.expected_writer_token.is_some_and(|expected_token| {
                    state.active_token.as_deref() != Some(expected_token)
                        || Some(state.active_generation) != command.expected_generation
                }) {
                    bail!("stale Redis broadcaster writer token");
                }
                state.active_generation = state.active_generation.saturating_add(1);
                state.active_token = Some(command.writer_token.to_string());
                let generation = state.active_generation;
                state.append_count = state.append_count.saturating_add(1);
                Ok(
                    runtime::broadcaster::redis_publisher::RedisPromotionResult {
                        generation,
                        entry_id: format!("{generation}-1"),
                    },
                )
            })
        }

        fn append_fenced<'a>(
            &'a self,
            command: runtime::broadcaster::redis_publisher::RedisAppendCommand<'a>,
        ) -> futures::future::BoxFuture<'a, Result<String>> {
            Box::pin(async move {
                let blocked = {
                    let mut state = self.state.lock().await;
                    let blocked = state.block_next_append;
                    state.block_next_append = false;
                    blocked
                };
                if blocked {
                    self.append_started.notify_one();
                    self.append_release.notified().await;
                }
                let mut state = self.state.lock().await;
                if state.active_token.as_deref() != Some(command.writer_token)
                    || state.active_generation != command.generation
                {
                    bail!("stale Redis broadcaster writer token");
                }
                if self
                    .fail_after_successes
                    .is_some_and(|threshold| state.append_count >= threshold)
                {
                    bail!("planned append failure");
                }
                let entry_id = format!("{}-{}", command.generation, command.entry.message_seq);
                state.append_count = state.append_count.saturating_add(1);
                Ok(entry_id)
            })
        }

        fn renew_writer<'a>(
            &'a self,
            command: runtime::broadcaster::redis_publisher::RedisRenewCommand<'a>,
        ) -> futures::future::BoxFuture<'a, Result<()>> {
            Box::pin(async move {
                let state = self.state.lock().await;
                if state.active_token.as_deref() != Some(command.writer_token)
                    || state.active_generation != command.generation
                {
                    bail!("stale Redis broadcaster writer token");
                }
                Ok(())
            })
        }

        fn inspect_writer<'a>(
            &'a self,
            command: runtime::broadcaster::redis_publisher::RedisInspectCommand<'a>,
        ) -> futures::future::BoxFuture<
            'a,
            Result<runtime::broadcaster::redis_publisher::RedisWriterInspection>,
        > {
            Box::pin(async move {
                let mut state = self.state.lock().await;
                if state.active_token.as_deref() != Some(command.writer_token)
                    || state.active_generation != command.generation
                {
                    bail!("stale Redis broadcaster writer token");
                }
                state.inspect_count = state.inspect_count.saturating_add(1);
                if state.fail_next_inspects > 0 {
                    state.fail_next_inspects -= 1;
                    bail!("planned Redis inspect failure");
                }
                Ok(
                    runtime::broadcaster::redis_publisher::RedisWriterInspection {
                        generation: state.active_generation,
                        first_entry_id: Some(format!(
                            "{}-{}",
                            state.active_generation, state.first_entry_sequence
                        )),
                        last_entry_id: Some(format!(
                            "{}-{}",
                            state.active_generation, state.append_count
                        )),
                    },
                )
            })
        }
    }

    fn redis_publisher_config() -> BroadcasterRedisPublisherConfig {
        BroadcasterRedisPublisherConfig {
            stream_key: "dsolver:broadcaster:test:events".to_string(),
            chain_id: Chain::Ethereum.id(),
            append_retry_window: Duration::from_millis(1),
            maxlen: None,
            writer_lease_ttl: Duration::from_secs(30),
            recovery_max_buffered_native_blocks: 64,
        }
    }

    async fn get_json(app: Router, uri: &str) -> Result<(StatusCode, serde_json::Value)> {
        let response = app
            .oneshot(Request::builder().uri(uri).body(Body::empty())?)
            .await?;
        let status = response.status();
        let body: serde_json::Value =
            serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await?)?;
        Ok((status, body))
    }

    async fn post_json(
        app: Router,
        uri: &str,
        body: serde_json::Value,
    ) -> Result<(StatusCode, serde_json::Value)> {
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(uri)
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_vec(&body)?))?,
            )
            .await?;
        let status = response.status();
        let body: serde_json::Value =
            serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await?)?;
        Ok((status, body))
    }

    fn native_only_update() -> Update {
        let mut new_pairs = HashMap::new();
        new_pairs.insert(
            "native-1".to_string(),
            native_component("native-1", "uniswap_v2"),
        );

        let mut states = HashMap::new();
        states.insert(
            "native-1".to_string(),
            Box::new(DummySim(1)) as Box<dyn ProtocolSim>,
        );

        Update::new(10, states, new_pairs).set_sync_states(HashMap::from([(
            "uniswap_v2".to_string(),
            SynchronizerState::Ready(block_header(10, 1)),
        )]))
    }

    fn rfq_only_update() -> Update {
        let mut new_pairs = HashMap::new();
        new_pairs.insert(
            "rfq-1".to_string(),
            native_component("rfq-1", "rfq:hashflow"),
        );

        let mut states = HashMap::new();
        states.insert(
            "rfq-1".to_string(),
            Box::new(DummySim(7)) as Box<dyn ProtocolSim>,
        );

        Update::new(12, states, new_pairs).set_sync_states(HashMap::from([(
            "rfq:hashflow".to_string(),
            SynchronizerState::Ready(block_header(12, 1)),
        )]))
    }

    fn native_component(_id: &str, protocol: &str) -> ProtocolComponent {
        ProtocolComponent::new(
            Bytes::from([3u8; 20]),
            protocol.to_string(),
            protocol.to_string(),
            Chain::Ethereum,
            vec![dummy_token(1, "TKNA"), dummy_token(2, "TKNB")],
            Vec::new(),
            HashMap::new(),
            Bytes::from([9u8; 32]),
            chrono::DateTime::UNIX_EPOCH.naive_utc(),
        )
    }

    fn dummy_token(seed: u8, symbol: &str) -> Token {
        Token::new(
            &Bytes::from([seed; 20]),
            symbol,
            18,
            0,
            &[],
            Chain::Ethereum,
            1,
        )
    }

    fn address(seed: u8) -> Bytes {
        Bytes::from([seed; 20])
    }

    fn token(seed: u8, symbol: &str, chain: Chain) -> Token {
        Token::new(&address(seed), symbol, 18, 0, &[Some(21_000)], chain, 100)
    }

    fn token_with_metadata(seed: u8, symbol: &str, decimals: u32, quality: u32) -> Token {
        Token::new(
            &address(seed),
            symbol,
            decimals,
            0,
            &[Some(21_000)],
            Chain::Ethereum,
            quality,
        )
    }

    fn raw_feed_with_new_token(token: Token) -> FeedMessage<BlockHeader> {
        raw_feed(&["uniswap_v2"], 1, Some(token))
    }

    fn raw_feed(
        protocols: &[&str],
        block_number: u64,
        token: Option<Token>,
    ) -> FeedMessage<BlockHeader> {
        let header = BlockHeader {
            hash: Bytes::from([(block_number + 1) as u8; 32]),
            number: block_number,
            parent_hash: Bytes::from([block_number as u8; 32]),
            revert: false,
            timestamp: block_number * 10,
            partial_block_index: None,
        };
        FeedMessage {
            state_msgs: protocols
                .iter()
                .map(|protocol| {
                    let mut deltas = BlockAggregatedChanges::default();
                    if let Some(token) = token.clone() {
                        deltas.new_tokens.insert(token.address.clone(), token);
                    }
                    (
                        (*protocol).to_string(),
                        StateSyncMessage {
                            header: header.clone(),
                            snapshots: Snapshot::default(),
                            deltas: Some(deltas),
                            removed_components: HashMap::new(),
                        },
                    )
                })
                .collect(),
            sync_states: protocols
                .iter()
                .map(|protocol| {
                    (
                        (*protocol).to_string(),
                        SynchronizerState::Ready(header.clone()),
                    )
                })
                .collect(),
        }
    }

    fn token_store(
        tokens: impl IntoIterator<Item = Token>,
        tycho_url: String,
        chain: Chain,
    ) -> Arc<TokenStore> {
        let initial = tokens
            .into_iter()
            .map(|token| (token.address.clone(), token))
            .collect();
        Arc::new(TokenStore::new(
            initial,
            tycho_url,
            "test-api-key".to_string(),
            chain,
            Duration::from_secs(1),
        ))
    }

    #[derive(Clone)]
    struct TychoTokenServerState {
        token: Option<Token>,
        request_count: Arc<AtomicUsize>,
        delay: Duration,
    }

    async fn spawn_tycho_token_server(
        token: Option<Token>,
        delay: Duration,
    ) -> Result<(String, Arc<AtomicUsize>, JoinHandle<()>)> {
        let request_count = Arc::new(AtomicUsize::new(0));
        let state = TychoTokenServerState {
            token,
            request_count: Arc::clone(&request_count),
            delay,
        };
        let app = Router::new()
            .route("/v1/tokens", post(tycho_tokens))
            .with_state(state);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("http://{}", listener.local_addr()?);
        let server_task = tokio::spawn(async move {
            let _ = axum::serve(listener, app.into_make_service()).await;
        });
        Ok((url, request_count, server_task))
    }

    async fn tycho_tokens(
        axum::extract::State(state): axum::extract::State<TychoTokenServerState>,
    ) -> Json<TokensRequestResponse> {
        state.request_count.fetch_add(1, Ordering::SeqCst);
        if !state.delay.is_zero() {
            sleep(state.delay).await;
        }
        let tokens = state.token.into_iter().map(Into::into).collect();
        Json(TokensRequestResponse::new(
            tokens,
            &PaginationResponse::new(0, 100, 0),
        ))
    }

    fn block_header(number: u64, seed: u8) -> BlockHeader {
        BlockHeader {
            hash: Bytes::from(vec![seed; 32]),
            number,
            parent_hash: Bytes::from(vec![seed.saturating_add(1); 32]),
            revert: false,
            timestamp: number * 10,
            partial_block_index: None,
        }
    }
}
