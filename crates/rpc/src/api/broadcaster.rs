use axum::{
    routing::{get, post},
    Router,
};

use runtime::broadcaster_service::BroadcasterAppState;

use crate::handlers::broadcaster::{
    create_snapshot_session, snapshot_session_payload, status, token_lookup, token_snapshot, ws,
};

pub fn create_broadcaster_router(app_state: BroadcasterAppState) -> Router {
    Router::new()
        .route("/status", get(status))
        .route("/snapshot-sessions", post(create_snapshot_session))
        .route(
            "/snapshot-sessions/:session_id/payloads/:index",
            get(snapshot_session_payload),
        )
        .route("/ws", get(ws))
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
    use futures::StreamExt;
    use num_bigint::BigUint;
    use num_traits::Zero;
    use runtime::{
        broadcaster::redis_publisher::{
            BroadcasterRedisPublisher, BroadcasterRedisPublisherConfig,
            BroadcasterRedisSnapshotSource, RedisStreamWriter,
        },
        broadcaster::state::{BroadcasterSnapshotCache, BroadcasterUpstreamState},
        broadcaster_service::BroadcasterAppState,
        config::BroadcasterRedisConfig,
        models::tokens::TokenStore,
        services::broadcaster::BroadcasterServiceState,
    };
    use simulator_core::broadcaster::{
        BroadcasterBackend, BroadcasterRedisSnapshotPointer, BroadcasterRedisStreamEntry,
    };
    use tokio::{
        sync::{Barrier, Mutex},
        task::JoinHandle,
        time::{sleep, timeout},
    };
    use tokio_tungstenite::{connect_async, tungstenite};
    use tower::ServiceExt;
    use tycho_simulation::{
        protocol::models::{ProtocolComponent, Update},
        tycho_client::feed::{BlockHeader, SynchronizerState},
        tycho_common::{
            dto::{PaginationResponse, ProtocolStateDelta, TokensRequestResponse},
            models::{token::Token, Chain},
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

        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
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
        let (status, body) = get_json(app, "/status").await?;

        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body["status"], "snapshot_warming_up");
        assert_eq!(body["upstream"]["connected"], true);
        assert_eq!(body["snapshot"]["ready"], false);
        Ok(())
    }

    #[tokio::test]
    async fn status_reports_ready_once_snapshot_is_bootstrapped() -> Result<()> {
        let app = create_broadcaster_router(build_state(SeedMode::Ready).await?);
        let (status, body) = get_json(app, "/status").await?;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["status"], "ready");
        assert_eq!(body["snapshot"]["ready"], true);
        assert_eq!(body["backends"]["native"]["block_number"], 10);
        assert_eq!(body["backends"]["native"]["pool_count"], 1);
        Ok(())
    }

    #[tokio::test]
    async fn status_includes_redis_publisher_when_attached() -> Result<()> {
        let app =
            create_broadcaster_router(build_state_with_redis(RpcFakeRedisWriter::healthy()).await?);

        let (status, body) = get_json(app, "/status").await?;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["status"], "ready");
        assert_eq!(body["redis_publisher"]["healthy"], true);
        let Some(stream_id) = body["redis_publisher"]["stream_id"].as_str() else {
            bail!("expected redis publisher stream_id");
        };
        assert!(stream_id.starts_with("chain-1-redis-stream-"));
        Ok(())
    }

    #[tokio::test]
    async fn status_reports_503_when_redis_publisher_is_unhealthy() -> Result<()> {
        let app = create_broadcaster_router(
            build_state_with_redis(RpcFakeRedisWriter::failing_appends()).await?,
        );

        let (status, body) = get_json(app, "/status").await?;

        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body["status"], "redis_publisher_unhealthy");
        assert_eq!(body["redis_publisher"]["healthy"], false);
        Ok(())
    }

    #[tokio::test]
    async fn status_preserves_snapshot_warming_while_redis_waits_for_snapshot() -> Result<()> {
        let app = create_broadcaster_router(build_warming_state_with_redis().await?);

        let (status, body) = get_json(app, "/status").await?;

        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body["status"], "snapshot_warming_up");
        assert_eq!(body["redis_publisher"]["healthy"], false);
        Ok(())
    }

    #[tokio::test]
    async fn snapshot_session_create_rejects_until_ready() -> Result<()> {
        let app = create_broadcaster_router(build_state(SeedMode::WarmingUp).await?);
        let (status, body) = post_json(app, "/snapshot-sessions", serde_json::json!({})).await?;

        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body["status"], "snapshot_warming_up");
        Ok(())
    }

    #[tokio::test]
    async fn snapshot_session_create_serves_payload_metadata_and_payloads() -> Result<()> {
        let app = create_broadcaster_router(build_state(SeedMode::Ready).await?);
        let (status, body) =
            post_json(app.clone(), "/snapshot-sessions", serde_json::json!({})).await?;

        assert_eq!(status, StatusCode::CREATED);
        assert_eq!(body["chainId"], Chain::Ethereum.id());
        assert_eq!(body["streamId"], "chain-1-stream-1");
        assert_eq!(body["snapshotId"], "chain-1-snapshot-1");
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

    #[tokio::test]
    async fn root_status_reports_warming_rfq_while_snapshot_sessions_remain_native_only(
    ) -> Result<()> {
        let app = create_broadcaster_router(
            build_state_with_rfq(SeedMode::Ready, SeedMode::WarmingUp).await?,
        );
        let (status, body) = get_json(app.clone(), "/status").await?;

        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body["status"], "snapshot_warming_up");
        assert_eq!(
            body["snapshot"]["configured_backends"],
            serde_json::json!(["native", "rfq"])
        );
        assert!(body["backends"]["rfq"].is_object());

        let (status, body) =
            post_json(app.clone(), "/snapshot-sessions", serde_json::json!({})).await?;
        assert_eq!(status, StatusCode::CREATED);
        let session_id = body["sessionId"]
            .as_u64()
            .ok_or_else(|| anyhow!("expected numeric sessionId"))?;
        let (_status, payload) =
            get_json(app, &format!("/snapshot-sessions/{session_id}/payloads/0")).await?;
        assert_eq!(payload["backends"], serde_json::json!(["native"]));
        Ok(())
    }

    #[tokio::test]
    async fn rfq_routes_are_not_registered() -> Result<()> {
        let app = create_broadcaster_router(
            build_state_with_rfq(SeedMode::Ready, SeedMode::Ready).await?,
        );

        assert_eq!(
            get_status(app.clone(), "/rfq/status").await?,
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            post_status(app.clone(), "/rfq/snapshot-sessions").await?,
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            get_status(app.clone(), "/rfq/snapshot-sessions/1/payloads/0").await?,
            StatusCode::NOT_FOUND
        );
        assert_eq!(get_status(app, "/rfq/ws").await?, StatusCode::NOT_FOUND);
        Ok(())
    }

    #[tokio::test]
    async fn root_status_reports_rfq_backend_readiness() -> Result<()> {
        let app = create_broadcaster_router(
            build_state_with_rfq(SeedMode::Ready, SeedMode::WarmingUp).await?,
        );

        let (status, body) = get_json(app, "/status").await?;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body["status"], "snapshot_warming_up");
        assert_eq!(
            body["snapshot"]["configured_backends"],
            serde_json::json!(["native", "rfq"])
        );
        assert!(body["upstream"]["connected"].as_bool().unwrap_or(false));
        assert!(!body["snapshot"]["ready"].as_bool().unwrap_or(true));
        Ok(())
    }

    #[tokio::test]
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
    async fn websocket_upgrade_requires_session_id() -> Result<()> {
        let (url, server_task) = spawn_server(
            create_broadcaster_router(build_state(SeedMode::Ready).await?),
            "/ws",
        )
        .await?;
        let result = connect_async(url).await;
        server_task.abort();

        match result {
            Err(tungstenite::Error::Http(response)) => {
                assert_eq!(response.status(), StatusCode::BAD_REQUEST);
            }
            Err(error) => bail!("unexpected websocket error: {error}"),
            Ok(_) => bail!("expected websocket handshake rejection"),
        }
        Ok(())
    }

    #[tokio::test]
    async fn websocket_upgrade_rejects_unknown_session_id() -> Result<()> {
        let (url, server_task) = spawn_server(
            create_broadcaster_router(build_state(SeedMode::Ready).await?),
            "/ws?sessionId=404",
        )
        .await?;
        let result = connect_async(url).await;
        server_task.abort();

        match result {
            Err(tungstenite::Error::Http(response)) => {
                assert_eq!(response.status(), StatusCode::NOT_FOUND);
            }
            Err(error) => bail!("unexpected websocket error: {error}"),
            Ok(_) => bail!("expected websocket handshake rejection"),
        }
        Ok(())
    }

    #[tokio::test]
    async fn websocket_upgrade_attaches_snapshot_session_without_replaying_snapshot() -> Result<()>
    {
        let state = build_state(SeedMode::Ready).await?;
        let session = state
            .create_snapshot_session()
            .await?
            .ok_or_else(|| anyhow!("expected ready snapshot session"))?;
        let (url, server_task) = spawn_server(
            create_broadcaster_router(state),
            &format!("/ws?sessionId={}", session.session_id),
        )
        .await?;
        let result = connect_async(url).await;

        let (mut stream, response) = match result {
            Ok(result) => result,
            Err(error) => bail!("expected websocket handshake success: {error}"),
        };
        assert_eq!(response.status(), StatusCode::SWITCHING_PROTOCOLS);
        assert!(
            timeout(Duration::from_millis(25), stream.next())
                .await
                .is_err(),
            "websocket attach should wait for live messages instead of replaying snapshot"
        );
        server_task.abort();
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
                "chainId": Chain::Base.id(),
                "addresses": [address(0x45)],
            }),
        )
        .await?;
        server_task.abort();

        assert_eq!(status, StatusCode::BAD_REQUEST);
        let Some(error) = body["error"].as_str() else {
            unreachable!("expected JSON error string");
        };
        assert!(error.contains("does not match"));
        Ok(())
    }

    #[tokio::test]
    async fn token_lookup_rejects_malformed_addresses() -> Result<()> {
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
                "addresses": ["0x1234"],
            }),
        )
        .await?;
        server_task.abort();

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
        let raw_service = service_with_backend(raw_mode, BroadcasterBackend::Native).await?;
        let rfq_service = service_with_backend(rfq_mode, BroadcasterBackend::Rfq).await?;
        Ok(BroadcasterAppState::with_rfq_snapshot_session_ttl(
            raw_service,
            rfq_service,
            tokens,
            Chain::Ethereum.id(),
            Duration::from_secs(300),
        ))
    }

    async fn build_state_with_tokens(
        mode: SeedMode,
        tokens: Arc<TokenStore>,
        chain: Chain,
    ) -> Result<BroadcasterAppState> {
        let cache = BroadcasterSnapshotCache::new(1, vec![BroadcasterBackend::Native]);
        let upstream = BroadcasterUpstreamState::default();
        let service = BroadcasterServiceState::new(8_388_608, 8, cache, upstream);

        match mode {
            SeedMode::Disconnected => {}
            SeedMode::WarmingUp => service.mark_upstream_connected().await,
            SeedMode::Ready => {
                service.mark_upstream_connected().await;
                service.apply_update(&native_only_update()).await?;
            }
        }

        Ok(BroadcasterAppState::new(service, tokens, chain.id()))
    }

    async fn build_state_with_redis(writer: RpcFakeRedisWriter) -> Result<BroadcasterAppState> {
        let cache = BroadcasterSnapshotCache::new(1, vec![BroadcasterBackend::Native]);
        let upstream = BroadcasterUpstreamState::default();
        let service = BroadcasterServiceState::new(8_388_608, 8, cache.clone(), upstream);
        service.mark_upstream_connected().await;
        service.apply_update(&native_only_update()).await?;
        let publisher = Arc::new(BroadcasterRedisPublisher::new(
            redis_publisher_config(),
            vec![BroadcasterRedisSnapshotSource::new(
                cache,
                vec![BroadcasterBackend::Native],
            )],
            Arc::new(writer),
        ));
        let _ = publisher.publish_snapshot_if_ready().await;
        Ok(BroadcasterAppState::with_snapshot_session_ttl(
            service,
            token_store(vec![], "http://127.0.0.1:1".to_string(), Chain::Ethereum),
            Chain::Ethereum.id(),
            Duration::from_secs(300),
        )
        .with_redis_publisher(publisher))
    }

    async fn build_warming_state_with_redis() -> Result<BroadcasterAppState> {
        let cache = BroadcasterSnapshotCache::new(1, vec![BroadcasterBackend::Native]);
        let upstream = BroadcasterUpstreamState::default();
        let service = BroadcasterServiceState::new(8_388_608, 8, cache.clone(), upstream);
        service.mark_upstream_connected().await;
        let publisher = Arc::new(BroadcasterRedisPublisher::new(
            redis_publisher_config(),
            vec![BroadcasterRedisSnapshotSource::new(
                cache,
                vec![BroadcasterBackend::Native],
            )],
            Arc::new(RpcFakeRedisWriter::healthy()),
        ));
        Ok(BroadcasterAppState::with_snapshot_session_ttl(
            service,
            token_store(vec![], "http://127.0.0.1:1".to_string(), Chain::Ethereum),
            Chain::Ethereum.id(),
            Duration::from_secs(300),
        )
        .with_redis_publisher(publisher))
    }

    async fn service_with_backend(
        mode: SeedMode,
        backend: BroadcasterBackend,
    ) -> Result<BroadcasterServiceState> {
        let cache = BroadcasterSnapshotCache::new(1, vec![backend]);
        let upstream = BroadcasterUpstreamState::default();
        let service = BroadcasterServiceState::new(8_388_608, 8, cache, upstream);

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

    #[derive(Debug, Clone)]
    struct RpcFakeRedisWriter {
        fail_appends: bool,
        appends: Arc<Mutex<Vec<BroadcasterRedisStreamEntry>>>,
        latest_pointer: Arc<Mutex<Option<BroadcasterRedisSnapshotPointer>>>,
    }

    impl RpcFakeRedisWriter {
        fn healthy() -> Self {
            Self {
                fail_appends: false,
                appends: Arc::new(Mutex::new(Vec::new())),
                latest_pointer: Arc::new(Mutex::new(None)),
            }
        }

        fn failing_appends() -> Self {
            Self {
                fail_appends: true,
                appends: Arc::new(Mutex::new(Vec::new())),
                latest_pointer: Arc::new(Mutex::new(None)),
            }
        }
    }

    impl RedisStreamWriter for RpcFakeRedisWriter {
        fn append<'a>(
            &'a self,
            _stream_key: &'a str,
            _dedupe_key_ttl_ms: u64,
            entry: &'a BroadcasterRedisStreamEntry,
        ) -> futures::future::BoxFuture<'a, Result<String>> {
            Box::pin(async move {
                if self.fail_appends {
                    bail!("planned append failure");
                }
                let mut appends = self.appends.lock().await;
                let entry_id = format!("1000-{}", appends.len());
                appends.push(entry.clone());
                Ok(entry_id)
            })
        }

        fn set_snapshot_pointer<'a>(
            &'a self,
            _snapshot_key: &'a str,
            pointer: &'a BroadcasterRedisSnapshotPointer,
        ) -> futures::future::BoxFuture<'a, Result<()>> {
            Box::pin(async move {
                *self.latest_pointer.lock().await = Some(pointer.clone());
                Ok(())
            })
        }
    }

    fn redis_publisher_config() -> BroadcasterRedisPublisherConfig {
        BroadcasterRedisPublisherConfig::from_redis_config(
            &BroadcasterRedisConfig {
                redis_url: "redis://127.0.0.1:6379/0".to_string(),
                stream_key: "dsolver:broadcaster:test:events".to_string(),
                snapshot_key: "dsolver:broadcaster:test:snapshot".to_string(),
                block_ms: 5_000,
                read_count: 128,
                append_retry_window_ms: 1,
                retention_secs: 300,
                maxlen: None,
            },
            Chain::Ethereum.id(),
            8_388_608,
        )
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

    async fn get_status(app: Router, uri: &str) -> Result<StatusCode> {
        let response = app
            .oneshot(Request::builder().uri(uri).body(Body::empty())?)
            .await?;
        Ok(response.status())
    }

    async fn post_status(app: Router, uri: &str) -> Result<StatusCode> {
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(uri)
                    .header("content-type", "application/json")
                    .body(Body::from("{}"))?,
            )
            .await?;
        Ok(response.status())
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

    async fn spawn_server(app: Router, path: &str) -> Result<(String, JoinHandle<()>)> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let server_task = tokio::spawn(async move {
            let _ = axum::serve(listener, app.into_make_service()).await;
        });
        Ok((format!("ws://{addr}{path}"), server_task))
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
