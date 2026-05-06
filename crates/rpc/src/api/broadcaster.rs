use axum::{
    routing::{get, post},
    Router,
};

use runtime::broadcaster_service::BroadcasterAppState;

use crate::handlers::broadcaster::{status, token_lookup, token_snapshot, ws};

pub fn create_broadcaster_router(app_state: BroadcasterAppState) -> Router {
    Router::new()
        .route("/status", get(status))
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

    use anyhow::{bail, Result};
    use axum::{
        body::{to_bytes, Body},
        http::{Request, StatusCode},
        routing::post,
        Json, Router,
    };
    use num_bigint::BigUint;
    use num_traits::Zero;
    use runtime::{
        broadcaster_service::BroadcasterAppState,
        models::broadcaster::{BroadcasterSnapshotCache, BroadcasterUpstreamState},
        models::tokens::TokenStore,
        services::broadcaster::BroadcasterServiceState,
    };
    use simulator_core::broadcaster::BroadcasterBackend;
    use tokio::{sync::Barrier, task::JoinHandle, time::sleep};
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
    async fn websocket_upgrade_is_rejected_until_ready() -> Result<()> {
        let (url, server_task) = spawn_server(
            create_broadcaster_router(build_state(SeedMode::WarmingUp).await?),
            "/ws",
        )
        .await?;
        let result = connect_async(url).await;
        server_task.abort();

        match result {
            Err(tungstenite::Error::Http(response)) => {
                assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
            }
            Err(error) => bail!("unexpected websocket error: {error}"),
            Ok(_) => bail!("expected websocket handshake rejection"),
        }
        Ok(())
    }

    #[tokio::test]
    async fn websocket_upgrade_is_admitted_once_ready() -> Result<()> {
        let (url, server_task) = spawn_server(
            create_broadcaster_router(build_state(SeedMode::Ready).await?),
            "/ws",
        )
        .await?;
        let result = connect_async(url).await;
        server_task.abort();

        let (_stream, response) = match result {
            Ok(result) => result,
            Err(error) => bail!("expected websocket handshake success: {error}"),
        };
        assert_eq!(response.status(), StatusCode::SWITCHING_PROTOCOLS);
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

    async fn build_state_with_tokens(
        mode: SeedMode,
        tokens: Arc<TokenStore>,
        chain: Chain,
    ) -> Result<BroadcasterAppState> {
        let cache = BroadcasterSnapshotCache::new(1, vec![BroadcasterBackend::Native]);
        let upstream = BroadcasterUpstreamState::default();
        let service = BroadcasterServiceState::new(2, 8, cache, upstream);

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
