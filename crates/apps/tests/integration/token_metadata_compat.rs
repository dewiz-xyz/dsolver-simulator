use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Result};
use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use chrono::NaiveDateTime;
use num_bigint::BigUint;
use num_traits::Zero;
use rpc::create_router;
use runtime::config::SlippageConfig;
use runtime::models::state::{
    AppState, BroadcasterSubscriptionStatus, ConfiguredBackends, StateStore, VmStreamStatus,
};
use runtime::models::stream_health::StreamHealth;
use runtime::models::tokens::TokenStore;
use runtime::simulator_service::SimulatorRuntime;
use simulator_core::broadcaster::{BroadcasterTokenDto, BroadcasterTokenLookupResponse};
use simulator_core::models::messages::{
    AmountOutRequest, EncodeErrorResponse, HopDraft, PoolRef, PoolSwapDraft, QuoteFailureKind,
    QuoteResult, QuoteResultQuality, QuoteStatus, RouteEncodeRequest, SegmentDraft, SwapKind,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;
use tower::ServiceExt;
use tycho_simulation::protocol::models::{ProtocolComponent, Update};
use tycho_simulation::tycho_common::dto::ProtocolStateDelta;
use tycho_simulation::tycho_common::models::{token::Token, Chain};
use tycho_simulation::tycho_common::simulation::errors::{SimulationError, TransitionError};
use tycho_simulation::tycho_common::simulation::protocol_sim::{
    Balances, GetAmountOutResult, ProtocolSim,
};
use tycho_simulation::tycho_common::Bytes;

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
struct EchoAmountSim;

#[typetag::serde]
impl ProtocolSim for EchoAmountSim {
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
        Ok((BigUint::from(1_000_000_u64), BigUint::zero()))
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

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }

    fn eq(&self, other: &dyn ProtocolSim) -> bool {
        other.as_any().is::<EchoAmountSim>()
    }
}

struct TokenAuthority {
    lookup_url: String,
    broadcaster_hits: Arc<AtomicUsize>,
    tycho_hits: Arc<AtomicUsize>,
    task: JoinHandle<Result<()>>,
}

impl Drop for TokenAuthority {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl TokenAuthority {
    async fn spawn(tokens: Vec<Token>) -> Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let base_url = format!("http://{}", listener.local_addr()?);
        let lookup_url = format!("{base_url}/tokens/lookup");
        let broadcaster_hits = Arc::new(AtomicUsize::new(0));
        let tycho_hits = Arc::new(AtomicUsize::new(0));
        let tokens = Arc::new(tokens);
        let broadcaster_hits_for_task = Arc::clone(&broadcaster_hits);
        let tycho_hits_for_task = Arc::clone(&tycho_hits);
        let task = tokio::spawn(async move {
            loop {
                let (stream, _) = listener.accept().await?;
                let tokens = Arc::clone(&tokens);
                let broadcaster_hits = Arc::clone(&broadcaster_hits_for_task);
                let tycho_hits = Arc::clone(&tycho_hits_for_task);
                tokio::spawn(async move {
                    let _ = handle_token_authority_request(
                        stream,
                        tokens,
                        broadcaster_hits,
                        tycho_hits,
                    )
                    .await;
                });
            }
        });

        Ok(Self {
            lookup_url,
            broadcaster_hits,
            tycho_hits,
            task,
        })
    }

    fn assert_no_tycho_fetches(&self) {
        assert_eq!(
            self.tycho_hits.load(Ordering::SeqCst),
            0,
            "simulator request paths must not use Tycho token metadata fetches"
        );
    }
}

async fn handle_token_authority_request(
    mut stream: TcpStream,
    tokens: Arc<Vec<Token>>,
    broadcaster_hits: Arc<AtomicUsize>,
    tycho_hits: Arc<AtomicUsize>,
) -> Result<()> {
    let mut buffer = vec![0_u8; 8192];
    let read = stream.read(&mut buffer).await?;
    let request = String::from_utf8_lossy(&buffer[..read]);
    let path = request
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .ok_or_else(|| anyhow!("token authority request line missing path"))?;

    let (status, body) = match path {
        "/tokens/lookup" => {
            broadcaster_hits.fetch_add(1, Ordering::SeqCst);
            let response = BroadcasterTokenLookupResponse {
                tokens: tokens
                    .iter()
                    .cloned()
                    .map(BroadcasterTokenDto::from)
                    .collect(),
                missing: Vec::new(),
            };
            ("200 OK", serde_json::to_vec(&response)?)
        }
        "/v1/tokens" => {
            tycho_hits.fetch_add(1, Ordering::SeqCst);
            (
                "500 Internal Server Error",
                b"tycho fallback forbidden".to_vec(),
            )
        }
        _ => ("404 Not Found", b"not found".to_vec()),
    };

    let headers = format!(
        "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(headers.as_bytes()).await?;
    stream.write_all(&body).await?;
    Ok(())
}

fn parse_address(value: &str) -> Result<Bytes> {
    Ok(Bytes::from_str(value)?)
}

fn make_token(address: &Bytes, symbol: &str) -> Token {
    Token::new(address, symbol, 18, 0, &[], Chain::Ethereum, 100)
}

fn make_component(id: &str, tokens: Vec<Token>) -> Result<ProtocolComponent> {
    Ok(ProtocolComponent::new(
        parse_address(id)?,
        "uniswap_v2".to_string(),
        "uniswap_v2".to_string(),
        Chain::Ethereum,
        tokens,
        Vec::new(),
        HashMap::new(),
        Bytes::default(),
        NaiveDateTime::default(),
    ))
}

async fn install_pool(
    store: &StateStore,
    pool_id: &str,
    component_address: &str,
    tokens: Vec<Token>,
) -> Result<()> {
    let update = Update::new(
        42,
        HashMap::from([(
            pool_id.to_string(),
            Box::new(EchoAmountSim) as Box<dyn ProtocolSim>,
        )]),
        HashMap::from([(
            pool_id.to_string(),
            make_component(component_address, tokens)?,
        )]),
    );
    store.apply_update(update).await;
    Ok(())
}

async fn build_app_state(token_store: Arc<TokenStore>) -> Result<AppState> {
    let native_state_store = Arc::new(StateStore::new(Arc::clone(&token_store)));
    let vm_state_store = Arc::new(StateStore::new(Arc::clone(&token_store)));
    let rfq_state_store = Arc::new(StateStore::new(Arc::clone(&token_store)));
    let ready_a = parse_address("0x00000000000000000000000000000000000000a1")?;
    let ready_b = parse_address("0x00000000000000000000000000000000000000b2")?;
    install_pool(
        &native_state_store,
        "ready-pool",
        "0x00000000000000000000000000000000000000c3",
        vec![make_token(&ready_a, "RDA"), make_token(&ready_b, "RDB")],
    )
    .await?;

    let native_stream_health = Arc::new(StreamHealth::new());
    native_stream_health.record_update(42).await;

    Ok(AppState {
        chain: Chain::Ethereum,
        native_token_protocol_allowlist: Arc::new(vec!["rocketpool".to_string()]),
        tokens: token_store,
        native_broadcaster_subscription: BroadcasterSubscriptionStatus::ready_for_test(),
        vm_broadcaster_subscription: BroadcasterSubscriptionStatus::ready_for_test(),
        rfq_broadcaster_subscription: BroadcasterSubscriptionStatus::ready_for_test(),
        native_state_store,
        vm_state_store,
        rfq_state_store,
        native_stream_health,
        vm_stream_health: Arc::new(StreamHealth::new()),
        rfq_stream_health: Arc::new(StreamHealth::new()),
        vm_stream: Arc::new(tokio::sync::RwLock::new(VmStreamStatus::default())),
        configured_backends: ConfiguredBackends {
            vm: false,
            rfq: false,
        },
        enable_vm_pools: false,
        enable_rfq_pools: false,
        readiness_stale: Duration::from_secs(120),
        request_timeout: Duration::from_secs(2),
        vm_simulation_rebuild_gate: Arc::new(tokio::sync::RwLock::new(())),
        rfq_simulation_rebuild_gate: Arc::new(tokio::sync::RwLock::new(())),
        slippage: SlippageConfig::default(),
        erc4626_deposits_enabled: false,
        erc4626_pair_policies: Arc::new(Vec::new()),
        reset_allowance_tokens: Arc::new(HashMap::<u64, HashSet<Bytes>>::new()),
    })
}

async fn post_simulate(app: axum::Router, request: &AmountOutRequest) -> Result<QuoteResult> {
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/simulate")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(request)?))?,
        )
        .await?;
    let status = response.status();
    let body = to_bytes(response.into_body(), usize::MAX).await?;
    assert_eq!(
        status,
        StatusCode::OK,
        "unexpected /simulate status {status}: {}",
        String::from_utf8_lossy(&body)
    );
    Ok(serde_json::from_slice(&body)?)
}

async fn post_encode(
    app: axum::Router,
    request: &RouteEncodeRequest,
) -> Result<(StatusCode, EncodeErrorResponse)> {
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/encode")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(request)?))?,
        )
        .await?;
    let status = response.status();
    let body = to_bytes(response.into_body(), usize::MAX).await?;
    Ok((status, serde_json::from_slice(&body)?))
}

fn encode_request(token_in: &str, token_out: &str, pool_id: &str) -> RouteEncodeRequest {
    RouteEncodeRequest {
        chain_id: Chain::Ethereum.id(),
        token_in: token_in.to_string(),
        token_out: token_out.to_string(),
        amount_in: "10".to_string(),
        min_amount_out: "8".to_string(),
        settlement_address: "0x0000000000000000000000000000000000000003".to_string(),
        tycho_router_address: "0x0000000000000000000000000000000000000004".to_string(),
        swap_kind: SwapKind::SimpleSwap,
        segments: vec![SegmentDraft {
            kind: SwapKind::SimpleSwap,
            share_bps: 0,
            hops: vec![HopDraft {
                token_in: token_in.to_string(),
                token_out: token_out.to_string(),
                swaps: vec![PoolSwapDraft {
                    pool: PoolRef {
                        protocol: "uniswap_v2".to_string(),
                        component_id: pool_id.to_string(),
                        pool_address: None,
                    },
                    token_in: token_in.to_string(),
                    token_out: token_out.to_string(),
                    split_bps: 0,
                }],
            }],
        }],
        request_id: Some("encode-missing-token".to_string()),
        estimated_amount_in: None,
    }
}

#[tokio::test]
async fn simulate_missing_token_preserves_token_coverage_semantics_without_tycho_fetch(
) -> Result<()> {
    let token_in_hex = "0x0000000000000000000000000000000000000001";
    let token_out_hex = "0x0000000000000000000000000000000000000002";
    let token_in = parse_address(token_in_hex)?;
    let authority = TokenAuthority::spawn(vec![make_token(&token_in, "TK1")]).await?;
    let token_store = Arc::new(TokenStore::broadcaster_backed(
        HashMap::new(),
        authority.lookup_url.clone(),
        Chain::Ethereum,
        Duration::from_secs(1),
    ));
    let app = create_router(SimulatorRuntime::new(
        build_app_state(Arc::clone(&token_store)).await?,
    ));

    let response = post_simulate(
        app,
        &AmountOutRequest {
            request_id: "simulate-missing-token".to_string(),
            auction_id: None,
            token_in: token_in_hex.to_string(),
            token_out: token_out_hex.to_string(),
            amounts: vec!["1".to_string()],
        },
    )
    .await?;

    assert_eq!(response.meta.status, QuoteStatus::TokenMissing);
    assert_eq!(
        response.meta.result_quality,
        QuoteResultQuality::RequestLevelFailure
    );
    assert!(
        response.meta.failures.iter().any(|failure| matches!(
            failure.kind,
            QuoteFailureKind::TokenCoverage
        ) && failure.message
            == format!(
                "Token not found: {}",
                token_out_hex.trim_start_matches("0x")
            )),
        "expected TokenCoverage failure for missing token, got {:?}",
        response.meta.failures
    );
    assert_eq!(
        authority.broadcaster_hits.load(Ordering::SeqCst),
        2,
        "both /simulate token lookups should use the broadcaster mirror"
    );
    authority.assert_no_tycho_fetches();
    assert!(
        token_store.snapshot().await.contains_key(&token_in),
        "resolved broadcaster token should be cached in the simulator mirror"
    );
    Ok(())
}

#[tokio::test]
async fn encode_missing_token_preserves_client_error_without_tycho_fetch() -> Result<()> {
    let token_in_hex = "0x0000000000000000000000000000000000000011";
    let token_out_hex = "0x0000000000000000000000000000000000000012";
    let token_in = parse_address(token_in_hex)?;
    let authority = TokenAuthority::spawn(vec![make_token(&token_in, "TK1")]).await?;
    let token_store = Arc::new(TokenStore::broadcaster_backed(
        HashMap::new(),
        authority.lookup_url.clone(),
        Chain::Ethereum,
        Duration::from_secs(1),
    ));
    let state = build_app_state(Arc::clone(&token_store)).await?;
    install_pool(
        &state.native_state_store,
        "encode-pool",
        "0x0000000000000000000000000000000000000019",
        Vec::new(),
    )
    .await?;
    let app = create_router(SimulatorRuntime::new(state));

    let (status, response) = post_encode(
        app,
        &encode_request(token_in_hex, token_out_hex, "encode-pool"),
    )
    .await?;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(response.error, "Token not found");
    assert_eq!(
        authority.broadcaster_hits.load(Ordering::SeqCst),
        2,
        "/encode token lookups should use the broadcaster mirror"
    );
    authority.assert_no_tycho_fetches();
    assert!(
        token_store.snapshot().await.contains_key(&token_in),
        "resolved broadcaster token should remain cached in the simulator mirror"
    );
    Ok(())
}
