use std::any::Any;
use std::collections::HashMap;

use alloy_primitives::U256;
use anyhow::{anyhow, Result};
use num_bigint::BigUint;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use simulator_core::broadcaster::{
    BroadcasterBackend, BroadcasterSnapshotPartition, BroadcasterTokenDto, BroadcasterUpdateMessage,
};
use simulator_replay::{
    DecoderConfig, ExactPoolQuote, ReplayBackend, ReplayDecoder, ReplayQuoteError, ReplayWorld,
    StatePoint,
};
use tycho_simulation::{
    protocol::models::{ProtocolComponent, Update},
    tycho_common::{
        dto::ProtocolStateDelta,
        models::{token::Token, Chain},
        simulation::{
            errors::{SimulationError, TransitionError},
            protocol_sim::{Balances, GetAmountOutResult, ProtocolSim},
        },
        Bytes,
    },
};

const NATIVE_CHECKPOINT_V1: &str = include_str!("fixtures/native_checkpoint_v1.json");
const NATIVE_DELTA_V1: &str = include_str!("fixtures/native_delta_v1.json");
const RFQ_CHECKPOINT_V1: &str = include_str!("fixtures/rfq_checkpoint_v1.json");
const RFQ_DELTA_V1: &str = include_str!("fixtures/rfq_delta_v1.json");

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct LiveParityCheckpoint {
    chain_id: u64,
    tokens: Vec<BroadcasterTokenDto>,
    partition: BroadcasterSnapshotPartition,
    quote: LiveParityQuote,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct LiveParityDelta {
    applicable_backends: Vec<BroadcasterBackend>,
    update: BroadcasterUpdateMessage,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct LiveParityQuote {
    backend: BroadcasterBackend,
    protocol: String,
    component_id: String,
    token_in: Bytes,
    token_out: Bytes,
    amount_in: String,
}

struct FixtureObservation {
    state_digest: String,
    amount_out: U256,
    block_number: u64,
    state_count: usize,
}

#[tokio::test]
async fn extracted_native_and_rfq_paths_match_characterized_results() -> Result<()> {
    let native = run_extracted_fixture(NATIVE_CHECKPOINT_V1, NATIVE_DELTA_V1).await?;
    assert_eq!(native.block_number, 101);
    assert_eq!(native.state_count, 1);
    assert_eq!(native.amount_out, U256::from(1_660_562_u64));
    assert_eq!(
        native.state_digest,
        "2ced602b273a04c50000edb041a94f9df77862c2889cbce5ca76438adf4596e3"
    );

    let rfq = run_extracted_fixture(RFQ_CHECKPOINT_V1, RFQ_DELTA_V1).await?;
    assert_eq!(rfq.block_number, 1_710_000_005);
    assert_eq!(rfq.state_count, 1);
    assert_eq!(rfq.amount_out, U256::from(1_995_000_000_u64));
    assert_eq!(
        rfq.state_digest,
        "e3f5c9f476d43de6e4eb7840644c5beccd90e00c9076334e02a958bee6d3f201"
    );
    Ok(())
}

#[tokio::test]
async fn delta_applies_only_listed_backends() -> Result<()> {
    let checkpoint: LiveParityCheckpoint = serde_json::from_str(NATIVE_CHECKPOINT_V1)?;
    let delta: LiveParityDelta = serde_json::from_str(NATIVE_DELTA_V1)?;
    let chain = Chain::Base;
    let tokens = fixture_tokens(checkpoint.tokens, chain)?;
    let backend = ReplayBackend::from(checkpoint.quote.backend);
    let decoder = ReplayDecoder::new(
        DecoderConfig::for_backend(backend, vec![checkpoint.quote.protocol.clone()], 0),
        tokens.clone(),
    )
    .await?;
    let decoded_checkpoint = decoder
        .decode_snapshot_partition(checkpoint.partition)
        .await?;
    let mut world = ReplayWorld::new(tokens, None);
    world.restore(
        decoded_checkpoint
            .update
            .ok_or_else(|| anyhow!("checkpoint did not decode"))?,
    );

    let decoded_delta = decoder.decode_delta(delta.update, &[]).await?;
    assert!(!decoded_delta.had_applicable_partition);
    assert!(decoded_delta.update.is_none());
    assert_eq!(decoded_delta.block_number, 0);
    assert_eq!(world.pin().current_block(), 100);
    Ok(())
}

#[test]
fn apply_report_preserves_live_anomaly_shape() {
    let token_a = test_token(0x51, "TOKEN_A");
    let token_b = test_token(0x52, "TOKEN_B");
    let component = test_component("missing-state", token_a.clone(), token_b.clone());
    let mut world = ReplayWorld::new(
        HashMap::from([
            (token_a.address.clone(), token_a),
            (token_b.address.clone(), token_b),
        ]),
        None,
    );

    let report = world.apply(Update::new(
        102,
        HashMap::new(),
        HashMap::from([("missing-state".to_string(), component)]),
    ));

    assert_eq!(report.block_number, 102);
    assert_eq!(report.new_pairs_missing_state, 1);
    assert_eq!(report.total_pairs, 0);
    assert_eq!(
        report.anomaly_samples,
        vec!["new_pairs_missing_state:missing-state"]
    );
    assert!(report.has_anomalies());
}

#[test]
fn exact_pool_quote_contains_simulation_panic() {
    let token_in = test_token(0x61, "TOKEN_A");
    let token_out = test_token(0x62, "TOKEN_B");
    let component_id = "panic-pool";
    let component = test_component(component_id, token_in.clone(), token_out.clone());
    let mut world = ReplayWorld::new(HashMap::new(), None);
    let report = world.apply(Update::new(
        1,
        HashMap::from([(
            component_id.to_string(),
            Box::new(PanicSim) as Box<dyn ProtocolSim>,
        )]),
        HashMap::from([(component_id.to_string(), component)]),
    ));
    assert!(!report.has_anomalies());

    let result = world.pin().quote(&ExactPoolQuote {
        backend: ReplayBackend::Native,
        protocol: "uniswap_v2".to_string(),
        component_id: component_id.to_string(),
        token_in: token_in.address,
        token_out: token_out.address,
        amount_in: U256::from(1_u64),
    });

    assert_eq!(
        result,
        Err(ReplayQuoteError::SimulationPanicked(
            "fixture quote panic".to_string()
        ))
    );
}

#[test]
fn exact_pool_quote_rejects_reversed_directional_rfq_before_simulation() {
    let token_in = test_token(0x63, "TOKEN_A");
    let token_out = test_token(0x64, "TOKEN_B");
    let component_id = "directional-rfq";
    let component = test_component_for_protocol(
        token_in.clone(),
        token_out.clone(),
        "rfq:hashflow",
        "hashflow_pool",
    );
    let mut world = ReplayWorld::new(HashMap::new(), None);
    let report = world.apply(Update::new(
        1,
        HashMap::from([(
            component_id.to_string(),
            Box::new(PanicSim) as Box<dyn ProtocolSim>,
        )]),
        HashMap::from([(component_id.to_string(), component)]),
    ));
    assert!(!report.has_anomalies());

    let result = world.pin().quote(&ExactPoolQuote {
        backend: ReplayBackend::Rfq,
        protocol: "rfq:hashflow".to_string(),
        component_id: component_id.to_string(),
        token_in: token_out.address,
        token_out: token_in.address,
        amount_in: U256::from(1_u64),
    });

    assert_eq!(
        result,
        Err(ReplayQuoteError::DirectionUnsupported(
            component_id.to_string()
        ))
    );
}

async fn run_extracted_fixture(
    checkpoint_json: &str,
    delta_json: &str,
) -> Result<FixtureObservation> {
    let checkpoint: LiveParityCheckpoint = serde_json::from_str(checkpoint_json)?;
    let delta: LiveParityDelta = serde_json::from_str(delta_json)?;
    let chain = Chain::Base;
    if checkpoint.chain_id != chain.id() {
        return Err(anyhow!("fixture chain mismatch"));
    }
    let tokens = fixture_tokens(checkpoint.tokens, chain)?;
    let backend = ReplayBackend::from(checkpoint.quote.backend);
    let decoder = ReplayDecoder::new(
        DecoderConfig::for_backend(backend, vec![checkpoint.quote.protocol.clone()], 0),
        tokens.clone(),
    )
    .await?;
    let decoded_checkpoint = decoder
        .decode_snapshot_partition(checkpoint.partition)
        .await?;
    let mut world = ReplayWorld::new(tokens, None);
    let checkpoint_report = world.restore(
        decoded_checkpoint
            .update
            .ok_or_else(|| anyhow!("checkpoint did not decode"))?,
    );
    if checkpoint_report.has_anomalies() {
        return Err(anyhow!("checkpoint apply produced anomalies"));
    }
    let applicable_backends = delta
        .applicable_backends
        .into_iter()
        .map(ReplayBackend::from)
        .collect::<Vec<_>>();
    let decoded_delta = decoder
        .decode_delta(delta.update, &applicable_backends)
        .await?;
    let delta_report = world.apply(
        decoded_delta
            .update
            .ok_or_else(|| anyhow!("delta did not decode"))?,
    );
    if delta_report.has_anomalies() {
        return Err(anyhow!("delta apply produced anomalies"));
    }

    let point = world.pin();
    let amount_out = point.quote(&ExactPoolQuote {
        backend,
        protocol: checkpoint.quote.protocol.clone(),
        component_id: checkpoint.quote.component_id.clone(),
        token_in: checkpoint.quote.token_in.clone(),
        token_out: checkpoint.quote.token_out.clone(),
        amount_in: U256::from_str_radix(&checkpoint.quote.amount_in, 10)?,
    })?;
    let state_digest = fixture_state_digest(&point, &checkpoint.quote)?;
    Ok(FixtureObservation {
        state_digest,
        amount_out,
        block_number: point.current_block(),
        state_count: point.total_states(),
    })
}

fn fixture_tokens(tokens: Vec<BroadcasterTokenDto>, chain: Chain) -> Result<HashMap<Bytes, Token>> {
    tokens
        .into_iter()
        .map(|token| {
            let token = token.into_token(chain)?;
            Ok((token.address.clone(), token))
        })
        .collect()
}

fn fixture_state_digest(point: &StatePoint, quote: &LiveParityQuote) -> Result<String> {
    let (pool_state, component) = point
        .pool_by_id(&quote.component_id)
        .ok_or_else(|| anyhow!("fixture pool was not restored"))?;
    let token_in = point
        .token(&quote.token_in)
        .ok_or_else(|| anyhow!("fixture input token was not restored"))?;
    let token_out = point
        .token(&quote.token_out)
        .ok_or_else(|| anyhow!("fixture output token was not restored"))?;
    let canonical = serde_json::json!({
        "blockNumber": point.current_block(),
        "component": serde_json::to_value(component.as_ref())?,
        "state": serde_json::to_value(pool_state.as_ref())?,
        "tokenIn": token_in,
        "tokenOut": token_out,
        "totalStates": point.total_states(),
    });
    Ok(format!(
        "{:x}",
        Sha256::digest(serde_json::to_vec(&canonical)?)
    ))
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
struct PanicSim;

#[typetag::serde]
impl ProtocolSim for PanicSim {
    fn fee(&self) -> f64 {
        0.0
    }

    fn spot_price(&self, _base: &Token, _quote: &Token) -> Result<f64, SimulationError> {
        Ok(0.0)
    }

    #[expect(
        clippy::panic,
        reason = "the fixture verifies replay panic containment"
    )]
    fn get_amount_out(
        &self,
        _amount_in: BigUint,
        _token_in: &Token,
        _token_out: &Token,
    ) -> Result<GetAmountOutResult, SimulationError> {
        panic!("fixture quote panic")
    }

    fn get_limits(
        &self,
        _sell_token: Bytes,
        _buy_token: Bytes,
    ) -> Result<(BigUint, BigUint), SimulationError> {
        Ok((BigUint::from(0_u8), BigUint::from(0_u8)))
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
        other.as_any().is::<Self>()
    }
}

fn test_token(seed: u8, symbol: &str) -> Token {
    let address = Bytes::from([seed; 20]);
    Token::new(&address, symbol, 18, 0, &[], Chain::Base, 100)
}

fn test_component(_component_id: &str, token_in: Token, token_out: Token) -> ProtocolComponent {
    test_component_for_protocol(token_in, token_out, "uniswap_v2", "uniswap_v2_pool")
}

fn test_component_for_protocol(
    token_in: Token,
    token_out: Token,
    protocol_system: &str,
    protocol_type_name: &str,
) -> ProtocolComponent {
    ProtocolComponent::new(
        Bytes::from([0x71; 32]),
        protocol_system.to_string(),
        protocol_type_name.to_string(),
        Chain::Base,
        vec![token_in, token_out],
        Vec::new(),
        HashMap::new(),
        Bytes::from([0x72; 32]),
        chrono::NaiveDateTime::default(),
    )
}
