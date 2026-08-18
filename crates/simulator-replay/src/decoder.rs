use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use serde_json::value::RawValue;
use thiserror::Error;
use tycho_simulation::{
    evm::{
        decoder::TychoStreamDecoder,
        engine_db::tycho_db::PreCachedDB,
        protocol::{
            aerodrome_slipstreams::state::AerodromeSlipstreamsState,
            ekubo::state::EkuboState,
            ekubo_v3::state::EkuboV3State,
            erc4626::state::ERC4626State,
            filters::{balancer_v2_pool_filter, erc4626_filter, fluid_v1_paused_pools_filter},
            fluid::FluidV1,
            pancakeswap_v2::state::PancakeswapV2State,
            rocketpool::state::RocketpoolState,
            uniswap_v2::state::UniswapV2State,
            uniswap_v3::state::UniswapV3State,
            uniswap_v4::hooks::hook_handler_creator::initialize_hook_handlers,
            uniswap_v4::state::UniswapV4State,
            vm::state::EVMPoolState,
        },
    },
    protocol::models::Update,
    tycho_client::feed::{BlockHeader, FeedMessage},
    tycho_common::{
        models::{token::Token, Chain},
        Bytes,
    },
};

use simulator_core::broadcaster::{
    BroadcasterEnvelope, BroadcasterPayload, BroadcasterProtocolMessage, BroadcasterSnapshotChunk,
    BroadcasterSnapshotPartition, BroadcasterSnapshotStart, BroadcasterTokenDto,
    BroadcasterUpdateMessage,
};

use crate::payload::{live_partition_update, snapshot_partition_update};
use crate::{DecodedReplay, RawSnapshotReassembly, ReplayBackend};

pub type TokenMap = HashMap<Bytes, Token>;

pub const RETAINED_DELTA_FORMAT_VERSION_V1: i16 = 1;
pub const RETAINED_TOKEN_QUALITY_V1: u32 = 0;

const RETAINED_NATIVE_PROTOCOLS_V1: &[&str] = &[
    "aerodrome_slipstreams",
    "ekubo_v2",
    "ekubo_v3",
    "erc4626",
    "fluid_v1",
    "pancakeswap_v2",
    "pancakeswap_v3",
    "rocketpool",
    "sushiswap_v2",
    "uniswap_v2",
    "uniswap_v3",
    "uniswap_v4",
];
const RETAINED_VM_PROTOCOLS_V1: &[&str] = &["vm:balancer_v2", "vm:curve", "vm:maverick_v2"];
const RETAINED_RFQ_PROTOCOLS_V1: &[&str] = &["rfq:bebop", "rfq:hashflow", "rfq:liquorice"];

#[derive(Debug, Clone)]
pub struct DecoderConfig {
    min_token_quality: u32,
    protocols: BTreeMap<ReplayBackend, Vec<String>>,
    profile: DecoderProfile,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DecoderProfile {
    Live,
    RetainedV1,
}

impl DecoderConfig {
    pub fn new(
        min_token_quality: u32,
        protocols: impl IntoIterator<Item = (ReplayBackend, Vec<String>)>,
    ) -> Self {
        let protocols = protocols.into_iter().collect();
        Self {
            min_token_quality,
            protocols,
            profile: DecoderProfile::Live,
        }
    }

    pub fn for_backend(
        backend: ReplayBackend,
        protocols: Vec<String>,
        min_token_quality: u32,
    ) -> Self {
        Self::new(min_token_quality, [(backend, protocols)])
    }

    pub fn retained_v1() -> Self {
        Self {
            min_token_quality: RETAINED_TOKEN_QUALITY_V1,
            protocols: [
                (
                    ReplayBackend::Native,
                    RETAINED_NATIVE_PROTOCOLS_V1
                        .iter()
                        .map(|protocol| (*protocol).to_owned())
                        .collect(),
                ),
                (
                    ReplayBackend::Vm,
                    RETAINED_VM_PROTOCOLS_V1
                        .iter()
                        .map(|protocol| (*protocol).to_owned())
                        .collect(),
                ),
                (
                    ReplayBackend::Rfq,
                    RETAINED_RFQ_PROTOCOLS_V1
                        .iter()
                        .map(|protocol| (*protocol).to_owned())
                        .collect(),
                ),
            ]
            .into_iter()
            .collect(),
            profile: DecoderProfile::RetainedV1,
        }
    }

    pub const fn min_token_quality(&self) -> u32 {
        self.min_token_quality
    }

    pub fn protocols(&self, backend: ReplayBackend) -> Option<&[String]> {
        self.protocols.get(&backend).map(Vec::as_slice)
    }

    fn contains_protocol(&self, backend: ReplayBackend, protocol: &str) -> bool {
        self.protocols(backend)
            .is_some_and(|protocols| protocols.iter().any(|configured| configured == protocol))
    }

    fn configured_backends(&self) -> impl Iterator<Item = ReplayBackend> + '_ {
        self.protocols.keys().copied()
    }
}

#[derive(Debug, Error)]
pub enum ReplayDecodeError {
    #[error("Unknown native protocol in chain profile: {0}")]
    UnknownNativeProtocol(String),
    #[error("Unknown VM protocol in chain profile: {0}")]
    UnknownVmProtocol(String),
    #[error("failed to initialize Uniswap v4 hook handlers: {0}")]
    HookInitialization(String),
    #[error("backend {0} is not configured in this replay decoder")]
    BackendNotConfigured(&'static str),
    #[error("raw RFQ broadcaster messages are unsupported; expected decoded RFQ state partitions")]
    RawRfqUnsupported,
    #[error("failed to decode broadcaster raw payload: {0}")]
    PayloadDecode(String),
    #[error("unsupported delta payload format {0}")]
    UnsupportedDeltaFormat(i16),
    #[error("delta payload format {0} requires its pinned retained decoder profile")]
    RetainedDecoderProfileRequired(i16),
    #[error("stored delta payload is not a broadcaster update")]
    StoredDeltaIsNotUpdate,
    #[error("checkpoint payload sequence is invalid: {0}")]
    InvalidCheckpoint(String),
    #[error("checkpoint is missing backend {0}")]
    MissingCheckpointBackend(&'static str),
    #[error("unsupported retained token chain {0}")]
    UnsupportedTokenChain(u64),
    #[error("failed to decode retained token snapshot: {0}")]
    TokenSnapshot(String),
}

pub struct ReplayDecoder {
    config: DecoderConfig,
    decoder: Arc<TychoStreamDecoder<BlockHeader>>,
}

impl ReplayDecoder {
    pub async fn new(
        config: DecoderConfig,
        mut tokens: TokenMap,
    ) -> Result<Self, ReplayDecodeError> {
        tokens.retain(|_, token| token.quality >= config.min_token_quality);
        let mut decoder = TychoStreamDecoder::new();
        decoder.skip_state_decode_failures(true);

        for backend in config.configured_backends() {
            let protocols = config.protocols(backend).unwrap_or_default();
            match backend {
                ReplayBackend::Native => {
                    for protocol in protocols {
                        register_native_decoder(&mut decoder, protocol)?;
                    }
                }
                ReplayBackend::Vm => {
                    for protocol in protocols {
                        register_vm_decoder(&mut decoder, protocol)?;
                    }
                }
                ReplayBackend::Rfq => {}
            }
        }

        initialize_hook_handlers()
            .map_err(|error| ReplayDecodeError::HookInitialization(format!("{error:?}")))?;
        decoder.set_tokens(tokens).await;
        Ok(Self::with_decoder(config, Arc::new(decoder)))
    }

    pub fn with_decoder(
        config: DecoderConfig,
        decoder: Arc<TychoStreamDecoder<BlockHeader>>,
    ) -> Self {
        Self { config, decoder }
    }

    /// Decodes one verified checkpoint archive into a deterministic replay update.
    pub async fn decode_checkpoint_payloads(
        &self,
        payloads_json: &[String],
        selected_backends: &[ReplayBackend],
    ) -> Result<DecodedReplay, ReplayDecodeError> {
        let selected = selected_backends.iter().copied().collect::<HashSet<_>>();
        if selected.is_empty() {
            return Err(ReplayDecodeError::InvalidCheckpoint(
                "selected backends must not be empty".to_owned(),
            ));
        }
        for backend in &selected {
            self.ensure_backend_configured(*backend)?;
        }

        let (start, chunks) = parse_checkpoint_payloads(payloads_json)?;
        let advertised = start
            .backends
            .iter()
            .copied()
            .map(ReplayBackend::from)
            .collect::<HashSet<_>>();
        if let Some(missing) = selected
            .iter()
            .find(|backend| !advertised.contains(backend))
        {
            return Err(ReplayDecodeError::MissingCheckpointBackend(
                missing.as_str(),
            ));
        }

        self.decode_checkpoint_chunks(&start.snapshot_id, chunks, &selected)
            .await
    }

    async fn decode_checkpoint_chunks(
        &self,
        snapshot_id: &str,
        chunks: BTreeMap<u32, BroadcasterSnapshotChunk>,
        selected: &HashSet<ReplayBackend>,
    ) -> Result<DecodedReplay, ReplayDecodeError> {
        let mut raw_messages = BTreeMap::<ReplayBackend, RawSnapshotReassembly>::new();
        let mut seen_backends = HashSet::new();
        let mut combined = None;
        let mut block_number = 0;
        for chunk in chunks.into_values() {
            if chunk.snapshot_id != snapshot_id {
                return Err(ReplayDecodeError::InvalidCheckpoint(
                    "snapshot chunk identifier differs from snapshot start".to_owned(),
                ));
            }
            for partition in chunk.partitions {
                let backend = ReplayBackend::from(partition.backend);
                if !selected.contains(&backend) {
                    continue;
                }
                seen_backends.insert(backend);
                block_number = block_number.max(partition.block_number);
                if partition.messages.is_empty() {
                    let decoded = self.decode_snapshot_partition(partition).await?;
                    merge_update(&mut combined, decoded.update);
                } else {
                    let reassembly = raw_messages.entry(backend).or_default();
                    for message in partition.messages {
                        reassembly.push(message).map_err(|error| {
                            ReplayDecodeError::InvalidCheckpoint(error.to_string())
                        })?;
                    }
                }
            }
        }
        for (backend, mut reassembly) in raw_messages {
            let update = self
                .decode_snapshot_messages(backend, reassembly.take_messages())
                .await?;
            merge_update(&mut combined, update);
        }
        if let Some(missing) = selected
            .iter()
            .find(|backend| !seen_backends.contains(backend))
        {
            return Err(ReplayDecodeError::MissingCheckpointBackend(
                missing.as_str(),
            ));
        }
        if let Some(update) = combined.as_mut() {
            update.block_number_or_timestamp = block_number;
        }
        Ok(DecodedReplay {
            had_applicable_partition: true,
            block_number,
            complete_native_block: None,
            update: combined,
        })
    }

    pub async fn decode_snapshot_partition(
        &self,
        partition: BroadcasterSnapshotPartition,
    ) -> Result<DecodedReplay, ReplayDecodeError> {
        let backend = ReplayBackend::from(partition.backend);
        self.ensure_backend_configured(backend)?;
        let block_number = partition.block_number;
        let update = if partition.messages.is_empty() {
            Some(snapshot_partition_update(partition))
        } else {
            self.ensure_raw_messages_supported(backend)?;
            self.decode_protocol_messages(backend, partition.messages)
                .await?
        };
        Ok(DecodedReplay {
            had_applicable_partition: true,
            block_number,
            complete_native_block: None,
            update,
        })
    }

    pub async fn decode_snapshot_messages(
        &self,
        backend: ReplayBackend,
        messages: Vec<BroadcasterProtocolMessage>,
    ) -> Result<Option<Update>, ReplayDecodeError> {
        self.ensure_backend_configured(backend)?;
        if messages.is_empty() {
            return Ok(None);
        }
        self.ensure_raw_messages_supported(backend)?;
        self.decode_protocol_messages(backend, messages).await
    }

    pub async fn decode_delta(
        &self,
        payload_format_version: i16,
        payload: &RawValue,
        applicable_backends: &[ReplayBackend],
    ) -> Result<DecodedReplay, ReplayDecodeError> {
        match payload_format_version {
            RETAINED_DELTA_FORMAT_VERSION_V1 => {
                if self.config.profile != DecoderProfile::RetainedV1 {
                    return Err(ReplayDecodeError::RetainedDecoderProfileRequired(
                        payload_format_version,
                    ));
                }
                let envelope: BroadcasterEnvelope = serde_json::from_str(payload.get())
                    .map_err(|error| ReplayDecodeError::PayloadDecode(error.to_string()))?;
                let BroadcasterPayload::Update(update) = envelope.payload else {
                    return Err(ReplayDecodeError::StoredDeltaIsNotUpdate);
                };
                self.decode_delta_v1(update, applicable_backends).await
            }
            other => Err(ReplayDecodeError::UnsupportedDeltaFormat(other)),
        }
    }

    pub async fn decode_live_delta(
        &self,
        update: BroadcasterUpdateMessage,
        applicable_backends: &[ReplayBackend],
    ) -> Result<DecodedReplay, ReplayDecodeError> {
        self.decode_delta_v1(update, applicable_backends).await
    }

    async fn decode_delta_v1(
        &self,
        update: BroadcasterUpdateMessage,
        applicable_backends: &[ReplayBackend],
    ) -> Result<DecodedReplay, ReplayDecodeError> {
        let applicable = applicable_backends.iter().copied().collect::<HashSet<_>>();
        for backend in &applicable {
            self.ensure_backend_configured(*backend)?;
        }

        let mut combined: Option<Update> = None;
        let mut block_number: Option<u64> = None;
        let mut complete_native_block: Option<u64> = None;
        let mut had_applicable_partition = false;
        for partition in update
            .partitions
            .into_iter()
            .filter(|partition| applicable.contains(&ReplayBackend::from(partition.backend)))
        {
            had_applicable_partition = true;
            let backend = ReplayBackend::from(partition.backend);
            if let Some(complete_block) = partition.complete_native_block() {
                complete_native_block = Some(
                    complete_native_block
                        .unwrap_or_default()
                        .max(complete_block),
                );
            }
            block_number = Some(block_number.unwrap_or_default().max(partition.block_number));
            let decoded = if partition.messages.is_empty() {
                Some(live_partition_update(partition))
            } else {
                self.ensure_raw_messages_supported(backend)?;
                self.decode_protocol_messages(backend, partition.messages)
                    .await?
            };
            if let Some(decoded) = decoded {
                combined = Some(match combined {
                    Some(current) => current.merge(decoded),
                    None => decoded,
                });
            }
        }

        let block_number = block_number.unwrap_or_default();
        if let Some(update) = combined.as_mut() {
            update.block_number_or_timestamp = block_number;
        }
        Ok(DecodedReplay {
            had_applicable_partition,
            block_number,
            complete_native_block,
            update: combined,
        })
    }

    async fn decode_protocol_messages(
        &self,
        backend: ReplayBackend,
        messages: Vec<BroadcasterProtocolMessage>,
    ) -> Result<Option<Update>, ReplayDecodeError> {
        let mut combined: Option<Update> = None;
        for raw in messages {
            if !self.config.contains_protocol(backend, &raw.protocol) {
                continue;
            }
            let mut state_msgs = HashMap::new();
            state_msgs.insert(raw.protocol.clone(), raw.message);
            let mut sync_states = HashMap::new();
            sync_states.insert(raw.protocol, raw.sync_state);
            let feed = FeedMessage {
                state_msgs,
                sync_states,
            };
            let update = self
                .decoder
                .decode(&feed)
                .await
                .map_err(|error| ReplayDecodeError::PayloadDecode(error.to_string()))?;
            combined = Some(match combined {
                Some(current) => current.merge(update),
                None => update,
            });
        }
        Ok(combined)
    }

    fn ensure_backend_configured(&self, backend: ReplayBackend) -> Result<(), ReplayDecodeError> {
        if self.config.protocols.contains_key(&backend) {
            Ok(())
        } else {
            Err(ReplayDecodeError::BackendNotConfigured(backend.as_str()))
        }
    }

    fn ensure_raw_messages_supported(
        &self,
        backend: ReplayBackend,
    ) -> Result<(), ReplayDecodeError> {
        if backend == ReplayBackend::Rfq {
            Err(ReplayDecodeError::RawRfqUnsupported)
        } else {
            Ok(())
        }
    }
}

fn parse_checkpoint_payloads(
    payloads_json: &[String],
) -> Result<
    (
        BroadcasterSnapshotStart,
        BTreeMap<u32, BroadcasterSnapshotChunk>,
    ),
    ReplayDecodeError,
> {
    let mut start = None;
    let mut chunks = BTreeMap::new();
    let mut end = None;
    for payload_json in payloads_json {
        let payload: BroadcasterPayload = serde_json::from_str(payload_json)
            .map_err(|error| ReplayDecodeError::PayloadDecode(error.to_string()))?;
        match payload {
            BroadcasterPayload::SnapshotStart(value) if start.is_none() => start = Some(value),
            BroadcasterPayload::SnapshotChunk(value) => {
                if chunks.insert(value.chunk_index, value).is_some() {
                    return Err(ReplayDecodeError::InvalidCheckpoint(
                        "duplicate snapshot chunk index".to_owned(),
                    ));
                }
            }
            BroadcasterPayload::SnapshotEnd(value) if end.is_none() => end = Some(value),
            _ => {
                return Err(ReplayDecodeError::InvalidCheckpoint(
                    "archive must contain one snapshot start, chunks, and one snapshot end"
                        .to_owned(),
                ));
            }
        }
    }
    let start = start
        .ok_or_else(|| ReplayDecodeError::InvalidCheckpoint("missing snapshot start".to_owned()))?;
    let end =
        end.ok_or_else(|| ReplayDecodeError::InvalidCheckpoint("missing snapshot end".to_owned()))?;
    if start.snapshot_id != end.snapshot_id {
        return Err(ReplayDecodeError::InvalidCheckpoint(
            "snapshot start and end identifiers differ".to_owned(),
        ));
    }
    if usize::try_from(start.total_chunks).ok() != Some(chunks.len())
        || chunks.keys().copied().ne(0..start.total_chunks)
    {
        return Err(ReplayDecodeError::InvalidCheckpoint(
            "snapshot chunks are incomplete or out of range".to_owned(),
        ));
    }
    Ok((start, chunks))
}

/// Decodes the canonical retained token array for a supported chain.
pub fn decode_token_map(
    chain_id: u64,
    tokens: &serde_json::value::RawValue,
) -> Result<TokenMap, ReplayDecodeError> {
    let chain = [
        Chain::Ethereum,
        Chain::Starknet,
        Chain::ZkSync,
        Chain::Arbitrum,
        Chain::Base,
        Chain::Bsc,
        Chain::Unichain,
        Chain::Polygon,
        Chain::Plasma,
    ]
    .into_iter()
    .find(|chain| chain.id() == chain_id)
    .ok_or(ReplayDecodeError::UnsupportedTokenChain(chain_id))?;
    let decoded: Vec<BroadcasterTokenDto> = serde_json::from_str(tokens.get())
        .map_err(|error| ReplayDecodeError::TokenSnapshot(error.to_string()))?;
    let mut result = HashMap::with_capacity(decoded.len());
    for token in decoded {
        let token = token
            .into_token(chain)
            .map_err(|error| ReplayDecodeError::TokenSnapshot(error.to_string()))?;
        if result.insert(token.address.clone(), token).is_some() {
            return Err(ReplayDecodeError::TokenSnapshot(
                "retained token snapshot contains a duplicate address".to_owned(),
            ));
        }
    }
    Ok(result)
}

fn merge_update(current: &mut Option<Update>, incoming: Option<Update>) {
    let Some(incoming) = incoming else {
        return;
    };
    *current = Some(match current.take() {
        Some(existing) => existing.merge(incoming),
        None => incoming,
    });
}

fn register_native_decoder(
    decoder: &mut TychoStreamDecoder<BlockHeader>,
    protocol: &str,
) -> Result<(), ReplayDecodeError> {
    match protocol {
        "uniswap_v2" | "sushiswap_v2" => decoder.register_decoder::<UniswapV2State>(protocol),
        "pancakeswap_v2" => decoder.register_decoder::<PancakeswapV2State>(protocol),
        "uniswap_v3" | "pancakeswap_v3" => decoder.register_decoder::<UniswapV3State>(protocol),
        "uniswap_v4" => decoder.register_decoder::<UniswapV4State>(protocol),
        "ekubo_v2" => decoder.register_decoder::<EkuboState>(protocol),
        "fluid_v1" => {
            decoder.register_decoder::<FluidV1>(protocol);
            decoder.register_filter(protocol, fluid_v1_paused_pools_filter);
        }
        "rocketpool" => decoder.register_decoder::<RocketpoolState>(protocol),
        "ekubo_v3" => decoder.register_decoder::<EkuboV3State>(protocol),
        "aerodrome_slipstreams" => decoder.register_decoder::<AerodromeSlipstreamsState>(protocol),
        "erc4626" => {
            decoder.register_decoder::<ERC4626State>(protocol);
            decoder.register_filter(protocol, erc4626_filter);
        }
        other => return Err(ReplayDecodeError::UnknownNativeProtocol(other.to_string())),
    }
    Ok(())
}

fn register_vm_decoder(
    decoder: &mut TychoStreamDecoder<BlockHeader>,
    protocol: &str,
) -> Result<(), ReplayDecodeError> {
    match protocol {
        "vm:balancer_v2" => {
            decoder.register_decoder::<EVMPoolState<PreCachedDB>>(protocol);
            decoder.register_filter(protocol, balancer_v2_pool_filter);
        }
        "vm:curve" | "vm:maverick_v2" => {
            decoder.register_decoder::<EVMPoolState<PreCachedDB>>(protocol);
        }
        other => return Err(ReplayDecodeError::UnknownVmProtocol(other.to_string())),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retained_v1_profile_is_pinned() {
        let config = DecoderConfig::retained_v1();

        assert_eq!(config.min_token_quality(), RETAINED_TOKEN_QUALITY_V1);
        assert_eq!(
            config.protocols(ReplayBackend::Native),
            Some(
                RETAINED_NATIVE_PROTOCOLS_V1
                    .iter()
                    .map(|protocol| (*protocol).to_owned())
                    .collect::<Vec<_>>()
                    .as_slice()
            )
        );
        assert_eq!(
            config.protocols(ReplayBackend::Vm),
            Some(
                RETAINED_VM_PROTOCOLS_V1
                    .iter()
                    .map(|protocol| (*protocol).to_owned())
                    .collect::<Vec<_>>()
                    .as_slice()
            )
        );
        assert_eq!(
            config.protocols(ReplayBackend::Rfq),
            Some(
                RETAINED_RFQ_PROTOCOLS_V1
                    .iter()
                    .map(|protocol| (*protocol).to_owned())
                    .collect::<Vec<_>>()
                    .as_slice()
            )
        );
    }

    #[tokio::test]
    async fn retained_delta_version_one_decodes_the_stored_envelope() -> anyhow::Result<()> {
        let payload = RawValue::from_string(
            include_str!("../../simulator-core/tests/fixtures/wire/native_update.json").to_owned(),
        )?;
        let decoder = test_decoder();

        let decoded = decoder
            .decode_delta(RETAINED_DELTA_FORMAT_VERSION_V1, &payload, &[])
            .await?;

        assert!(!decoded.had_applicable_partition);
        assert_eq!(decoded.block_number, 0);
        Ok(())
    }

    #[tokio::test]
    async fn unknown_retained_delta_version_fails_closed() -> anyhow::Result<()> {
        let payload = RawValue::from_string("{}".to_owned())?;
        let Err(error) = test_decoder().decode_delta(99, &payload, &[]).await else {
            anyhow::bail!("unknown retained delta formats must fail closed");
        };

        assert!(matches!(
            error,
            ReplayDecodeError::UnsupportedDeltaFormat(99)
        ));
        Ok(())
    }

    #[tokio::test]
    async fn retained_delta_version_one_rejects_a_live_decoder_profile() -> anyhow::Result<()> {
        let payload = RawValue::from_string(
            include_str!("../../simulator-core/tests/fixtures/wire/native_update.json").to_owned(),
        )?;
        let decoder = ReplayDecoder::with_decoder(
            DecoderConfig::for_backend(ReplayBackend::Native, Vec::new(), 100),
            Arc::new(TychoStreamDecoder::<BlockHeader>::new()),
        );

        let Err(error) = decoder
            .decode_delta(RETAINED_DELTA_FORMAT_VERSION_V1, &payload, &[])
            .await
        else {
            anyhow::bail!("format 1 must reject a live decoder profile");
        };

        assert!(matches!(
            error,
            ReplayDecodeError::RetainedDecoderProfileRequired(RETAINED_DELTA_FORMAT_VERSION_V1)
        ));
        Ok(())
    }

    fn test_decoder() -> ReplayDecoder {
        ReplayDecoder::with_decoder(
            DecoderConfig::retained_v1(),
            Arc::new(TychoStreamDecoder::<BlockHeader>::new()),
        )
    }
}
