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
    BroadcasterUpdateMessage, ProtocolHeadUpdate,
};

use simulator_core::models::protocol::ProtocolKind;

use crate::payload::{live_partition_update, snapshot_partition_update};
use crate::{DecodedReplay, RawSnapshotReassembly, ReplayBackend};

pub type TokenMap = HashMap<Bytes, Token>;

pub const RETAINED_DELTA_FORMAT_VERSION_V1: i16 = 1;
pub const RETAINED_TOKEN_QUALITY_V1: u32 = 0;

const RETAINED_NATIVE_PROTOCOLS_V1: &[ProtocolKind] = &[
    ProtocolKind::AerodromeSlipstreams,
    ProtocolKind::EkuboV2,
    ProtocolKind::EkuboV3,
    ProtocolKind::ERC4626,
    ProtocolKind::FluidV1,
    ProtocolKind::PancakeswapV2,
    ProtocolKind::PancakeswapV3,
    ProtocolKind::Rocketpool,
    ProtocolKind::SushiswapV2,
    ProtocolKind::UniswapV2,
    ProtocolKind::UniswapV3,
    ProtocolKind::UniswapV4,
];
const RETAINED_VM_PROTOCOLS_V1: &[ProtocolKind] = &[
    ProtocolKind::BalancerV2,
    ProtocolKind::Curve,
    ProtocolKind::MaverickV2,
];
const RETAINED_RFQ_PROTOCOLS_V1: &[ProtocolKind] = &[
    ProtocolKind::Bebop,
    ProtocolKind::Hashflow,
    ProtocolKind::Liquorice,
];

#[derive(Debug, Clone)]
pub struct DecoderConfig {
    min_token_quality: u32,
    protocols: BTreeMap<ReplayBackend, Vec<ProtocolKind>>,
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
        protocols: impl IntoIterator<Item = (ReplayBackend, Vec<ProtocolKind>)>,
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
        protocols: Vec<ProtocolKind>,
        min_token_quality: u32,
    ) -> Self {
        Self::new(min_token_quality, [(backend, protocols)])
    }

    pub fn retained_v1() -> Self {
        Self {
            min_token_quality: RETAINED_TOKEN_QUALITY_V1,
            protocols: [
                (ReplayBackend::Native, RETAINED_NATIVE_PROTOCOLS_V1),
                (ReplayBackend::Vm, RETAINED_VM_PROTOCOLS_V1),
                (ReplayBackend::Rfq, RETAINED_RFQ_PROTOCOLS_V1),
            ]
            .into_iter()
            .map(|(backend, protocols)| (backend, protocols.to_vec()))
            .collect(),
            profile: DecoderProfile::RetainedV1,
        }
    }

    pub const fn min_token_quality(&self) -> u32 {
        self.min_token_quality
    }

    pub fn protocols(&self, backend: ReplayBackend) -> Option<&[ProtocolKind]> {
        self.protocols.get(&backend).map(Vec::as_slice)
    }

    fn contains_protocol(&self, backend: ReplayBackend, protocol: ProtocolKind) -> bool {
        self.protocols(backend)
            .is_some_and(|protocols| protocols.contains(&protocol))
    }

    fn configured_backends(&self) -> impl Iterator<Item = ReplayBackend> + '_ {
        self.protocols.keys().copied()
    }
}

#[derive(Debug, Error)]
pub enum ReplayDecodeError {
    #[error("Unknown native protocol in chain profile: {0}")]
    UnknownNativeProtocol(ProtocolKind),
    #[error("Unknown VM protocol in chain profile: {0}")]
    UnknownVmProtocol(ProtocolKind),
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
                        register_native_decoder(&mut decoder, *protocol)?;
                    }
                }
                ReplayBackend::Vm => {
                    for protocol in protocols {
                        register_vm_decoder(&mut decoder, *protocol)?;
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
        let mut protocol_head_updates = Vec::new();
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
                    protocol_head_updates.extend(decoded.protocol_head_updates);
                    merge_update(&mut combined, decoded.update);
                    continue;
                }
                let reassembly = raw_messages.entry(backend).or_default();
                for message in partition.messages {
                    reassembly
                        .push(message)
                        .map_err(|error| ReplayDecodeError::InvalidCheckpoint(error.to_string()))?;
                }
            }
        }
        for (backend, mut reassembly) in raw_messages {
            let (update, head_updates) = self
                .decode_protocol_messages(backend, reassembly.take_messages())
                .await?;
            protocol_head_updates.extend(head_updates);
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
            protocol_head_updates,
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
        let (update, protocol_head_updates) = if partition.messages.is_empty() {
            let head_updates = if backend != ReplayBackend::Rfq && !partition.states.is_empty() {
                self.config
                    .protocols(backend)
                    .unwrap_or_default()
                    .iter()
                    .map(|protocol| ProtocolHeadUpdate {
                        protocol: *protocol,
                        head: None,
                    })
                    .collect()
            } else {
                Vec::new()
            };
            (Some(snapshot_partition_update(partition)), head_updates)
        } else {
            self.ensure_raw_messages_supported(backend)?;
            self.decode_protocol_messages(backend, partition.messages)
                .await?
        };
        let block_number = update
            .as_ref()
            .map_or(block_number, |update| update.block_number_or_timestamp);
        Ok(DecodedReplay {
            had_applicable_partition: true,
            block_number,
            complete_native_block: None,
            protocol_head_updates,
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
        self.decode_protocol_messages(backend, messages)
            .await
            .map(|(update, _)| update)
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
        let mut block_number = 0;
        let mut complete_native_block: Option<u64> = None;
        let mut had_applicable_partition = false;
        let mut protocol_head_updates = Vec::new();
        for partition in update
            .partitions
            .into_iter()
            .filter(|partition| applicable.contains(&ReplayBackend::from(partition.backend)))
        {
            let backend = ReplayBackend::from(partition.backend);
            had_applicable_partition = true;
            if let Some(complete_block) = partition.complete_native_block() {
                complete_native_block = Some(
                    complete_native_block
                        .unwrap_or_default()
                        .max(complete_block),
                );
            }
            let decoded = if partition.messages.is_empty() {
                let has_state_payload = !partition.new_pairs.is_empty()
                    || !partition.updated_states.is_empty()
                    || !partition.removed_pairs.is_empty();
                if backend != ReplayBackend::Rfq && has_state_payload {
                    protocol_head_updates.extend(
                        self.config
                            .protocols(backend)
                            .unwrap_or_default()
                            .iter()
                            .map(|protocol| ProtocolHeadUpdate {
                                protocol: *protocol,
                                head: None,
                            }),
                    );
                }
                (backend == ReplayBackend::Rfq || has_state_payload)
                    .then(|| live_partition_update(partition))
            } else {
                self.ensure_raw_messages_supported(backend)?;
                let (decoded, head_updates) = self
                    .decode_protocol_messages(backend, partition.messages)
                    .await?;
                protocol_head_updates.extend(head_updates);
                decoded
            };
            if let Some(update) = &decoded {
                block_number = block_number.max(update.block_number_or_timestamp);
            }
            merge_update(&mut combined, decoded);
        }
        if let Some(update) = combined.as_mut() {
            update.block_number_or_timestamp = block_number;
        }
        Ok(DecodedReplay {
            had_applicable_partition,
            block_number,
            complete_native_block,
            protocol_head_updates,
            update: combined,
        })
    }

    async fn decode_protocol_messages(
        &self,
        backend: ReplayBackend,
        messages: Vec<BroadcasterProtocolMessage>,
    ) -> Result<(Option<Update>, Vec<ProtocolHeadUpdate>), ReplayDecodeError> {
        let mut combined: Option<Update> = None;
        let mut head_updates = Vec::new();
        for raw in messages {
            let Some(head_update) = ProtocolHeadUpdate::from_message(&raw) else {
                continue;
            };
            if !self.config.contains_protocol(backend, head_update.protocol) {
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
            merge_update(&mut combined, Some(update));
            head_updates.push(head_update);
        }
        Ok((combined, head_updates))
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
    protocol: ProtocolKind,
) -> Result<(), ReplayDecodeError> {
    match protocol {
        ProtocolKind::UniswapV2 | ProtocolKind::SushiswapV2 => {
            decoder.register_decoder::<UniswapV2State>(protocol.as_str())
        }
        ProtocolKind::PancakeswapV2 => {
            decoder.register_decoder::<PancakeswapV2State>(protocol.as_str())
        }
        ProtocolKind::UniswapV3 | ProtocolKind::PancakeswapV3 => {
            decoder.register_decoder::<UniswapV3State>(protocol.as_str())
        }
        ProtocolKind::UniswapV4 => decoder.register_decoder::<UniswapV4State>(protocol.as_str()),
        ProtocolKind::EkuboV2 => decoder.register_decoder::<EkuboState>(protocol.as_str()),
        ProtocolKind::FluidV1 => {
            decoder.register_decoder::<FluidV1>(protocol.as_str());
            decoder.register_filter(protocol.as_str(), fluid_v1_paused_pools_filter);
        }
        ProtocolKind::Rocketpool => decoder.register_decoder::<RocketpoolState>(protocol.as_str()),
        ProtocolKind::EkuboV3 => decoder.register_decoder::<EkuboV3State>(protocol.as_str()),
        ProtocolKind::AerodromeSlipstreams => {
            decoder.register_decoder::<AerodromeSlipstreamsState>(protocol.as_str())
        }
        ProtocolKind::ERC4626 => {
            decoder.register_decoder::<ERC4626State>(protocol.as_str());
            decoder.register_filter(protocol.as_str(), erc4626_filter);
        }
        other => return Err(ReplayDecodeError::UnknownNativeProtocol(other)),
    }
    Ok(())
}

fn register_vm_decoder(
    decoder: &mut TychoStreamDecoder<BlockHeader>,
    protocol: ProtocolKind,
) -> Result<(), ReplayDecodeError> {
    match protocol {
        ProtocolKind::BalancerV2 => {
            decoder.register_decoder::<EVMPoolState<PreCachedDB>>(protocol.as_str());
            decoder.register_filter(protocol.as_str(), balancer_v2_pool_filter);
        }
        ProtocolKind::Curve | ProtocolKind::MaverickV2 => {
            decoder.register_decoder::<EVMPoolState<PreCachedDB>>(protocol.as_str());
        }
        other => return Err(ReplayDecodeError::UnknownVmProtocol(other)),
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
        let canonical_names = |backend| {
            config
                .protocols(backend)
                .unwrap_or_default()
                .iter()
                .map(ProtocolKind::as_str)
                .collect::<Vec<_>>()
        };
        assert_eq!(
            canonical_names(ReplayBackend::Native),
            [
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
            ]
        );
        assert_eq!(
            canonical_names(ReplayBackend::Vm),
            ["vm:balancer_v2", "vm:curve", "vm:maverick_v2"]
        );
        assert_eq!(
            canonical_names(ReplayBackend::Rfq),
            ["rfq:bebop", "rfq:hashflow", "rfq:liquorice"]
        );
    }

    #[tokio::test]
    async fn raw_selection_ignores_unknown_alias_and_unselected_protocols() -> anyhow::Result<()> {
        let decoder = ReplayDecoder::new(
            DecoderConfig::for_backend(ReplayBackend::Native, vec![ProtocolKind::UniswapV2], 0),
            HashMap::new(),
        )
        .await?;
        let ignored = [
            "Uniswap_V2",
            "uniswap-v2",
            "uniswap v2",
            "future_protocol",
            "uniswap_v3",
        ];
        let (update, heads) = decoder
            .decode_protocol_messages(
                ReplayBackend::Native,
                ignored.into_iter().map(raw_message).collect(),
            )
            .await?;
        assert!(update.is_none());
        assert!(heads.is_empty());

        let (update, heads) = decoder
            .decode_protocol_messages(
                ReplayBackend::Native,
                vec![raw_message("uniswap_v2"), raw_message("UNISWAP_V2")],
            )
            .await?;
        assert!(update.is_some());
        assert_eq!(heads.len(), 1);
        assert_eq!(heads[0].protocol, ProtocolKind::UniswapV2);
        assert_eq!(heads[0].head.as_ref().map(|head| head.number), Some(123));
        Ok(())
    }

    #[test]
    fn reassembly_preserves_distinct_wire_identities_before_selection() -> anyhow::Result<()> {
        let mut reassembly = RawSnapshotReassembly::default();
        for name in ["uniswap_v2", "Uniswap_V2", "future_protocol"] {
            let message = raw_message(name);
            let json = serde_json::to_value(&message)?;
            assert_eq!(json["protocol"], name);
            let roundtrip = serde_json::from_value(json)?;
            reassembly.push(roundtrip)?;
        }
        let names = reassembly
            .take_messages()
            .into_iter()
            .map(|message| message.protocol)
            .collect::<Vec<_>>();
        assert_eq!(names, ["Uniswap_V2", "future_protocol", "uniswap_v2"]);
        Ok(())
    }

    fn raw_message(protocol: &str) -> BroadcasterProtocolMessage {
        use tycho_simulation::tycho_client::feed::{
            synchronizer::{Snapshot, StateSyncMessage},
            SynchronizerState,
        };
        let header = BlockHeader {
            number: 123,
            hash: Bytes::from([1; 32]),
            parent_hash: Bytes::from([0; 32]),
            revert: false,
            timestamp: 1230,
            partial_block_index: None,
        };
        BroadcasterProtocolMessage::new(
            protocol,
            SynchronizerState::Ready(header.clone()),
            StateSyncMessage {
                header,
                snapshots: Snapshot {
                    states: HashMap::new(),
                    vm_storage: HashMap::new(),
                },
                deltas: None,
                removed_components: HashMap::new(),
            },
        )
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
