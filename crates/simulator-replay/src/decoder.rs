use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

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
    tycho_common::{models::token::Token, Bytes},
};

use simulator_core::broadcaster::{
    BroadcasterProtocolMessage, BroadcasterSnapshotPartition, BroadcasterUpdateMessage,
};

use crate::payload::{live_partition_update, snapshot_partition_update};
use crate::{DecodedReplay, ReplayBackend};

pub type TokenMap = HashMap<Bytes, Token>;

#[derive(Debug, Clone)]
pub struct DecoderConfig {
    pub min_token_quality: u32,
    protocols: BTreeMap<ReplayBackend, Vec<String>>,
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
        }
    }

    pub fn for_backend(
        backend: ReplayBackend,
        protocols: Vec<String>,
        min_token_quality: u32,
    ) -> Self {
        Self::new(min_token_quality, [(backend, protocols)])
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
