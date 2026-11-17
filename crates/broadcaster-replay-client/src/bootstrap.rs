use std::collections::BTreeMap;

use futures::StreamExt;
use simulator_core::broadcaster::{
    BroadcasterBackend, BroadcasterEnvelope, BroadcasterPayload, BroadcasterProtocolMessage,
    BroadcasterProtocolSyncStatus, BroadcasterSnapshotPartition,
    BroadcasterSnapshotSessionResponse, BroadcasterSnapshotStart, BroadcasterStateEntry,
    BroadcasterSubscriptionTracker, RawSnapshotReassembly,
};

use crate::checkpoint::ReplayCheckpoint;
use crate::client::BroadcasterReplayClient;
use crate::error::{BroadcasterReplayClientError, Result};

/// One backend's part of an assembled snapshot.
#[derive(Debug, Default)]
pub struct SnapshotBackend {
    /// The highest block any of the backend's partitions named. The messages keep their own
    /// headers, and the snapshot's exact position is the session's replay boundary.
    pub highest_block_number: u64,
    /// The raw protocol messages, each merged whole across the chunks it was split over.
    pub messages: Vec<BroadcasterProtocolMessage>,
    /// The sync status of every protocol the partitions named.
    pub sync_statuses: BTreeMap<String, BroadcasterProtocolSyncStatus>,
    /// State entries the wire sent in decoded form, kept as received.
    pub states: Vec<BroadcasterStateEntry>,
}

/// A snapshot session validated and assembled whole, with the checkpoint its Redis
/// continuation starts from.
///
/// Only a successful [`BroadcasterReplayClient::bootstrap`] creates one, so its checkpoint
/// always belongs to its snapshot.
#[derive(Debug)]
pub struct SnapshotBootstrap {
    session: BroadcasterSnapshotSessionResponse,
    checkpoint: ReplayCheckpoint,
    backends: BTreeMap<BroadcasterBackend, SnapshotBackend>,
}

impl SnapshotBootstrap {
    /// The snapshot session the bootstrap was assembled from.
    pub fn session(&self) -> &BroadcasterSnapshotSessionResponse {
        &self.session
    }

    /// Where the Redis continuation of this snapshot starts.
    pub fn checkpoint(&self) -> &ReplayCheckpoint {
        &self.checkpoint
    }

    /// Each backend's assembled part of the snapshot.
    pub fn backends(&self) -> &BTreeMap<BroadcasterBackend, SnapshotBackend> {
        &self.backends
    }

    /// Splits the bootstrap into its session, checkpoint and backends, in that order.
    pub fn into_parts(
        self,
    ) -> (
        BroadcasterSnapshotSessionResponse,
        ReplayCheckpoint,
        BTreeMap<BroadcasterBackend, SnapshotBackend>,
    ) {
        (self.session, self.checkpoint, self.backends)
    }
}

impl BroadcasterReplayClient {
    /// Open a snapshot session and assemble it whole, checked by the protocol's subscription
    /// tracker and reassembly.
    ///
    /// # Errors
    ///
    /// Returns an error when the session cannot be fetched, or the snapshot is for another
    /// chain, out of order, incomplete, or does not merge.
    pub async fn bootstrap(&self, expected_chain_id: u64) -> Result<SnapshotBootstrap> {
        let session = self.create_snapshot_session().await?;
        let mut assembly = Assembly::new(&session, expected_chain_id)?;
        {
            let mut payloads = self.snapshot_payloads(&session);
            while let Some(envelope) = payloads.next().await {
                assembly.push(envelope?)?;
            }
        }
        assembly.finish(session)
    }
}

struct Assembly {
    expected_chain_id: u64,
    snapshot_chunk_count: u32,
    tracker: BroadcasterSubscriptionTracker,
    backends: BTreeMap<BroadcasterBackend, BackendAssembly>,
}

/// A backend's snapshot while it is being assembled.
#[derive(Default)]
struct BackendAssembly {
    snapshot: SnapshotBackend,
    reassembly: RawSnapshotReassembly,
}

impl Assembly {
    /// An assembly for `session`, refused when the session names another chain.
    fn new(session: &BroadcasterSnapshotSessionResponse, expected_chain_id: u64) -> Result<Self> {
        if session.chain_id != expected_chain_id {
            return Err(BroadcasterReplayClientError::snapshot(format!(
                "session {} carries chain {}, expected {expected_chain_id}",
                session.session_id, session.chain_id
            )));
        }
        Ok(Self {
            expected_chain_id,
            snapshot_chunk_count: session.snapshot_chunk_count,
            tracker: BroadcasterSubscriptionTracker::new(),
            backends: BTreeMap::new(),
        })
    }

    fn push(&mut self, envelope: BroadcasterEnvelope) -> Result<()> {
        if let BroadcasterPayload::SnapshotStart(start) = &envelope.payload {
            self.ensure_start(start)?;
        }
        self.tracker
            .observe(&envelope)
            .map_err(|error| BroadcasterReplayClientError::snapshot(error.to_string()))?;
        match envelope.payload {
            BroadcasterPayload::SnapshotChunk(chunk) => {
                for partition in chunk.partitions {
                    self.absorb(partition)?;
                }
                Ok(())
            }
            BroadcasterPayload::SnapshotStart(_) | BroadcasterPayload::SnapshotEnd(_) => Ok(()),
            other => Err(BroadcasterReplayClientError::snapshot(format!(
                "a {} payload inside a snapshot session",
                other.kind()
            ))),
        }
    }

    /// The snapshot must declare the chain and the chunk count its session announced.
    fn ensure_start(&self, start: &BroadcasterSnapshotStart) -> Result<()> {
        if start.chain_id != self.expected_chain_id {
            return Err(BroadcasterReplayClientError::snapshot(format!(
                "snapshot {} carries chain {}, expected {}",
                start.snapshot_id, start.chain_id, self.expected_chain_id
            )));
        }
        if start.total_chunks != self.snapshot_chunk_count {
            return Err(BroadcasterReplayClientError::snapshot(format!(
                "snapshot {} declares {} chunks, its session announced {}",
                start.snapshot_id, start.total_chunks, self.snapshot_chunk_count
            )));
        }
        Ok(())
    }

    fn absorb(&mut self, partition: BroadcasterSnapshotPartition) -> Result<()> {
        let backend = self.backends.entry(partition.backend).or_default();
        let snapshot = &mut backend.snapshot;
        snapshot.highest_block_number = snapshot.highest_block_number.max(partition.block_number);
        snapshot.sync_statuses.extend(partition.sync_statuses);
        snapshot.states.extend(partition.states);
        for message in partition.messages {
            backend
                .reassembly
                .push(message)
                .map_err(|error| BroadcasterReplayClientError::snapshot(error.to_string()))?;
        }
        Ok(())
    }

    fn finish(mut self, session: BroadcasterSnapshotSessionResponse) -> Result<SnapshotBootstrap> {
        self.tracker
            .align_live_replay_boundary(&session.redis_replay_boundary)
            .map_err(|error| BroadcasterReplayClientError::snapshot(error.to_string()))?;
        let backends = self
            .backends
            .into_iter()
            .map(|(backend, mut assembly)| {
                assembly.snapshot.messages = assembly.reassembly.take_messages();
                (backend, assembly.snapshot)
            })
            .collect();
        let checkpoint = ReplayCheckpoint::new(
            session.redis_replay_boundary.clone(),
            self.expected_chain_id,
        );
        Ok(SnapshotBootstrap {
            session,
            checkpoint,
            backends,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap};

    use anyhow::{anyhow, Result};
    use simulator_core::broadcaster::{
        BroadcasterBackend, BroadcasterEnvelope, BroadcasterPayload, BroadcasterProtocolMessage,
        BroadcasterProtocolSyncStatus, BroadcasterRedisReplayBoundary, BroadcasterSnapshotChunk,
        BroadcasterSnapshotEnd, BroadcasterSnapshotPartition, BroadcasterSnapshotSessionResponse,
        BroadcasterSnapshotStart, BroadcasterStateEntry,
    };
    use tycho_simulation::tycho_client::feed::{dto, BlockHeader, SynchronizerState};
    use tycho_simulation::tycho_common::dto as common;

    use super::Assembly;

    const STREAM: &str = "stream-1";
    const SNAPSHOT: &str = "snapshot-1";
    const CHAIN: u64 = 8453;

    fn fragment(protocol: &str, ids: &[&str]) -> BroadcasterProtocolMessage {
        let header = BlockHeader {
            number: 100,
            ..Default::default()
        };
        let states = ids
            .iter()
            .map(|id| {
                let state = dto::ComponentWithState {
                    state: common::ResponseProtocolState {
                        component_id: (*id).into(),
                        ..Default::default()
                    },
                    component: common::ProtocolComponent {
                        id: (*id).into(),
                        ..Default::default()
                    },
                    component_tvl: None,
                    entrypoints: Vec::new(),
                };
                ((*id).to_string(), state)
            })
            .collect();
        let message = dto::StateSyncMessage {
            header: header.clone(),
            snapshots: dto::Snapshot {
                states,
                vm_storage: HashMap::new(),
            },
            deltas: None,
            removed_components: HashMap::new(),
        };
        BroadcasterProtocolMessage::new(protocol, SynchronizerState::Ready(header), message.into())
    }

    fn chunk(
        message_seq: u64,
        index: u32,
        messages: Vec<BroadcasterProtocolMessage>,
    ) -> Result<BroadcasterEnvelope> {
        let partition = BroadcasterSnapshotPartition::with_messages(
            BroadcasterBackend::Native,
            100,
            messages,
            BTreeMap::new(),
        );
        chunk_of(message_seq, index, vec![partition])
    }

    fn chunk_of(
        message_seq: u64,
        index: u32,
        partitions: Vec<BroadcasterSnapshotPartition>,
    ) -> Result<BroadcasterEnvelope> {
        Ok(BroadcasterEnvelope::new(
            STREAM,
            message_seq,
            BroadcasterPayload::SnapshotChunk(BroadcasterSnapshotChunk::new(
                SNAPSHOT, index, partitions,
            )?),
        ))
    }

    fn start(chunks: u32) -> Result<BroadcasterEnvelope> {
        start_of(CHAIN, chunks, vec![BroadcasterBackend::Native])
    }

    fn start_on(chain_id: u64, chunks: u32) -> Result<BroadcasterEnvelope> {
        start_of(chain_id, chunks, vec![BroadcasterBackend::Native])
    }

    fn start_of(
        chain_id: u64,
        chunks: u32,
        backends: Vec<BroadcasterBackend>,
    ) -> Result<BroadcasterEnvelope> {
        Ok(BroadcasterEnvelope::new(
            STREAM,
            1,
            BroadcasterPayload::SnapshotStart(BroadcasterSnapshotStart::new(
                SNAPSHOT, chain_id, backends, chunks,
            )?),
        ))
    }

    fn ready(block: u64) -> BroadcasterProtocolSyncStatus {
        BroadcasterProtocolSyncStatus::from_synchronizer_state(&SynchronizerState::Ready(
            BlockHeader {
                number: block,
                ..Default::default()
            },
        ))
    }

    /// The decoded RFQ states of the checked-in wire fixture.
    fn rfq_states() -> Result<Vec<BroadcasterStateEntry>> {
        let fixture = std::fs::read(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../simulator-core/tests/fixtures/wire/rfq_update.json"
        ))?;
        let envelope: BroadcasterEnvelope = serde_json::from_slice(&fixture)?;
        let BroadcasterPayload::Update(update) = envelope.payload else {
            return Err(anyhow!("the RFQ fixture is an update"));
        };
        Ok(update
            .partitions
            .into_iter()
            .flat_map(|partition| partition.new_pairs)
            .collect())
    }

    fn end(message_seq: u64) -> BroadcasterEnvelope {
        BroadcasterEnvelope::new(
            STREAM,
            message_seq,
            BroadcasterPayload::SnapshotEnd(BroadcasterSnapshotEnd::new(SNAPSHOT)),
        )
    }

    fn session(chunks: u32) -> Result<BroadcasterSnapshotSessionResponse> {
        Ok(session_bounded(chunks, boundary(STREAM, SNAPSHOT)?))
    }

    fn boundary(stream_id: &str, snapshot_id: &str) -> Result<BroadcasterRedisReplayBoundary> {
        Ok(BroadcasterRedisReplayBoundary::new(
            "events",
            stream_id,
            snapshot_id,
            7,
            10,
            10,
        )?)
    }

    fn session_bounded(
        chunks: u32,
        redis_replay_boundary: BroadcasterRedisReplayBoundary,
    ) -> BroadcasterSnapshotSessionResponse {
        BroadcasterSnapshotSessionResponse {
            chain_id: CHAIN,
            session_id: 4,
            stream_id: STREAM.to_string(),
            snapshot_id: SNAPSHOT.to_string(),
            redis_replay_boundary,
            payload_count: chunks.saturating_add(2),
            snapshot_chunk_count: chunks,
            expires_in_ms: 60_000,
        }
    }

    #[test]
    fn a_protocol_split_across_chunks_is_assembled_whole_at_the_boundary() -> Result<()> {
        let mut assembly = Assembly::new(&session(2)?, CHAIN)?;
        assembly.push(start(2)?)?;
        assembly.push(chunk(2, 0, vec![fragment("uniswap_v3", &["pool-a"])])?)?;
        assembly.push(chunk(
            3,
            1,
            vec![
                fragment("uniswap_v3", &["pool-b"]),
                fragment("uniswap_v2", &["pair-x"]),
            ],
        )?)?;
        assembly.push(end(4))?;
        let bootstrap = assembly.finish(session(2)?)?;

        assert_eq!(
            bootstrap.checkpoint().boundary(),
            &boundary(STREAM, SNAPSHOT)?
        );
        let native = bootstrap
            .backends()
            .get(&BroadcasterBackend::Native)
            .ok_or_else(|| anyhow!("a native backend"))?;
        assert_eq!(native.highest_block_number, 100);
        assert_eq!(native.messages.len(), 2, "one message per protocol");
        let v3 = native
            .messages
            .iter()
            .find(|message| message.protocol == "uniswap_v3")
            .ok_or_else(|| anyhow!("the merged uniswap_v3 message"))?;
        let mut ids: Vec<_> = v3.message.snapshots.states.keys().cloned().collect();
        ids.sort();
        assert_eq!(ids, ["pool-a", "pool-b"]);
        let v2 = native
            .messages
            .iter()
            .find(|message| message.protocol == "uniswap_v2")
            .ok_or_else(|| anyhow!("the uniswap_v2 message"))?;
        assert_eq!(
            v2.message.snapshots.states.keys().collect::<Vec<_>>(),
            ["pair-x"]
        );
        Ok(())
    }

    #[test]
    fn a_chunk_out_of_order_or_a_session_without_its_end_is_refused() -> Result<()> {
        let mut out_of_order = Assembly::new(&session(2)?, CHAIN)?;
        out_of_order.push(start(2)?)?;
        assert!(out_of_order
            .push(chunk(2, 1, vec![fragment("uniswap_v3", &["pool-a"])])?)
            .is_err());

        let mut unfinished = Assembly::new(&session(1)?, CHAIN)?;
        unfinished.push(start(1)?)?;
        unfinished.push(chunk(2, 0, vec![fragment("uniswap_v3", &["pool-a"])])?)?;
        assert!(unfinished.finish(session(1)?).is_err());
        Ok(())
    }

    #[test]
    fn fragments_naming_one_component_twice_are_refused() -> Result<()> {
        let mut assembly = Assembly::new(&session(2)?, CHAIN)?;
        assembly.push(start(2)?)?;
        assembly.push(chunk(2, 0, vec![fragment("uniswap_v3", &["pool-a"])])?)?;
        assert!(assembly
            .push(chunk(3, 1, vec![fragment("uniswap_v3", &["pool-a"])])?)
            .is_err());
        Ok(())
    }

    #[test]
    fn a_session_or_snapshot_that_disagrees_with_the_announcement_is_refused() -> Result<()> {
        const ETHEREUM: u64 = 1;
        assert!(
            Assembly::new(&session(1)?, ETHEREUM).is_err(),
            "a session of another chain"
        );

        let mut other_chain = Assembly::new(&session(1)?, CHAIN)?;
        assert!(
            other_chain.push(start_on(ETHEREUM, 1)?).is_err(),
            "a snapshot declaring another chain than its session"
        );

        let mut other_count = Assembly::new(&session(2)?, CHAIN)?;
        assert!(
            other_count.push(start(3)?).is_err(),
            "a snapshot declaring another chunk count than its session"
        );
        Ok(())
    }

    #[test]
    fn every_backend_keeps_what_its_partitions_carried() -> Result<()> {
        let session = session(2)?;
        let mut assembly = Assembly::new(&session, CHAIN)?;
        assembly.push(start_of(
            CHAIN,
            2,
            vec![BroadcasterBackend::Native, BroadcasterBackend::Rfq],
        )?)?;
        let states = rfq_states()?;
        assembly.push(chunk_of(
            2,
            0,
            vec![BroadcasterSnapshotPartition::with_messages(
                BroadcasterBackend::Native,
                101,
                vec![fragment("uniswap_v3", &["pool-a"])],
                BTreeMap::from([("uniswap_v3".to_string(), ready(101))]),
            )],
        )?)?;
        assembly.push(chunk_of(
            3,
            1,
            vec![
                BroadcasterSnapshotPartition::with_messages(
                    BroadcasterBackend::Native,
                    100,
                    vec![fragment("uniswap_v2", &["pair-x"])],
                    BTreeMap::from([("uniswap_v2".to_string(), ready(100))]),
                ),
                BroadcasterSnapshotPartition::new(
                    BroadcasterBackend::Rfq,
                    1_710_000_000,
                    states.clone(),
                    BTreeMap::new(),
                ),
            ],
        )?)?;
        assembly.push(end(4))?;
        let (_, _, backends) = assembly.finish(session)?.into_parts();

        let native = backends
            .get(&BroadcasterBackend::Native)
            .ok_or_else(|| anyhow!("a native backend"))?;
        assert_eq!(
            native.highest_block_number, 101,
            "the highest block, not the last one"
        );
        assert_eq!(
            native.sync_statuses,
            BTreeMap::from([
                ("uniswap_v2".to_string(), ready(100)),
                ("uniswap_v3".to_string(), ready(101)),
            ])
        );
        let mut protocols: Vec<_> = native
            .messages
            .iter()
            .map(|message| message.protocol.as_str())
            .collect();
        protocols.sort_unstable();
        assert_eq!(protocols, ["uniswap_v2", "uniswap_v3"]);
        assert!(native.states.is_empty());

        let rfq = backends
            .get(&BroadcasterBackend::Rfq)
            .ok_or_else(|| anyhow!("an RFQ backend"))?;
        assert_eq!(rfq.highest_block_number, 1_710_000_000);
        assert!(!states.is_empty());
        assert_eq!(
            serde_json::to_value(&rfq.states)?,
            serde_json::to_value(&states)?
        );
        assert!(rfq.messages.is_empty());
        Ok(())
    }

    #[test]
    fn a_boundary_of_another_stream_or_snapshot_is_refused() -> Result<()> {
        for boundary in [
            boundary("stream-2", SNAPSHOT)?,
            boundary(STREAM, "snapshot-2")?,
        ] {
            let session = session_bounded(1, boundary);
            let mut assembly = Assembly::new(&session, CHAIN)?;
            assembly.push(start(1)?)?;
            assembly.push(chunk(2, 0, vec![fragment("uniswap_v3", &["pool-a"])])?)?;
            assembly.push(end(3))?;
            assert!(assembly.finish(session).is_err());
        }
        Ok(())
    }
}
