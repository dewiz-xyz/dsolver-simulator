use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use tycho_simulation::{
    protocol::models::{ProtocolComponent, Update},
    tycho_common::simulation::protocol_sim::ProtocolSim,
};

use simulator_core::{
    broadcaster::{
        BroadcasterBackend, BroadcasterSnapshotPartition, BroadcasterUpdatePartition,
        ProtocolHeadUpdate,
    },
    models::protocol::ProtocolKind,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReplayBackend {
    Native,
    Vm,
    Rfq,
    PropAmm,
}

impl ReplayBackend {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Native => "native",
            Self::Vm => "vm",
            Self::Rfq => "rfq",
            Self::PropAmm => "prop_amm",
        }
    }

    pub const fn is_book(self) -> bool {
        matches!(self, Self::Rfq | Self::PropAmm)
    }

    pub const fn from_protocol_kind(kind: ProtocolKind) -> Self {
        Self::from_broadcaster_backend(BroadcasterBackend::from_protocol_kind(kind))
    }

    const fn from_broadcaster_backend(value: BroadcasterBackend) -> Self {
        match value {
            BroadcasterBackend::Native => Self::Native,
            BroadcasterBackend::Vm => Self::Vm,
            BroadcasterBackend::Rfq => Self::Rfq,
            BroadcasterBackend::PropAmm => Self::PropAmm,
        }
    }
}

impl From<BroadcasterBackend> for ReplayBackend {
    fn from(value: BroadcasterBackend) -> Self {
        Self::from_broadcaster_backend(value)
    }
}

impl From<ReplayBackend> for BroadcasterBackend {
    fn from(value: ReplayBackend) -> Self {
        match value {
            ReplayBackend::Native => Self::Native,
            ReplayBackend::Vm => Self::Vm,
            ReplayBackend::Rfq => Self::Rfq,
            ReplayBackend::PropAmm => Self::PropAmm,
        }
    }
}

#[derive(Debug)]
pub struct DecodedReplay {
    pub had_applicable_partition: bool,
    pub block_number: u64,
    pub complete_native_block: Option<u64>,
    pub protocol_head_updates: Vec<ProtocolHeadUpdate>,
    pub update: Option<Update>,
}

pub(crate) fn snapshot_partition_update(partition: BroadcasterSnapshotPartition) -> Update {
    let mut states = HashMap::new();
    let mut new_pairs = HashMap::new();

    for entry in partition.states {
        states.insert(entry.component_id.clone(), entry.state);
        new_pairs.insert(entry.component_id, entry.component);
    }

    Update::new(partition.block_number, states, new_pairs)
}

pub(crate) fn live_partition_update(partition: BroadcasterUpdatePartition) -> Update {
    let block_number = partition.block_number;
    let mut states: HashMap<String, Box<dyn ProtocolSim>> = HashMap::new();
    let mut new_pairs: HashMap<String, ProtocolComponent> = HashMap::new();
    let mut removed_pairs = HashMap::new();

    for entry in partition.new_pairs {
        states.insert(entry.component_id.clone(), entry.state);
        new_pairs.insert(entry.component_id, entry.component);
    }

    for delta in partition.updated_states {
        states.insert(delta.component_id, delta.state);
    }

    for removed in partition.removed_pairs {
        removed_pairs.insert(removed.component_id, removed.component);
    }

    Update::new(block_number, states, new_pairs).set_removed_pairs(removed_pairs)
}
