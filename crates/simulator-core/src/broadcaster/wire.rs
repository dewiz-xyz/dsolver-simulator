//! Serializable raw feed contract. Keep transport DTOs separate from Tycho's internal models.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use tycho_simulation::{
    tycho_client::feed::{self, dto, synchronizer, BlockHeader, HeaderLike, SynchronizerState},
    tycho_common::{
        dto::{
            BlockAggregatedChanges, EntryPointWithTracingParams, ProtocolComponent,
            ResponseAccount, ResponseProtocolState, TracingResult,
        },
        Bytes,
    },
};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ComponentWithState {
    pub state: ResponseProtocolState,
    pub component: ProtocolComponent,
    pub component_tvl: Option<f64>,
    pub entrypoints: Vec<(EntryPointWithTracingParams, TracingResult)>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Snapshot {
    pub states: HashMap<String, ComponentWithState>,
    pub vm_storage: HashMap<Bytes, ResponseAccount>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct StateSyncMessage<H = BlockHeader> {
    pub header: H,
    pub snapshots: Snapshot,
    pub deltas: Option<BlockAggregatedChanges>,
    pub removed_components: HashMap<String, ProtocolComponent>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct FeedMessage<H = BlockHeader> {
    pub state_msgs: HashMap<String, StateSyncMessage<H>>,
    pub sync_states: HashMap<String, SynchronizerState>,
}

impl<H> StateSyncMessage<H> {
    pub fn merge(mut self, other: Self) -> Self {
        self.removed_components
            .retain(|id, _| !other.snapshots.states.contains_key(id));
        self.snapshots
            .states
            .retain(|id, _| !other.removed_components.contains_key(id));
        self.snapshots.states.extend(other.snapshots.states);
        self.snapshots.vm_storage.extend(other.snapshots.vm_storage);
        self.deltas = match (self.deltas, other.deltas) {
            (Some(left), Some(right)) => Some(left.merge(right)),
            (left, right) => left.or(right),
        };
        self.removed_components.extend(other.removed_components);
        self.header = other.header;
        self
    }
}

impl From<dto::ComponentWithState> for ComponentWithState {
    fn from(value: dto::ComponentWithState) -> Self {
        Self {
            state: value.state,
            component: value.component,
            component_tvl: value.component_tvl,
            entrypoints: value.entrypoints,
        }
    }
}

impl From<ComponentWithState> for dto::ComponentWithState {
    fn from(value: ComponentWithState) -> Self {
        Self {
            state: value.state,
            component: value.component,
            component_tvl: value.component_tvl,
            entrypoints: value.entrypoints,
        }
    }
}

impl<H: HeaderLike> From<synchronizer::StateSyncMessage<H>> for StateSyncMessage<H> {
    fn from(value: synchronizer::StateSyncMessage<H>) -> Self {
        let value = dto::StateSyncMessage::from(value);
        Self {
            header: value.header,
            snapshots: Snapshot {
                states: value
                    .snapshots
                    .states
                    .into_iter()
                    .map(|(id, state)| (id, state.into()))
                    .collect(),
                vm_storage: value.snapshots.vm_storage,
            },
            deltas: value.deltas,
            removed_components: value.removed_components,
        }
    }
}

impl<H: HeaderLike> From<StateSyncMessage<H>> for synchronizer::StateSyncMessage<H> {
    fn from(value: StateSyncMessage<H>) -> Self {
        dto::StateSyncMessage {
            header: value.header,
            snapshots: dto::Snapshot {
                states: value
                    .snapshots
                    .states
                    .into_iter()
                    .map(|(id, state)| (id, state.into()))
                    .collect(),
                vm_storage: value.snapshots.vm_storage,
            },
            deltas: value.deltas,
            removed_components: value.removed_components,
        }
        .into()
    }
}

impl<H: HeaderLike> From<feed::FeedMessage<H>> for FeedMessage<H> {
    fn from(value: feed::FeedMessage<H>) -> Self {
        Self {
            state_msgs: value
                .state_msgs
                .into_iter()
                .map(|(id, message)| (id, message.into()))
                .collect(),
            sync_states: value.sync_states,
        }
    }
}

impl<H: HeaderLike> From<FeedMessage<H>> for feed::FeedMessage<H> {
    fn from(value: FeedMessage<H>) -> Self {
        Self {
            state_msgs: value
                .state_msgs
                .into_iter()
                .map(|(id, message)| (id, message.into()))
                .collect(),
            sync_states: value.sync_states,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tycho_simulation::tycho_common::dto::ProtocolStateDelta;

    #[test]
    fn raw_model_and_json_round_trip_preserves_snapshot_and_delta_fields() -> anyhow::Result<()> {
        let mut component = ProtocolComponent {
            id: "0x1111111111111111111111111111111111111111".to_string(),
            protocol_system: "uniswap_v2".to_string(),
            protocol_type_name: "uniswap_v2_pool".to_string(),
            ..Default::default()
        };
        component
            .static_attributes
            .insert("fee".to_string(), Bytes::from([30u8]));
        let id = component.id.clone();
        let mut changes = BlockAggregatedChanges {
            partial_block_index: Some(2),
            ..Default::default()
        };
        changes.paused_components.insert("paused-pool".to_string());
        changes
            .unpaused_components
            .insert("resumed-pool".to_string());
        changes.state_updates.insert(
            id.clone(),
            ProtocolStateDelta {
                component_id: id.clone(),
                updated_attributes: HashMap::from([("reserve0".to_string(), Bytes::from([42u8]))]),
                deleted_attributes: Default::default(),
            },
        );
        let wire = FeedMessage {
            state_msgs: HashMap::from([(
                "uniswap_v2".to_string(),
                StateSyncMessage {
                    header: BlockHeader {
                        number: 123,
                        partial_block_index: Some(2),
                        ..Default::default()
                    },
                    snapshots: Snapshot {
                        states: HashMap::from([(
                            id.clone(),
                            ComponentWithState {
                                state: ResponseProtocolState {
                                    component_id: id,
                                    ..Default::default()
                                },
                                component,
                                component_tvl: Some(100.0),
                                entrypoints: Vec::new(),
                            },
                        )]),
                        vm_storage: HashMap::new(),
                    },
                    deltas: Some(changes),
                    removed_components: HashMap::new(),
                },
            )]),
            sync_states: HashMap::new(),
        };
        let decoded: FeedMessage = serde_json::from_slice(&serde_json::to_vec(&wire)?)?;
        assert_eq!(wire, decoded);
        let model: feed::FeedMessage = decoded.into();
        let restored: FeedMessage = model.into();
        assert_eq!(wire, restored);
        Ok(())
    }
}
