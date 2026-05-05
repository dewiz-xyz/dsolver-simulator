use tycho_simulation::protocol::models::ProtocolComponent;

use crate::models::protocol::ProtocolKind;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum PoolBackend {
    Native,
    Vm,
    Rfq,
}

impl PoolBackend {
    fn from_kind(kind: Option<ProtocolKind>) -> Self {
        match kind {
            Some(ProtocolKind::Curve | ProtocolKind::BalancerV2 | ProtocolKind::MaverickV2) => {
                Self::Vm
            }
            Some(ProtocolKind::Hashflow | ProtocolKind::Bebop | ProtocolKind::Liquorice) => {
                Self::Rfq
            }
            _ => Self::Native,
        }
    }

    pub(super) fn from_component(component: &ProtocolComponent) -> Self {
        Self::from_kind(ProtocolKind::from_component(component))
    }

    pub(super) fn from_protocol_hint(protocol: &str) -> Self {
        Self::from_kind(ProtocolKind::from_sync_state_key(protocol))
    }

    pub(super) const fn is_native(self) -> bool {
        matches!(self, Self::Native)
    }

    pub(super) const fn is_vm(self) -> bool {
        matches!(self, Self::Vm)
    }

    pub(super) const fn is_rfq(self) -> bool {
        matches!(self, Self::Rfq)
    }

    pub(super) const fn uses_rebuild_guard(self) -> bool {
        matches!(self, Self::Vm | Self::Rfq)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use tycho_simulation::{
        protocol::models::ProtocolComponent,
        tycho_common::{models::Chain, Bytes},
    };

    use super::PoolBackend;

    fn protocol_component(protocol_system: &str, protocol_type_name: &str) -> ProtocolComponent {
        ProtocolComponent::new(
            Bytes::default(),
            protocol_system.to_string(),
            protocol_type_name.to_string(),
            Chain::Ethereum,
            Vec::new(),
            Vec::new(),
            HashMap::new(),
            Bytes::default(),
            chrono::NaiveDateTime::default(),
        )
    }

    #[test]
    fn from_protocol_hint_recognizes_vm_and_rfq_hints() {
        assert!(PoolBackend::from_protocol_hint("VM:MAVERICK_V2").is_vm());
        assert!(PoolBackend::from_protocol_hint("RFQ:BEBOP").is_rfq());
        assert!(PoolBackend::from_protocol_hint("uniswap_v3").is_native());
    }

    #[test]
    fn from_protocol_hint_defaults_unknown_to_native() {
        assert!(PoolBackend::from_protocol_hint("unknown").is_native());
        assert!(PoolBackend::from_protocol_hint("hashflow_pool").is_native());
    }

    #[test]
    fn from_component_recognizes_type_name_only_rfq_components() {
        assert!(PoolBackend::from_component(&protocol_component("", "hashflow_pool")).is_rfq());
        assert!(PoolBackend::from_component(&protocol_component("", "bebop_pool")).is_rfq());
        assert!(PoolBackend::from_component(&protocol_component("", "liquorice_pool")).is_rfq());
    }

    #[test]
    fn from_component_prefers_protocol_system_before_type_name() {
        let component = protocol_component("uniswap_v3", "hashflow_pool");

        assert!(PoolBackend::from_component(&component).is_native());
    }
}
