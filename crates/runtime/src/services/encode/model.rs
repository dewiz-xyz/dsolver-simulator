use std::sync::Arc;

use num_bigint::BigUint;
use tycho_simulation::{
    protocol::models::ProtocolComponent,
    tycho_common::{simulation::protocol_sim::ProtocolSim, Bytes},
};

use crate::models::messages::PoolRef;

use super::backend::PoolBackend;

pub(super) struct NormalizedRouteInternal {
    pub(super) segments: Vec<NormalizedSegmentInternal>,
}

pub(super) struct NormalizedSegmentInternal {
    pub(super) share_bps: u32,
    pub(super) amount_in: BigUint,
    pub(super) hops: Vec<NormalizedHopInternal>,
}

pub(super) struct NormalizedHopInternal {
    pub(super) token_in: Bytes,
    pub(super) token_out: Bytes,
    pub(super) swaps: Vec<NormalizedSwapDraftInternal>,
}

pub(super) struct NormalizedSwapDraftInternal {
    pub(super) pool: PoolRef,
    pub(super) token_in: Bytes,
    pub(super) token_out: Bytes,
    pub(super) split_bps: u32,
}

pub(super) struct ResimulatedRouteInternal {
    pub(super) segments: Vec<ResimulatedSegmentInternal>,
}

pub(super) struct ResimulatedSegmentInternal {
    pub(super) share_bps: u32,
    pub(super) amount_in: BigUint,
    pub(super) expected_amount_out: BigUint,
    pub(super) hops: Vec<ResimulatedHopInternal>,
}

pub(super) struct ResimulatedHopInternal {
    pub(super) token_in: Bytes,
    pub(super) token_out: Bytes,
    pub(super) amount_in: BigUint,
    pub(super) expected_amount_out: BigUint,
    pub(super) swaps: Vec<ResimulatedSwapInternal>,
}

pub(super) struct ResimulatedSwapInternal {
    pub(super) pool: PoolRef,
    pub(super) backend: PoolBackend,
    pub(super) token_in: Bytes,
    pub(super) token_out: Bytes,
    pub(super) split_bps: u32,
    pub(super) amount_in: BigUint,
    pub(super) expected_amount_out: BigUint,
    pub(super) pool_state: Arc<dyn ProtocolSim>,
    pub(super) component: Arc<ProtocolComponent>,
}
