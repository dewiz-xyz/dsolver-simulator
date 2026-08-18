//! Wire contract for the Historical Pool Quote API.

pub mod api;
pub mod openapi;

#[cfg(feature = "engine")]
mod consistency;
#[cfg(feature = "engine")]
mod coverage;
#[cfg(feature = "engine")]
mod error;
#[cfg(feature = "engine")]
mod history;
#[cfg(feature = "engine")]
mod quote_job;
#[cfg(feature = "engine")]
mod replay;

#[cfg(feature = "engine")]
pub use consistency::ConsistencyChecker;
#[cfg(feature = "engine")]
pub use coverage::CoverageService;
#[cfg(feature = "engine")]
pub use error::HistoricalError;
#[cfg(feature = "engine")]
pub use history::{EngineProgress, HistorySource};
#[cfg(feature = "engine")]
pub use quote_job::HistoricalQuoteExecutor;
#[cfg(feature = "engine")]
pub use replay::{
    HistoricalReconstructor, ReconstructedState, ReconstructedTimeline, ReconstructionRequest,
    StateProvenance, UnavailableState,
};
