use anyhow::Error as AnyError;

#[derive(Debug, thiserror::Error)]
pub enum HistoricalError {
    #[error("historical work was cancelled")]
    Cancelled,
    #[error("historical state read failed during {operation}")]
    HistoryRead {
        operation: &'static str,
        #[source]
        source: AnyError,
    },
    #[error("retained historical data is invalid: {0}")]
    HistoricalDataInvalid(String),
    #[error("historical state reconstruction failed: {0}")]
    StateReconstructionFailed(String),
    #[error("historical request selector is invalid: {0}")]
    InvalidSelector(String),
    #[error("canonical VM comparison is unsupported")]
    UnsupportedCanonicalVm,
}

impl HistoricalError {
    pub(crate) fn history_read(operation: &'static str, source: impl Into<AnyError>) -> Self {
        Self::HistoryRead {
            operation,
            source: source.into(),
        }
    }
}
