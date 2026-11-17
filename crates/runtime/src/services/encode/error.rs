use tycho_execution::encoding::errors::EncodingError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EncodeErrorKind {
    InvalidRequest,
    NotFound,
    Unavailable,
    Simulation,
    Encoding,
    Internal,
}

#[derive(Debug)]
pub struct EncodeError {
    kind: EncodeErrorKind,
    message: String,
}

#[derive(Debug)]
pub(super) enum AttemptError {
    StateDependent(EncodeError),
    Deterministic(EncodeError),
}

impl AttemptError {
    pub(super) fn state_dependent(error: EncodeError) -> Self {
        Self::StateDependent(error)
    }

    pub(super) fn deterministic(error: EncodeError) -> Self {
        Self::Deterministic(error)
    }

    #[cfg(test)]
    pub(super) fn is_state_dependent(&self) -> bool {
        matches!(self, Self::StateDependent(_))
    }

    #[cfg(test)]
    pub(super) fn kind(&self) -> EncodeErrorKind {
        match self {
            Self::StateDependent(error) | Self::Deterministic(error) => error.kind(),
        }
    }

    #[cfg(test)]
    pub(super) fn message(&self) -> &str {
        match self {
            Self::StateDependent(error) | Self::Deterministic(error) => error.message(),
        }
    }
}

impl From<AttemptError> for EncodeError {
    fn from(error: AttemptError) -> Self {
        match error {
            AttemptError::StateDependent(error) | AttemptError::Deterministic(error) => error,
        }
    }
}

impl EncodeError {
    pub fn invalid<T: Into<String>>(message: T) -> Self {
        Self {
            kind: EncodeErrorKind::InvalidRequest,
            message: message.into(),
        }
    }

    pub fn not_found<T: Into<String>>(message: T) -> Self {
        Self {
            kind: EncodeErrorKind::NotFound,
            message: message.into(),
        }
    }

    pub fn simulation<T: Into<String>>(message: T) -> Self {
        Self {
            kind: EncodeErrorKind::Simulation,
            message: message.into(),
        }
    }

    pub fn unavailable<T: Into<String>>(message: T) -> Self {
        Self {
            kind: EncodeErrorKind::Unavailable,
            message: message.into(),
        }
    }

    pub fn encoding<T: Into<String>>(message: T) -> Self {
        Self {
            kind: EncodeErrorKind::Encoding,
            message: message.into(),
        }
    }

    pub fn internal<T: Into<String>>(message: T) -> Self {
        Self {
            kind: EncodeErrorKind::Internal,
            message: message.into(),
        }
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    pub fn kind(&self) -> EncodeErrorKind {
        self.kind
    }
}

pub(super) fn map_encoding_error(err: EncodingError) -> EncodeError {
    match err {
        EncodingError::InvalidInput(_) => {
            EncodeError::invalid(format!("Tycho encoding error: {err}"))
        }
        _ => EncodeError::encoding(format!("Tycho encoding error: {err}")),
    }
}

#[cfg(test)]
mod tests {
    use super::{AttemptError, EncodeError, EncodeErrorKind};

    #[test]
    fn unavailable_errors_keep_kind_and_message() {
        let error = EncodeError::unavailable("Encode unavailable: native state warming up");

        assert_eq!(error.kind(), EncodeErrorKind::Unavailable);
        assert_eq!(
            error.message(),
            "Encode unavailable: native state warming up"
        );
    }

    #[test]
    fn attempt_error_buckets_preserve_the_public_error() {
        let state_dependent =
            AttemptError::state_dependent(EncodeError::simulation("pool state changed"));
        assert!(state_dependent.is_state_dependent());
        let state_dependent = EncodeError::from(state_dependent);
        assert_eq!(state_dependent.kind(), EncodeErrorKind::Simulation);
        assert_eq!(state_dependent.message(), "pool state changed");

        // Unavailable state must fail immediately instead of entering the future retry loop.
        let deterministic =
            AttemptError::deterministic(EncodeError::unavailable("state unavailable"));
        assert!(!deterministic.is_state_dependent());
        let deterministic = EncodeError::from(deterministic);
        assert_eq!(deterministic.kind(), EncodeErrorKind::Unavailable);
        assert_eq!(deterministic.message(), "state unavailable");
    }
}
