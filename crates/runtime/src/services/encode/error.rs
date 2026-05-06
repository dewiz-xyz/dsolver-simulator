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
    use super::{EncodeError, EncodeErrorKind};

    #[test]
    fn unavailable_errors_keep_kind_and_message() {
        let error = EncodeError::unavailable("Encode unavailable: native state warming up");

        assert_eq!(error.kind(), EncodeErrorKind::Unavailable);
        assert_eq!(
            error.message(),
            "Encode unavailable: native state warming up"
        );
    }
}
