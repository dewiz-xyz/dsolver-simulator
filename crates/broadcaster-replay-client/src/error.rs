use thiserror::Error;

/// Result type used by the broadcaster replay client.
pub type Result<T> = std::result::Result<T, BroadcasterReplayClientError>;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
/// Errors returned while bootstrapping snapshots or replaying Redis streams.
pub enum BroadcasterReplayClientError {
    /// The configured broadcaster URL is not a usable HTTP(S) base URL.
    #[error("invalid broadcaster URL: {message}")]
    InvalidBroadcasterUrl { message: String },
    /// Redis connection setup failed.
    #[error("failed to connect to broadcaster Redis: {message}")]
    RedisConnect { message: String },
    /// Blocking Redis stream read failed.
    #[error("Redis XREAD failed: {message}")]
    RedisRead { message: String },
    /// Blocking Redis stream read hit a transient transport failure.
    #[error("Redis XREAD transport failed: {message}")]
    RedisReadTransport { message: String },
    /// Redis stream inspection failed.
    #[error("Redis XINFO STREAM failed: {message}")]
    RedisInspect { message: String },
    /// Redis stream inspection hit a transient transport failure.
    #[error("Redis XINFO STREAM transport failed: {message}")]
    RedisInspectTransport { message: String },
    /// Redis returned a stream entry that could not be decoded.
    #[error("failed to decode Redis stream entry: {message}")]
    RedisDecode { message: String },
    /// Replay continuity checks failed and the caller should rebuild from a snapshot.
    #[error("{message}")]
    RedisGap { message: String },
    /// Snapshot-session HTTP request failed before a response was received.
    #[error("failed to {operation} at {url}: {message}")]
    HttpRequest {
        operation: &'static str,
        url: String,
        message: String,
    },
    /// Snapshot-session HTTP request returned a non-success status.
    #[error("{operation} at {url} failed with HTTP {status}")]
    HttpStatus {
        operation: &'static str,
        url: String,
        status: u16,
    },
    /// Snapshot-session HTTP response body could not be read.
    #[error("failed to read {operation} response from {url}: {message}")]
    HttpBody {
        operation: &'static str,
        url: String,
        message: String,
    },
    /// Snapshot-session HTTP response body could not be decoded.
    #[error("failed to decode {operation} response from {url}: {message}")]
    JsonDecode {
        operation: &'static str,
        url: String,
        message: String,
    },
}

impl BroadcasterReplayClientError {
    pub(crate) fn invalid_broadcaster_url(message: impl Into<String>) -> Self {
        Self::InvalidBroadcasterUrl {
            message: message.into(),
        }
    }

    pub(crate) fn redis_connect(message: impl Into<String>) -> Self {
        Self::RedisConnect {
            message: message.into(),
        }
    }

    pub(crate) fn redis_read(message: impl Into<String>) -> Self {
        Self::RedisRead {
            message: message.into(),
        }
    }

    pub(crate) fn redis_read_transport(message: impl Into<String>) -> Self {
        Self::RedisReadTransport {
            message: message.into(),
        }
    }

    pub(crate) fn redis_inspect(message: impl Into<String>) -> Self {
        Self::RedisInspect {
            message: message.into(),
        }
    }

    pub(crate) fn redis_inspect_transport(message: impl Into<String>) -> Self {
        Self::RedisInspectTransport {
            message: message.into(),
        }
    }

    pub(crate) fn redis_decode(message: impl Into<String>) -> Self {
        Self::RedisDecode {
            message: message.into(),
        }
    }

    pub(crate) fn redis_gap(message: impl Into<String>) -> Self {
        Self::RedisGap {
            message: message.into(),
        }
    }

    pub(crate) fn http_request(
        operation: &'static str,
        url: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self::HttpRequest {
            operation,
            url: url.into(),
            message: message.into(),
        }
    }

    pub(crate) fn http_status(
        operation: &'static str,
        url: impl Into<String>,
        status: u16,
    ) -> Self {
        Self::HttpStatus {
            operation,
            url: url.into(),
            status,
        }
    }

    pub(crate) fn http_body(
        operation: &'static str,
        url: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self::HttpBody {
            operation,
            url: url.into(),
            message: message.into(),
        }
    }

    pub(crate) fn json_decode(
        operation: &'static str,
        url: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self::JsonDecode {
            operation,
            url: url.into(),
            message: message.into(),
        }
    }
}
