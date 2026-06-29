use std::error::Error;
use std::fmt;

/// Result type used by the broadcaster replay client.
pub type Result<T> = std::result::Result<T, BroadcasterReplayClientError>;

#[derive(Debug, Clone, PartialEq, Eq)]
/// Errors returned while bootstrapping snapshots or replaying Redis streams.
pub enum BroadcasterReplayClientError {
    /// The configured broadcaster URL is not a usable HTTP(S) base URL.
    InvalidBroadcasterUrl { message: String },
    /// Redis connection setup failed.
    RedisConnect { message: String },
    /// Blocking Redis stream read failed.
    RedisRead { message: String },
    /// Redis stream inspection failed.
    RedisInspect { message: String },
    /// Redis returned a stream entry that could not be decoded.
    RedisDecode { message: String },
    /// Replay continuity checks failed and the caller should rebuild from a snapshot.
    RedisGap { message: String },
    /// Snapshot-session HTTP request failed before a response was received.
    HttpRequest {
        operation: &'static str,
        url: String,
        message: String,
    },
    /// Snapshot-session HTTP request returned a non-success status.
    HttpStatus {
        operation: &'static str,
        url: String,
        status: u16,
    },
    /// Snapshot-session HTTP response body could not be read.
    HttpBody {
        operation: &'static str,
        url: String,
        message: String,
    },
    /// Snapshot-session HTTP response body could not be decoded.
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

    pub(crate) fn redis_inspect(message: impl Into<String>) -> Self {
        Self::RedisInspect {
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

impl fmt::Display for BroadcasterReplayClientError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidBroadcasterUrl { message } => {
                write!(formatter, "invalid broadcaster URL: {message}")
            }
            Self::RedisConnect { message } => {
                write!(
                    formatter,
                    "failed to connect to broadcaster Redis: {message}"
                )
            }
            Self::RedisRead { message } => write!(formatter, "Redis XREAD failed: {message}"),
            Self::RedisInspect { message } => {
                write!(formatter, "Redis XINFO STREAM failed: {message}")
            }
            Self::RedisDecode { message } => {
                write!(formatter, "failed to decode Redis stream entry: {message}")
            }
            Self::RedisGap { message } => formatter.write_str(message),
            Self::HttpRequest {
                operation,
                url,
                message,
            } => write!(formatter, "failed to {operation} at {url}: {message}"),
            Self::HttpStatus {
                operation,
                url,
                status,
            } => write!(formatter, "{operation} at {url} failed with HTTP {status}"),
            Self::HttpBody {
                operation,
                url,
                message,
            } => write!(
                formatter,
                "failed to read {operation} response from {url}: {message}"
            ),
            Self::JsonDecode {
                operation,
                url,
                message,
            } => write!(
                formatter,
                "failed to decode {operation} response from {url}: {message}"
            ),
        }
    }
}

impl Error for BroadcasterReplayClientError {}
