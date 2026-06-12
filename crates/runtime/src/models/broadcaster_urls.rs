use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BroadcasterUrlError {
    message: String,
}

impl BroadcasterUrlError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for BroadcasterUrlError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for BroadcasterUrlError {}

pub fn derive_broadcaster_http_url(
    ws_url: &str,
    relative_path: &str,
) -> Result<String, BroadcasterUrlError> {
    let mut url = parse_broadcaster_ws_url(ws_url)?;
    let scheme = match url.scheme() {
        "ws" => "http",
        "wss" => "https",
        other => {
            return Err(BroadcasterUrlError::new(format!(
                "TYCHO_BROADCASTER_WS_URL must use ws or wss, got {other}"
            )))
        }
    };
    let prefix = websocket_path_prefix(&url)?;
    let http_path = prefixed_path(prefix, relative_path);

    url.set_scheme(scheme)
        .map_err(|_| BroadcasterUrlError::new("invalid broadcaster URL scheme"))?;
    url.set_path(&http_path);
    url.set_query(None);
    Ok(url.to_string())
}

pub fn derive_broadcaster_session_ws_url(
    ws_url: &str,
    session_id: u64,
) -> Result<String, BroadcasterUrlError> {
    let mut url = parse_broadcaster_ws_url(ws_url)?;
    websocket_path_prefix(&url)?;
    url.query_pairs_mut()
        .clear()
        .append_pair("sessionId", &session_id.to_string());
    Ok(url.to_string())
}

pub fn derive_broadcaster_rfq_session_ws_url(
    ws_url: &str,
    session_id: u64,
) -> Result<String, BroadcasterUrlError> {
    let mut url = parse_broadcaster_ws_url(ws_url)?;
    let prefix = websocket_path_prefix(&url)?;
    let rfq_ws_path = prefixed_path(prefix, "rfq/ws");
    url.set_path(&rfq_ws_path);
    url.query_pairs_mut()
        .clear()
        .append_pair("sessionId", &session_id.to_string());
    Ok(url.to_string())
}

fn parse_broadcaster_ws_url(ws_url: &str) -> Result<reqwest::Url, BroadcasterUrlError> {
    reqwest::Url::parse(ws_url)
        .map_err(|err| BroadcasterUrlError::new(format!("invalid TYCHO_BROADCASTER_WS_URL: {err}")))
}

fn prefixed_path(prefix: &str, relative_path: &str) -> String {
    if prefix.is_empty() {
        format!("/{relative_path}")
    } else {
        format!("{prefix}/{relative_path}")
    }
}

fn websocket_path_prefix(url: &reqwest::Url) -> Result<&str, BroadcasterUrlError> {
    url.path()
        .strip_suffix("/ws")
        .ok_or_else(|| BroadcasterUrlError::new("TYCHO_BROADCASTER_WS_URL must end with /ws"))
}

#[cfg(test)]
mod tests {
    use anyhow::Result;

    use super::{
        derive_broadcaster_http_url, derive_broadcaster_rfq_session_ws_url,
        derive_broadcaster_session_ws_url,
    };

    #[test]
    fn derives_http_url_from_broadcaster_websocket_url() -> Result<()> {
        assert_eq!(
            derive_broadcaster_http_url("ws://127.0.0.1:3001/ws", "snapshot-sessions")?,
            "http://127.0.0.1:3001/snapshot-sessions"
        );
        assert_eq!(
            derive_broadcaster_http_url(
                "wss://broadcaster.example/prod/base/ws",
                "snapshot-sessions/7/payloads/2"
            )?,
            "https://broadcaster.example/prod/base/snapshot-sessions/7/payloads/2"
        );
        Ok(())
    }

    #[test]
    fn derives_session_websocket_url_from_broadcaster_websocket_url() -> Result<()> {
        assert_eq!(
            derive_broadcaster_session_ws_url("ws://127.0.0.1:3001/ws", 42)?,
            "ws://127.0.0.1:3001/ws?sessionId=42"
        );
        assert_eq!(
            derive_broadcaster_session_ws_url("wss://broadcaster.example/prod/base/ws", 9)?,
            "wss://broadcaster.example/prod/base/ws?sessionId=9"
        );
        Ok(())
    }

    #[test]
    fn derives_rfq_session_websocket_url_from_broadcaster_websocket_url() -> Result<()> {
        assert_eq!(
            derive_broadcaster_rfq_session_ws_url("ws://127.0.0.1:3001/ws", 42)?,
            "ws://127.0.0.1:3001/rfq/ws?sessionId=42"
        );
        assert_eq!(
            derive_broadcaster_rfq_session_ws_url("wss://broadcaster.example/prod/base/ws", 9)?,
            "wss://broadcaster.example/prod/base/rfq/ws?sessionId=9"
        );
        Ok(())
    }

    #[test]
    fn rejects_non_websocket_url() {
        let Err(error) =
            derive_broadcaster_http_url("http://127.0.0.1:3001/ws", "snapshot-sessions")
        else {
            unreachable!("non-websocket URL should fail");
        };

        assert!(error.to_string().contains("must use ws or wss"));
    }

    #[test]
    fn rejects_urls_without_ws_suffix() {
        let Err(error) = derive_broadcaster_session_ws_url("ws://127.0.0.1:3001/socket", 1) else {
            unreachable!("non-/ws URL should fail");
        };

        assert!(error.to_string().contains("must end with /ws"));
    }
}
