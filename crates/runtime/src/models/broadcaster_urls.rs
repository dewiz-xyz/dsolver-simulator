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
    base_url: &str,
    relative_path: &str,
) -> Result<String, BroadcasterUrlError> {
    let mut url = parse_broadcaster_url(base_url)?;
    match url.scheme() {
        "http" | "https" => {}
        other => {
            return Err(BroadcasterUrlError::new(format!(
                "TYCHO_BROADCASTER_URL must use http or https, got {other}"
            )))
        }
    };
    let prefix = broadcaster_path_prefix(&url);
    let http_path = prefixed_path(prefix, relative_path);

    url.set_path(&http_path);
    url.set_query(None);
    Ok(url.to_string())
}

fn parse_broadcaster_url(base_url: &str) -> Result<reqwest::Url, BroadcasterUrlError> {
    reqwest::Url::parse(base_url)
        .map_err(|err| BroadcasterUrlError::new(format!("invalid TYCHO_BROADCASTER_URL: {err}")))
}

fn prefixed_path(prefix: &str, relative_path: &str) -> String {
    let relative_path = relative_path.trim_start_matches('/');
    if prefix.is_empty() {
        format!("/{relative_path}")
    } else {
        format!("{prefix}/{relative_path}")
    }
}

fn broadcaster_path_prefix(url: &reqwest::Url) -> &str {
    match url.path().trim_end_matches('/') {
        "" => "",
        path => path,
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Result;

    use super::derive_broadcaster_http_url;

    #[test]
    fn derives_http_url_from_broadcaster_base_url() -> Result<()> {
        assert_eq!(
            derive_broadcaster_http_url("http://127.0.0.1:3001", "snapshot-sessions")?,
            "http://127.0.0.1:3001/snapshot-sessions"
        );
        assert_eq!(
            derive_broadcaster_http_url(
                "https://broadcaster.example/prod/base",
                "snapshot-sessions/7/payloads/2"
            )?,
            "https://broadcaster.example/prod/base/snapshot-sessions/7/payloads/2"
        );
        Ok(())
    }

    #[test]
    fn rejects_non_http_url() {
        let Err(error) = derive_broadcaster_http_url("ws://127.0.0.1:3001/ws", "snapshot-sessions")
        else {
            unreachable!("non-http URL should fail");
        };

        assert!(error.to_string().contains("must use http or https"));
    }

    #[test]
    fn trims_base_url_trailing_slash() -> Result<()> {
        assert_eq!(
            derive_broadcaster_http_url("http://127.0.0.1:3001/prod/base/", "/tokens/lookup")?,
            "http://127.0.0.1:3001/prod/base/tokens/lookup"
        );
        Ok(())
    }
}
