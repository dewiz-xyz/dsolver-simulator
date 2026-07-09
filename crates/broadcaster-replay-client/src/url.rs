use crate::error::{BroadcasterReplayClientError, Result};

pub(crate) fn derive_broadcaster_http_url(base_url: &str, relative_path: &str) -> Result<String> {
    let mut url = reqwest::Url::parse(base_url).map_err(|error| {
        BroadcasterReplayClientError::invalid_broadcaster_url(error.to_string())
    })?;
    match url.scheme() {
        "http" | "https" => {}
        other => {
            return Err(BroadcasterReplayClientError::invalid_broadcaster_url(
                format!("TYCHO_BROADCASTER_URL must use http or https, got {other}"),
            ))
        }
    };
    let prefix = broadcaster_path_prefix(&url);
    let http_path = prefixed_path(prefix, relative_path);

    url.set_path(&http_path);
    url.set_query(None);
    Ok(url.to_string())
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
