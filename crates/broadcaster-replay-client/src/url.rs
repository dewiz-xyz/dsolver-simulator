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
    let prefix = url.path().trim_end_matches('/');
    let relative_path = relative_path.trim_start_matches('/');
    let http_path = format!("{prefix}/{relative_path}");

    url.set_path(&http_path);
    url.set_query(None);
    Ok(url.to_string())
}
