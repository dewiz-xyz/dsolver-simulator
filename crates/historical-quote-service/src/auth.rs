use std::collections::HashSet;

use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::header::AUTHORIZATION;
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use historical_quote::api::{ApiError, ApiErrorCode};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;

mod reload;

pub use reload::{
    BearerKeySetHandle, BearerKeySetMetadata, BearerKeySetReloader, BearerKeySetSnapshot,
    KeySetSource, KeySetVersion, SecretsManagerKeySetSource,
};

const BEARER_KEY_SCHEMA_VERSION: u32 = 1;
const MAX_BEARER_KEYS: usize = 32;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct BearerKeySetDocument {
    schema_version: u32,
    keys: Vec<BearerKeyDocument>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BearerKeyDocument {
    id: String,
    sha256: String,
}

struct BearerKey {
    id: String,
    digest: [u8; 32],
}

/// Verifies tokens against a fully validated key document.
pub struct BearerTokenVerifier {
    keys: Vec<BearerKey>,
}

impl BearerTokenVerifier {
    /// Parses the complete document before any of its keys can be used.
    ///
    /// # Errors
    ///
    /// Returns a fixed error for invalid schema, IDs, counts, or digests.
    pub fn from_json(value: &str) -> Result<Self, BearerKeySetError> {
        let document: BearerKeySetDocument =
            serde_json::from_str(value).map_err(|_| BearerKeySetError::InvalidDocument)?;
        if document.schema_version != BEARER_KEY_SCHEMA_VERSION
            || !(1..=MAX_BEARER_KEYS).contains(&document.keys.len())
        {
            return Err(BearerKeySetError::InvalidDocument);
        }

        let mut ids = HashSet::with_capacity(document.keys.len());
        let mut digests = HashSet::with_capacity(document.keys.len());
        let mut keys = Vec::with_capacity(document.keys.len());
        for entry in &document.keys {
            if !valid_bearer_key_id(&entry.id) || !ids.insert(entry.id.as_str()) {
                return Err(BearerKeySetError::InvalidDocument);
            }
            let decoded =
                hex::decode(&entry.sha256).map_err(|_| BearerKeySetError::InvalidDocument)?;
            let digest: [u8; 32] = decoded
                .try_into()
                .map_err(|_| BearerKeySetError::InvalidDocument)?;
            if !digests.insert(digest) {
                return Err(BearerKeySetError::InvalidDocument);
            }
            keys.push(BearerKey {
                id: entry.id.clone(),
                digest,
            });
        }
        keys.sort_by(|left, right| left.id.cmp(&right.id));
        Ok(Self { keys })
    }

    /// Returns the matching audit ID after comparing every configured digest.
    #[must_use]
    pub fn verify(&self, presented: &str) -> Option<&str> {
        let candidate: [u8; 32] = Sha256::digest(presented.as_bytes()).into();
        let mut matched_id = None;
        for key in &self.keys {
            if bool::from(candidate.ct_eq(&key.digest)) {
                matched_id = Some(key.id.as_str());
            }
        }
        matched_id
    }
}

/// Fixed errors keep documents and SDK response details out of logs.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum BearerKeySetError {
    #[error("bearer key document is invalid")]
    InvalidDocument,
    #[error("bearer key source is unavailable")]
    SourceUnavailable,
    #[error("bearer key source response is invalid")]
    InvalidSourceResponse,
    #[error("bearer key source read timed out")]
    ReadTimeout,
}

pub async fn require_bearer(
    State(keys): State<BearerKeySetHandle>,
    mut request: Request<Body>,
    next: Next,
) -> Response {
    let snapshot = keys.snapshot();
    let token = request
        .headers()
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(bearer_value);
    if let Some(key_id) = token.and_then(|token| snapshot.verify(token)) {
        tracing::info!(
            bearer_key_id = key_id,
            method = %request.method(),
            path = request.uri().path(),
            "historical quote API request authenticated"
        );
        // Status must describe the same keys that authenticated this request.
        request.extensions_mut().insert(snapshot);
        return next.run(request).await;
    }
    (
        StatusCode::UNAUTHORIZED,
        axum::Json(ApiError {
            code: ApiErrorCode::Unauthorized,
            message: "bearer token is missing or invalid".to_owned(),
            retryable: false,
            details: None,
        }),
    )
        .into_response()
}

fn valid_bearer_key_id(id: &str) -> bool {
    let bytes = id.as_bytes();
    let is_alphanumeric = |byte: u8| byte.is_ascii_lowercase() || byte.is_ascii_digit();
    (1..=63).contains(&bytes.len())
        && bytes.first().is_some_and(|byte| is_alphanumeric(*byte))
        && bytes.last().is_some_and(|byte| is_alphanumeric(*byte))
        && bytes
            .iter()
            .all(|byte| is_alphanumeric(*byte) || *byte == b'-')
}

fn bearer_value(value: &str) -> Option<&str> {
    let (scheme, token) = value.split_once(' ')?;
    (scheme.eq_ignore_ascii_case("bearer") && !token.is_empty() && !token.contains(' '))
        .then_some(token)
}

#[cfg(test)]
#[path = "auth/tests.rs"]
mod tests;
