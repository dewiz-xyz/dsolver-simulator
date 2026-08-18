use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::header::AUTHORIZATION;
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use historical_quote::api::{ApiError, ApiErrorCode};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;

#[derive(Clone)]
pub struct BearerTokenVerifier {
    expected_digest: [u8; 32],
}

impl BearerTokenVerifier {
    #[must_use]
    pub const fn new(expected_digest: [u8; 32]) -> Self {
        Self { expected_digest }
    }

    #[must_use]
    pub fn verify(&self, presented: &str) -> bool {
        let candidate: [u8; 32] = Sha256::digest(presented.as_bytes()).into();
        bool::from(candidate.ct_eq(&self.expected_digest))
    }
}

pub async fn require_bearer(
    State(verifier): State<BearerTokenVerifier>,
    request: Request<Body>,
    next: Next,
) -> Response {
    let token = request
        .headers()
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(bearer_value);
    if token.is_some_and(|token| verifier.verify(token)) {
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

fn bearer_value(value: &str) -> Option<&str> {
    let (scheme, token) = value.split_once(' ')?;
    (scheme.eq_ignore_ascii_case("bearer") && !token.is_empty() && !token.contains(' '))
        .then_some(token)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn verifies_only_the_token_matching_the_configured_digest() {
        let expected_digest = Sha256::digest(b"expected-token").into();
        let verifier = BearerTokenVerifier::new(expected_digest);

        assert!(verifier.verify("expected-token"));
        assert!(!verifier.verify("wrong-token"));
        assert!(!verifier.verify(""));
    }

    #[test]
    fn accepts_one_bearer_value_without_normalizing_the_secret() {
        assert_eq!(bearer_value("Bearer exact-token"), Some("exact-token"));
        assert_eq!(bearer_value("bearer exact-token"), Some("exact-token"));
        assert_eq!(bearer_value("Bearer exact-token "), None);
        assert_eq!(bearer_value("Bearer  exact-token"), None);
        assert_eq!(bearer_value("Basic exact-token"), None);
    }
}
