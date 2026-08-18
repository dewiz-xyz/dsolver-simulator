use std::future::Future;
use std::time::Duration;

use historical_quote::api::{
    ApiError, ConsistencyCheckRequest, CoverageRequest, CoverageResponse, JobEnvelope,
    JobSubmission, QuoteJobRequest, StatusResponse, API_REVISION,
};
use reqwest::header::RETRY_AFTER;
use reqwest::{RequestBuilder, StatusCode, Url};
use serde::de::DeserializeOwned;
use tokio::time::Instant;
use uuid::Uuid;

use crate::retry::{sleep_before, RetryState};

const REVISION_HEADER: &str = "X-DSolver-History-API-Revision";

#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("request deadline expired")]
    DeadlineExpired,
    #[error("HTTP transport failed: {0}")]
    Transport(String),
    #[error("service returned HTTP {status}: {error:?}")]
    Api { status: u16, error: ApiError },
    #[error("service returned an invalid response: {0}")]
    InvalidResponse(String),
}

impl ClientError {
    #[must_use]
    pub fn is_admission_or_authentication(&self) -> bool {
        matches!(
            self,
            Self::Api {
                status: 400 | 401 | 409 | 422 | 429 | 503,
                ..
            }
        )
    }

    #[must_use]
    pub fn is_job_not_found(&self) -> bool {
        matches!(
            self,
            Self::Api {
                status: 404,
                error: ApiError {
                    code: historical_quote::api::ApiErrorCode::JobNotFound,
                    ..
                }
            }
        )
    }
}

#[derive(Clone)]
pub struct HistoryClient {
    base_url: Url,
    token: String,
    http: reqwest::Client,
}

impl HistoryClient {
    pub fn new(url: &str, token: String) -> Result<Self, String> {
        let mut base_url = Url::parse(url).map_err(|_| "DSOLVER_HISTORY_URL is invalid")?;
        if !matches!(base_url.scheme(), "http" | "https") {
            return Err("DSOLVER_HISTORY_URL must use http or https".to_owned());
        }
        if base_url.query().is_some() || base_url.fragment().is_some() {
            return Err("DSOLVER_HISTORY_URL must not contain a query or fragment".to_owned());
        }
        if !base_url.path().ends_with('/') {
            let path = format!("{}/", base_url.path());
            base_url.set_path(&path);
        }
        Ok(Self {
            base_url,
            token,
            http: reqwest::Client::new(),
        })
    }

    pub async fn submit_quote(
        &self,
        request: &QuoteJobRequest,
        deadline: Instant,
    ) -> Result<JobSubmission, ClientError> {
        let url = self.endpoint("jobs/quote")?;
        self.send_json(deadline, || {
            self.authorized(self.http.post(url.clone())).json(request)
        })
        .await
    }

    pub async fn submit_consistency(
        &self,
        request: &ConsistencyCheckRequest,
        deadline: Instant,
    ) -> Result<JobSubmission, ClientError> {
        let url = self.endpoint("jobs/consistency-check")?;
        self.send_json(deadline, || {
            self.authorized(self.http.post(url.clone())).json(request)
        })
        .await
    }

    pub async fn coverage(
        &self,
        request: &CoverageRequest,
        deadline: Instant,
    ) -> Result<CoverageResponse, ClientError> {
        let url = self.endpoint("coverage")?;
        self.send_json(deadline, || {
            self.authorized(self.http.post(url.clone())).json(request)
        })
        .await
    }

    pub async fn status(&self, deadline: Instant) -> Result<StatusResponse, ClientError> {
        let url = self.endpoint("status")?;
        self.send_json(deadline, || {
            self.with_revision(self.authorized(self.http.get(url.clone())))
        })
        .await
    }

    pub async fn poll(&self, job_id: Uuid, deadline: Instant) -> Result<PollResponse, ClientError> {
        let url = self.endpoint(&format!("jobs/{job_id}"))?;
        let response = self
            .send(deadline, || {
                self.with_revision(self.authorized(self.http.get(url.clone())))
            })
            .await?;
        let retry_after = retry_after(&response.headers);
        let envelope = decode_response(&response.body)?;
        Ok(PollResponse {
            envelope,
            retry_after,
        })
    }

    pub async fn cancel(
        &self,
        job_id: Uuid,
        deadline: Instant,
    ) -> Result<JobEnvelope, ClientError> {
        let url = self.endpoint(&format!("jobs/{job_id}/cancel"))?;
        self.send_json(deadline, || {
            self.with_revision(self.authorized(self.http.post(url.clone())))
        })
        .await
    }

    fn authorized(&self, request: RequestBuilder) -> RequestBuilder {
        request.bearer_auth(&self.token)
    }

    fn with_revision(&self, request: RequestBuilder) -> RequestBuilder {
        request.header(REVISION_HEADER, API_REVISION)
    }

    fn endpoint(&self, path: &str) -> Result<Url, ClientError> {
        self.base_url.join(path).map_err(|_| {
            ClientError::InvalidResponse("service URL cannot resolve a route".to_owned())
        })
    }

    async fn send_json<T, F>(&self, deadline: Instant, request: F) -> Result<T, ClientError>
    where
        T: DeserializeOwned,
        F: Fn() -> RequestBuilder,
    {
        let response = self.send(deadline, request).await?;
        decode_response(&response.body)
    }

    async fn send<F>(&self, deadline: Instant, request: F) -> Result<BufferedResponse, ClientError>
    where
        F: Fn() -> RequestBuilder,
    {
        tokio::time::timeout_at(deadline, retry_request(deadline, || request().send()))
            .await
            .map_err(|_| ClientError::DeadlineExpired)?
    }
}

pub struct PollResponse {
    pub envelope: JobEnvelope,
    pub retry_after: Option<Duration>,
}

struct BufferedResponse {
    headers: reqwest::header::HeaderMap,
    body: Vec<u8>,
}

async fn retry_request<F, Fut>(
    deadline: Instant,
    request: F,
) -> Result<BufferedResponse, ClientError>
where
    F: Fn() -> Fut,
    Fut: Future<Output = Result<reqwest::Response, reqwest::Error>>,
{
    let mut retry = RetryState::new();
    loop {
        if Instant::now() >= deadline {
            return Err(ClientError::DeadlineExpired);
        }
        match request().await {
            Ok(response) => {
                let status = response.status();
                let headers = response.headers().clone();
                let body = match response.bytes().await {
                    Ok(body) => body.to_vec(),
                    Err(error) => {
                        let delay = retry.next_delay(None);
                        eprintln!(
                            "HTTP response body was uncertain; retrying in {} ms",
                            delay.as_millis()
                        );
                        if !sleep_before(deadline, delay).await {
                            return Err(ClientError::Transport(error.to_string()));
                        }
                        continue;
                    }
                };
                if status.is_success() {
                    return Ok(BufferedResponse { headers, body });
                }
                if !matches!(
                    status,
                    StatusCode::TOO_MANY_REQUESTS | StatusCode::SERVICE_UNAVAILABLE
                ) {
                    return decode_api_error(status.as_u16(), &body);
                }
                let delay = retry.next_delay(retry_after(&headers));
                eprintln!(
                    "service returned {}; retrying in {} ms",
                    status.as_u16(),
                    delay.as_millis()
                );
                if !sleep_before(deadline, delay).await {
                    return decode_api_error(status.as_u16(), &body);
                }
            }
            Err(error) => {
                let delay = retry.next_delay(None);
                eprintln!(
                    "HTTP transport was uncertain; retrying in {} ms",
                    delay.as_millis()
                );
                if !sleep_before(deadline, delay).await {
                    return Err(ClientError::Transport(error.to_string()));
                }
            }
        }
    }
}

fn decode_response<T: DeserializeOwned>(body: &[u8]) -> Result<T, ClientError> {
    serde_json::from_slice(body).map_err(|_| {
        ClientError::InvalidResponse("response body does not match the API contract".to_owned())
    })
}

fn decode_api_error<T>(status: u16, body: &[u8]) -> Result<T, ClientError> {
    let error = serde_json::from_slice(body).map_err(|_| {
        ClientError::InvalidResponse(format!(
            "service returned HTTP {status} without a valid error"
        ))
    })?;
    Err(ClientError::Api { status, error })
}

fn retry_after(headers: &reqwest::header::HeaderMap) -> Option<Duration> {
    headers
        .get(RETRY_AFTER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
        .map(Duration::from_secs)
}
