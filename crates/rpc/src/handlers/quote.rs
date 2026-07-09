use axum::{extract::State, Json};
use runtime::services::QuoteService;

use crate::models::messages::{AmountOutRequest, QuoteResult};

pub async fn simulate(
    State(quote_service): State<QuoteService>,
    Json(request): Json<AmountOutRequest>,
) -> Json<QuoteResult> {
    Json(quote_service.quote(request).await)
}
