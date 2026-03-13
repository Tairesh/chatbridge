use axum::http::StatusCode;
use axum::response::IntoResponse;

#[derive(Debug, thiserror::Error)]
pub enum WebhookError {
    #[error("forbidden: {0}")]
    Forbidden(String),

    #[error("bad request: {0}")]
    BadRequest(String),

    #[error("not found: {0}")]
    NotFound(String),

    #[error("internal: {0}")]
    Internal(String),
}

impl IntoResponse for WebhookError {
    fn into_response(self) -> axum::response::Response {
        let status = match &self {
            WebhookError::Forbidden(_) => StatusCode::FORBIDDEN,
            WebhookError::BadRequest(_) => StatusCode::BAD_REQUEST,
            WebhookError::NotFound(_) => StatusCode::NOT_FOUND,
            WebhookError::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
        };
        (status, self.to_string()).into_response()
    }
}

impl From<sqlx::Error> for WebhookError {
    fn from(e: sqlx::Error) -> Self {
        WebhookError::Internal(e.to_string())
    }
}
