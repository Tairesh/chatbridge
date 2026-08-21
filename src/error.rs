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

    /// 409 with a machine-readable JSON body. The settings panel needs to tell
    /// "already exists" from "exists but deleted" and to read the conflicting
    /// channel's id, which a flat string body cannot carry.
    #[error("conflict")]
    Conflict(serde_json::Value),

    /// 502 — an upstream provider (Telegram) failed or was unreachable.
    #[error("bad gateway: {0}")]
    BadGateway(String),

    #[error("internal: {0}")]
    Internal(String),
}

impl IntoResponse for WebhookError {
    fn into_response(self) -> axum::response::Response {
        // Conflict is the only variant with a structured body.
        if let WebhookError::Conflict(body) = self {
            return (StatusCode::CONFLICT, axum::Json(body)).into_response();
        }

        let status = match &self {
            WebhookError::Forbidden(_) => StatusCode::FORBIDDEN,
            WebhookError::BadRequest(_) => StatusCode::BAD_REQUEST,
            WebhookError::NotFound(_) => StatusCode::NOT_FOUND,
            WebhookError::BadGateway(_) => StatusCode::BAD_GATEWAY,
            WebhookError::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
            WebhookError::Conflict(_) => unreachable!("handled above"),
        };
        (status, self.to_string()).into_response()
    }
}

impl From<sqlx::Error> for WebhookError {
    fn from(e: sqlx::Error) -> Self {
        tracing::error!("database error: {e}");
        WebhookError::Internal("internal server error".into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::to_bytes;

    #[tokio::test]
    async fn conflict_responds_409_with_a_json_body() {
        let err = WebhookError::Conflict(serde_json::json!({
            "error": "channel_deleted",
            "channel_id": "550e8400-e29b-41d4-a716-446655440000"
        }));
        let resp = err.into_response();
        assert_eq!(resp.status(), StatusCode::CONFLICT);

        let body = to_bytes(resp.into_body(), 4096).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["error"], "channel_deleted");
        assert_eq!(json["channel_id"], "550e8400-e29b-41d4-a716-446655440000");
    }

    #[test]
    fn bad_gateway_responds_502() {
        let err = WebhookError::BadGateway("setWebhook failed: timeout".into());
        assert_eq!(err.into_response().status(), StatusCode::BAD_GATEWAY);
    }
}
