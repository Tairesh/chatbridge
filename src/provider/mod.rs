use axum::http::HeaderMap;
use sqlx::PgPool;

use crate::error::WebhookError;
use crate::model::IncomingMessage;

pub mod instagram;
pub mod telegram;

pub trait WebhookProvider: Send + Sync {
    fn verify(&self, headers: &HeaderMap, body: &[u8]) -> Result<(), WebhookError>;

    fn parse(
        &self,
        body: &[u8],
        db: &PgPool,
        redis: redis::aio::ConnectionManager,
    ) -> impl std::future::Future<Output = Result<Vec<IncomingMessage>, WebhookError>> + Send;
}
