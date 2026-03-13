use axum::http::HeaderMap;
use sqlx::PgPool;

use crate::error::WebhookError;
use crate::model::InternalMessage;

pub mod instagram;
pub mod telegram;

pub trait WebhookProvider: Send + Sync {
    fn verify(&self, headers: &HeaderMap, body: &[u8]) -> Result<(), WebhookError>;

    fn parse(
        &self,
        body: &[u8],
        db: &PgPool,
    ) -> impl std::future::Future<Output = Result<Vec<InternalMessage>, WebhookError>> + Send;
}
