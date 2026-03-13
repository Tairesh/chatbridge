use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use bytes::Bytes;
use serde::Deserialize;
use uuid::Uuid;

use crate::config::AppState;
use crate::error::WebhookError;
use crate::provider::WebhookProvider;
use crate::provider::instagram::InstagramProvider;
use crate::provider::telegram::{self, TelegramProvider};

#[derive(Deserialize)]
pub struct VerifyParams {
    #[serde(rename = "hub.mode")]
    hub_mode: String,
    #[serde(rename = "hub.challenge")]
    hub_challenge: String,
    #[serde(rename = "hub.verify_token")]
    hub_verify_token: String,
}

pub async fn meta_verify(
    State(state): State<Arc<AppState>>,
    Query(params): Query<VerifyParams>,
) -> Result<String, WebhookError> {
    if params.hub_mode == "subscribe" && params.hub_verify_token == state.config.meta_verify_token {
        tracing::info!("webhook verified, returning challenge");
        Ok(params.hub_challenge)
    } else {
        Err(WebhookError::Forbidden(
            "invalid verify token or mode".into(),
        ))
    }
}

pub async fn instagram_ingest(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> StatusCode {
    let provider = InstagramProvider::new(&state.config.instagram_app_secret);

    if let Err(e) = provider.verify(&headers, &body) {
        tracing::warn!("instagram verify failed: {e}");
        return StatusCode::FORBIDDEN;
    }

    // Return 200 immediately, process in background
    let db = state.db.clone();
    let body = body.to_vec();
    tokio::spawn(async move {
        match provider.parse(&body, &db).await {
            Ok(messages) => {
                for msg in &messages {
                    tracing::info!(
                        message_id = %msg.message_id,
                        channel_id = %msg.channel_id,
                        provider = %msg.provider,
                        event = ?msg.event,
                        "processed instagram event"
                    );
                }
            }
            Err(e) => tracing::error!("instagram parse failed: {e}"),
        }
    });

    StatusCode::OK
}

pub async fn telegram_ingest(
    State(state): State<Arc<AppState>>,
    Path(channel_id): Path<Uuid>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<StatusCode, WebhookError> {
    let (provider, bot_secret) = TelegramProvider::load(channel_id, &state.db).await?;

    telegram::verify_secret_token(&headers, &bot_secret)?;

    // Return 200 immediately, process in background
    let db = state.db.clone();
    let body = body.to_vec();
    tokio::spawn(async move {
        match provider.parse(&body, &db).await {
            Ok(messages) => {
                for msg in &messages {
                    tracing::info!(
                        message_id = %msg.message_id,
                        channel_id = %msg.channel_id,
                        provider = %msg.provider,
                        event = ?msg.event,
                        "processed telegram event"
                    );
                }
            }
            Err(e) => tracing::error!("telegram parse failed: {e}"),
        }
    });

    Ok(StatusCode::OK)
}
