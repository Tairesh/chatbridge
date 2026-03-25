use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use bytes::Bytes;
use serde::Deserialize;
use tracing::Instrument;
use uuid::Uuid;

use crate::config::AppState;
use crate::error::WebhookError;
use crate::pipeline::persist_and_publish;
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
    let provider = InstagramProvider::new(
        &state.config.instagram_app_secret,
        state.cache.clone(),
        state.client_cache.clone(),
    );

    if let Err(e) = provider.verify(&headers, &body) {
        tracing::warn!("instagram verify failed: {e}");
        return StatusCode::FORBIDDEN;
    }

    let state = state.clone();
    let body = body.to_vec();
    tokio::spawn(
        async move {
            match provider.parse(&body, &state.db, state.redis.clone()).await {
                Ok(messages) => {
                    let mut redis = state.redis.clone();
                    for msg in &messages {
                        persist_and_publish(&state.db, &mut redis, msg, None, &state).await;
                    }
                }
                Err(e) => tracing::error!("instagram parse failed: {e}"),
            }
        }
        .instrument(tracing::info_span!("instagram_bg")),
    );

    StatusCode::OK
}

pub async fn telegram_ingest(
    State(state): State<Arc<AppState>>,
    Path(channel_id): Path<Uuid>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<StatusCode, WebhookError> {
    let (provider, bot_secret) = TelegramProvider::load(
        channel_id,
        &state.db,
        &state.cache,
        state.client_cache.clone(),
    )
    .await?;

    telegram::verify_secret_token(&headers, &bot_secret)?;

    let state = state.clone();
    let body = body.to_vec();
    tokio::spawn(
        async move {
            match provider.parse(&body, &state.db, state.redis.clone()).await {
                Ok(messages) => {
                    let mut redis = state.redis.clone();
                    for msg in &messages {
                        persist_and_publish(&state.db, &mut redis, msg, None, &state).await;
                    }
                }
                Err(e) => tracing::error!("telegram parse failed: {e}"),
            }
        }
        .instrument(tracing::info_span!("telegram_bg")),
    );

    Ok(StatusCode::OK)
}
