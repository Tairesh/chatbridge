use std::sync::Arc;

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use bytes::Bytes;
use redis::AsyncCommands;
use serde::Deserialize;
use uuid::Uuid;

use crate::config::AppState;
use crate::db;
use crate::error::WebhookError;
use crate::model::{EventKind, InternalMessage, ProviderKind, WsAck, WsError, WsInbound};
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
    let mut redis = state.redis.clone();
    let body = body.to_vec();
    tokio::spawn(async move {
        match provider.parse(&body, &db).await {
            Ok(messages) => {
                for msg in &messages {
                    tracing::info!(
                        message_id = %msg.message_id,
                        channel_id = %msg.channel_id,
                        event = ?msg.event,
                        raw_data = ?msg.raw,
                        "processed instagram event"
                    );
                    if let Ok(payload) = serde_json::to_string(msg) {
                        let channel = format!("instagram:{}", msg.channel_id);
                        if let Err(e) = redis.publish::<_, _, ()>(&channel, &payload).await {
                            tracing::error!("redis publish failed: {e}");
                        }
                    }
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
    let mut redis = state.redis.clone();
    let body = body.to_vec();
    tokio::spawn(async move {
        match provider.parse(&body, &db).await {
            Ok(messages) => {
                for msg in &messages {
                    tracing::info!(
                        message_id = %msg.message_id,
                        channel_id = %msg.channel_id,
                        event = ?msg.event,
                        raw_data = ?msg.raw,
                        "processed telegram event"
                    );
                    if let Ok(payload) = serde_json::to_string(msg) {
                        let channel = format!("telegram:{}", msg.channel_id);
                        if let Err(e) = redis.publish::<_, _, ()>(&channel, &payload).await {
                            tracing::error!("redis publish failed: {e}");
                        }
                    }
                }
            }
            Err(e) => tracing::error!("telegram parse failed: {e}"),
        }
    });

    Ok(StatusCode::OK)
}

pub async fn widget_ws(
    State(state): State<Arc<AppState>>,
    Path(widget_id): Path<String>,
    ws: WebSocketUpgrade,
) -> impl IntoResponse {
    let channel = match db::find_widget_channel_by_widget_id(&state.db, &widget_id).await {
        Ok(Some(ch)) => ch,
        Ok(None) => {
            tracing::warn!(widget_id = %widget_id, "unknown widget_id");
            return StatusCode::NOT_FOUND.into_response();
        }
        Err(e) => {
            tracing::error!("db error looking up widget: {e}");
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    };

    tracing::info!(widget_id = %widget_id, channel_id = %channel.id, "websocket upgrade");
    ws.on_upgrade(move |socket| handle_widget_socket(socket, channel.id, state))
}

async fn handle_widget_socket(mut socket: WebSocket, channel_id: Uuid, state: Arc<AppState>) {
    while let Some(msg) = socket.recv().await {
        let msg = match msg {
            Ok(Message::Text(text)) => text,
            Ok(Message::Close(_)) => {
                tracing::info!(channel_id = %channel_id, "client disconnected");
                break;
            }
            Ok(_) => continue,
            Err(e) => {
                tracing::warn!(channel_id = %channel_id, "ws recv error: {e}");
                break;
            }
        };

        let inbound: WsInbound = match serde_json::from_str(&msg) {
            Ok(v) => v,
            Err(e) => {
                let err = WsError {
                    status: "error",
                    reason: format!("invalid message: {e}"),
                };
                let _ = socket
                    .send(Message::Text(serde_json::to_string(&err).unwrap().into()))
                    .await;
                tracing::warn!(
                    channel_id = %channel_id,
                    raw = %msg,
                    "failed to parse inbound message: {e}"
                );
                continue;
            }
        };

        let event = match inbound.action.as_str() {
            "send" => EventKind::Message,
            "edit" => EventKind::Edit,
            other => {
                let err = WsError {
                    status: "error",
                    reason: format!("unknown action: {other}"),
                };
                let _ = socket
                    .send(Message::Text(serde_json::to_string(&err).unwrap().into()))
                    .await;
                continue;
            }
        };

        let now = chrono::Utc::now().timestamp();

        let internal = InternalMessage {
            message_id: format!("widget:{}", inbound.mid),
            channel_id,
            provider: ProviderKind::Widget,
            event,
            timestamp: now,
            raw: serde_json::to_value(&inbound).unwrap_or_default(),
        };

        tracing::info!(
            message_id = %internal.message_id,
            channel_id = %channel_id,
            event = ?internal.event,
            raw_data = ?internal.raw,
            "processed widget event"
        );

        // Publish to Redis for cross-replica routing
        let redis_channel = format!("widget:{channel_id}");
        if let Ok(payload) = serde_json::to_string(&internal) {
            let mut redis = state.redis.clone();
            if let Err(e) = redis.publish::<_, _, ()>(&redis_channel, &payload).await {
                tracing::error!("redis publish failed: {e}");
            }
        }

        let ack = WsAck {
            status: "ok",
            message_id: inbound.mid,
        };
        if socket
            .send(Message::Text(serde_json::to_string(&ack).unwrap().into()))
            .await
            .is_err()
        {
            break;
        }
    }
}
