use std::sync::Arc;

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use bytes::Bytes;
use redis::AsyncCommands;
use serde::Deserialize;
use std::time::Duration;
use tokio::time::timeout;
use tracing::Instrument;
use uuid::Uuid;

use crate::config::AppState;
use crate::error::WebhookError;
use crate::model::{InternalMessage, ProviderKind, WsInbound, WsOutbound};
use crate::provider::WebhookProvider;
use crate::provider::instagram::InstagramProvider;
use crate::provider::telegram::{self, TelegramProvider};

struct ConnectionGuard {
    client_id: Uuid,
    conn_id: u64,
    state: Arc<AppState>,
}

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        self.state.registry.deregister(self.client_id, self.conn_id);
        let count = self.state.registry.connection_count();
        tracing::info!(active_connections = count, "ws disconnected");
    }
}

const SEND_TIMEOUT: Duration = Duration::from_secs(5);
const IDLE_TIMEOUT: Duration = Duration::from_secs(300);
const PING_TIMEOUT: Duration = Duration::from_secs(10);

/// Send a WsOutbound message to the socket. Returns false if the send fails.
async fn send_outbound(socket: &mut WebSocket, msg: &WsOutbound) -> bool {
    let text = serde_json::to_string(msg).expect("WsOutbound serialization cannot fail");
    matches!(
        timeout(SEND_TIMEOUT, socket.send(Message::Text(text.into()))).await,
        Ok(Ok(()))
    )
}

async fn process_text_message(
    text: &str,
    channel_id: Uuid,
    client_id: Uuid,
    socket: &mut WebSocket,
    redis: &mut redis::aio::ConnectionManager,
) -> bool {
    let inbound: WsInbound = match serde_json::from_str(text) {
        Ok(v) => v,
        Err(e) => {
            let err = WsOutbound::Error {
                reason: format!("invalid message: {e}"),
            };
            if !send_outbound(socket, &err).await {
                tracing::warn!(channel_id = %channel_id, "error send failed, disconnecting");
                return false;
            }
            tracing::warn!(
                channel_id = %channel_id,
                raw = %text,
                "failed to parse inbound message: {e}"
            );
            return true; // keep connection alive
        }
    };

    let now = chrono::Utc::now().timestamp();

    let internal = InternalMessage {
        message_id: format!("widget:{}", inbound.mid),
        channel_id,
        client_id: Some(client_id),
        provider: ProviderKind::Widget,
        event: inbound.action.into(),
        timestamp: now,
        raw: serde_json::to_value(&inbound).unwrap_or_default(),
    };

    tracing::info!(
        message_id = %internal.message_id,
        channel_id = %channel_id,
        client_id = %client_id,
        event = ?internal.event,
        raw_data = ?internal.raw,
        "processed widget event"
    );

    let redis_channel = format!("widget:{channel_id}");
    let payload =
        serde_json::to_string(&internal).expect("InternalMessage serialization cannot fail");
    if let Err(e) = redis.publish::<_, _, ()>(&redis_channel, &payload).await {
        tracing::error!("redis publish failed: {e}");
    }

    let ack = WsOutbound::Ack {
        message_id: inbound.mid,
    };
    if !send_outbound(socket, &ack).await {
        tracing::warn!(channel_id = %channel_id, "ack send failed, disconnecting");
        return false;
    }

    true // keep connection alive
}

async fn await_pong(
    socket: &mut WebSocket,
    deadline: tokio::time::Instant,
    channel_id: Uuid,
    client_id: Uuid,
    redis: &mut redis::aio::ConnectionManager,
) -> bool {
    loop {
        match tokio::time::timeout_at(deadline, socket.recv()).await {
            Ok(Some(Ok(Message::Pong(_)))) => return true,
            Ok(Some(Ok(Message::Text(text)))) => {
                if !process_text_message(&text, channel_id, client_id, socket, redis).await {
                    return false;
                }
            }
            Ok(Some(Ok(Message::Close(_)))) => return false,
            Ok(Some(Ok(_))) => continue,
            Ok(Some(Err(_))) | Ok(None) | Err(_) => return false,
        }
    }
}

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

    // Return 200 immediately, process in background
    let db = state.db.clone();
    let mut redis = state.redis.clone();
    let body = body.to_vec();
    tokio::spawn(
        async move {
            match provider.parse(&body, &db, redis.clone()).await {
                Ok(messages) => {
                    for msg in &messages {
                        tracing::info!(
                            message_id = %msg.message_id,
                            channel_id = %msg.channel_id,
                            client_id = ?msg.client_id,
                            event = ?msg.event,
                            raw_data = ?msg.raw,
                            "processed instagram event"
                        );
                        let payload = serde_json::to_string(msg)
                            .expect("InternalMessage serialization cannot fail");
                        let channel = format!("instagram:{}", msg.channel_id);
                        if let Err(e) = redis.publish::<_, _, ()>(&channel, &payload).await {
                            tracing::error!("redis publish failed: {e}");
                        }
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

    // Return 200 immediately, process in background
    let db = state.db.clone();
    let mut redis = state.redis.clone();
    let body = body.to_vec();
    tokio::spawn(
        async move {
            match provider.parse(&body, &db, redis.clone()).await {
                Ok(messages) => {
                    for msg in &messages {
                        tracing::info!(
                            message_id = %msg.message_id,
                            channel_id = %msg.channel_id,
                            client_id = ?msg.client_id,
                            event = ?msg.event,
                            raw_data = ?msg.raw,
                            "processed telegram event"
                        );
                        let payload = serde_json::to_string(msg)
                            .expect("InternalMessage serialization cannot fail");
                        let channel = format!("telegram:{}", msg.channel_id);
                        if let Err(e) = redis.publish::<_, _, ()>(&channel, &payload).await {
                            tracing::error!("redis publish failed: {e}");
                        }
                    }
                }
                Err(e) => tracing::error!("telegram parse failed: {e}"),
            }
        }
        .instrument(tracing::info_span!("telegram_bg")),
    );

    Ok(StatusCode::OK)
}

#[derive(Deserialize)]
pub struct WsTokenParams {
    pub token: Option<String>,
}

/// Resolve or create a client from an optional JWT token.
/// Returns the client_id and the JWT to send to the client (only if newly created or re-issued).
async fn resolve_client(
    token: Option<&str>,
    jwt_secret: &str,
    db: &sqlx::PgPool,
) -> Result<(Uuid, Option<String>), sqlx::Error> {
    if let Some(client_id) = token.and_then(|t| crate::jwt::verify(t, jwt_secret.as_bytes()))
        && crate::db::find_client_by_id(db, client_id).await?
    {
        return Ok((client_id, None));
    }

    // Create new client
    let client_id = crate::db::create_client(db).await?;
    let token = crate::jwt::sign(client_id, jwt_secret.as_bytes());
    Ok((client_id, Some(token)))
}

pub async fn widget_ws(
    State(state): State<Arc<AppState>>,
    Path(widget_id): Path<String>,
    Query(params): Query<WsTokenParams>,
    ws: WebSocketUpgrade,
) -> impl IntoResponse {
    let channel = match state.cache.get_widget_channel(&state.db, &widget_id).await {
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

    let (client_id, new_token) = match resolve_client(
        params.token.as_deref(),
        &state.config.widget_jwt_secret,
        &state.db,
    )
    .await
    {
        Ok(result) => result,
        Err(e) => {
            tracing::error!("db error resolving client: {e}");
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    };

    tracing::info!(
        widget_id = %widget_id,
        channel_id = %channel.id,
        %client_id,
        returning_client = new_token.is_none(),
        "websocket upgrade"
    );
    ws.on_upgrade(move |socket| {
        handle_widget_socket(socket, channel.id, client_id, new_token, state)
    })
}

async fn handle_widget_socket(
    mut socket: WebSocket,
    channel_id: Uuid,
    client_id: Uuid,
    new_token: Option<String>,
    state: Arc<AppState>,
) {
    let conn_id = state.registry.register(client_id);
    let count = state.registry.connection_count();
    tracing::info!(active_connections = count, channel_id = %channel_id, %client_id, "ws connected");
    let _guard = ConnectionGuard {
        client_id,
        conn_id,
        state: state.clone(),
    };

    // Send auth message if this is a new client
    if let Some(token) = new_token {
        let auth = WsOutbound::Auth { token };
        if !send_outbound(&mut socket, &auth).await {
            tracing::warn!(%channel_id, %client_id, "auth send failed, disconnecting");
            return;
        }
    }

    let mut redis = state.redis.clone();

    loop {
        tokio::select! {
            result = timeout(IDLE_TIMEOUT, socket.recv()) => {
                match result {
                    Ok(Some(Ok(Message::Text(text)))) => {
                        if !process_text_message(&text, channel_id, client_id, &mut socket, &mut redis).await {
                            break;
                        }
                    }
                    Ok(Some(Ok(Message::Close(_)))) => {
                        tracing::info!(%client_id, "client disconnected");
                        break;
                    }
                    Ok(Some(Ok(Message::Pong(_)))) => continue,
                    Ok(Some(Err(e))) => {
                        tracing::warn!(%channel_id, %client_id, "ws recv error: {e}");
                        break;
                    }
                    Ok(None) => break,
                    Ok(Some(Ok(_))) => continue,
                    Err(_) => {
                        // Idle timeout — ping to check if client is alive
                        if timeout(SEND_TIMEOUT, socket.send(Message::Ping(vec![1].into())))
                            .await
                            .is_err()
                        {
                            tracing::info!(%channel_id, %client_id, "idle client unreachable, disconnecting");
                            break;
                        }
                        let deadline = tokio::time::Instant::now() + PING_TIMEOUT;
                        if !await_pong(&mut socket, deadline, channel_id, client_id, &mut redis).await {
                            tracing::info!(%channel_id, %client_id, "idle timeout, disconnecting");
                            break;
                        }
                    }
                }
            }
            _ = state.shutdown.cancelled() => {
                let _ = timeout(SEND_TIMEOUT, socket.send(Message::Close(None))).await;
                break;
            }
        }
    }
}
