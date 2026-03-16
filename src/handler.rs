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
use crate::model::{EventKind, IncomingEvent, NewMessage, ProviderKind, WsInbound, WsOutbound};
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
const REDIS_CHANNEL: &str = "incoming_messages";

async fn publish_event(redis: &mut redis::aio::ConnectionManager, event: &IncomingEvent) {
    let payload = serde_json::to_string(event).expect("IncomingEvent serialization cannot fail");
    if let Err(e) = redis.publish::<_, _, ()>(REDIS_CHANNEL, &payload).await {
        tracing::error!("redis publish failed: {e}");
    }
}

async fn persist_and_publish(
    db: &sqlx::PgPool,
    redis: &mut redis::aio::ConnectionManager,
    msg: &NewMessage,
) {
    match msg.event {
        EventKind::Message => {
            // Verify the client exists before using sender_id for FK-constrained inserts.
            // Instagram/Telegram resolve client_id optimistically before the async upsert completes.
            let verified_sender = match msg.sender_id {
                Some(id) if crate::db::find_client_by_id(db, id).await.unwrap_or(false) => Some(id),
                _ => None,
            };

            let chat_id = match verified_sender {
                Some(sender_id) => {
                    match crate::db::find_or_create_chat(db, sender_id, msg.channel_id).await {
                        Ok(id) => Some(id),
                        Err(e) => {
                            tracing::error!("find_or_create_chat failed: {e}");
                            None
                        }
                    }
                }
                None => None,
            };

            let insert_msg = if verified_sender != msg.sender_id {
                &NewMessage {
                    sender_id: verified_sender,
                    ..msg.clone()
                }
            } else {
                msg
            };

            match crate::db::insert_message(db, insert_msg, chat_id).await {
                Ok(Some(incoming)) => {
                    tracing::info!(message = ?incoming, "processed incoming message");
                    publish_event(redis, &IncomingEvent::Message(incoming)).await;
                }
                Ok(None) => {
                    tracing::debug!(
                        external_message_id = %msg.external_message_id,
                        "duplicate message, skipping"
                    );
                }
                Err(e) => {
                    tracing::error!("insert_message failed: {e}");
                }
            }
        }
        EventKind::Edit => {
            match crate::db::edit_message(
                db,
                msg.channel_id,
                &msg.external_message_id,
                msg.text.as_deref(),
            )
            .await
            {
                Ok(Some(edit)) => {
                    tracing::info!(edit = ?edit, "processed edit");
                    publish_event(redis, &IncomingEvent::Edit(edit)).await;
                }
                Ok(None) => {
                    tracing::warn!(
                        external_message_id = %msg.external_message_id,
                        "edit for unknown message, dropping"
                    );
                }
                Err(e) => {
                    tracing::error!("edit_message failed: {e}");
                }
            }
        }
        EventKind::Read => {
            match crate::db::mark_messages_read(db, msg.channel_id, &msg.external_message_id).await
            {
                Ok(reads) if reads.is_empty() => {
                    tracing::warn!(
                        external_message_id = %msg.external_message_id,
                        "read receipt for unknown or already-read message"
                    );
                }
                Ok(reads) => {
                    tracing::info!(count = reads.len(), "processed read receipt");
                    for read in &reads {
                        publish_event(redis, &IncomingEvent::Read(read.clone())).await;
                    }
                }
                Err(e) => {
                    tracing::error!("mark_messages_read failed: {e}");
                }
            }
        }
        EventKind::Reaction | EventKind::Unknown => {
            tracing::info!(
                event = %msg.event,
                external_message_id = %msg.external_message_id,
                "event logged (not persisted)"
            );
        }
    }
}

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
    db: &sqlx::PgPool,
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

    let event_kind: EventKind = inbound.action.into();
    let text = match event_kind {
        EventKind::Message | EventKind::Edit => Some(inbound.text.clone()),
        _ => None,
    };

    let msg = NewMessage {
        external_message_id: format!("widget:{}", inbound.mid),
        channel_id,
        sender_id: Some(client_id),
        provider: ProviderKind::Widget,
        event: event_kind,
        text,
        raw: serde_json::to_value(&inbound).unwrap_or_default(),
    };

    persist_and_publish(db, redis, &msg).await;

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
    db: &sqlx::PgPool,
    redis: &mut redis::aio::ConnectionManager,
) -> bool {
    loop {
        match tokio::time::timeout_at(deadline, socket.recv()).await {
            Ok(Some(Ok(Message::Pong(_)))) => return true,
            Ok(Some(Ok(Message::Text(text)))) => {
                if !process_text_message(&text, channel_id, client_id, socket, db, redis).await {
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
                        persist_and_publish(&db, &mut redis, msg).await;
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
                        persist_and_publish(&db, &mut redis, msg).await;
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
                        if !process_text_message(&text, channel_id, client_id, &mut socket, &state.db, &mut redis).await {
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
                        if !await_pong(&mut socket, deadline, channel_id, client_id, &state.db, &mut redis).await {
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
