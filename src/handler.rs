use std::sync::Arc;

use axum::Json;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use bytes::Bytes;
use futures_util::StreamExt;
use redis::AsyncCommands;
use serde::Deserialize;
use std::time::Duration;
use tokio::time::timeout;
use tracing::Instrument;
use uuid::Uuid;

use crate::config::AppState;
use crate::error::WebhookError;
use crate::model::{
    EventKind, IncomingEdit, IncomingEvent, IncomingMessage, IncomingRead, NewMessage,
    OperatorInbound, ProviderKind, Sender, WsInbound, WsOutbound,
};
use crate::provider::WebhookProvider;
use crate::provider::instagram::InstagramProvider;
use crate::provider::telegram::{self, TelegramProvider};

struct ConnectionGuard {
    entity_id: Uuid,
    conn_id: u64,
    state: Arc<AppState>,
}

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        self.state.registry.deregister(self.entity_id, self.conn_id);
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

/// Build a Sender object from caches.
async fn resolve_sender(
    sender_id: Uuid,
    sender_type: &str,
    db: &sqlx::PgPool,
    state: &AppState,
) -> Sender {
    match sender_type {
        "operator" => {
            let op = state
                .operator_cache
                .get_operator(db, sender_id)
                .await
                .ok()
                .flatten();
            Sender {
                id: sender_id,
                sender_type: "operator".into(),
                name: op.and_then(|o| o.name),
                username: None,
            }
        }
        _ => {
            let client = crate::db::find_client_by_uuid(db, sender_id)
                .await
                .ok()
                .flatten();
            Sender {
                id: sender_id,
                sender_type: "client".into(),
                name: client.as_ref().and_then(|c| c.name.clone()),
                username: client.as_ref().and_then(|c| c.username.clone()),
            }
        }
    }
}

async fn persist_and_publish(
    db: &sqlx::PgPool,
    redis: &mut redis::aio::ConnectionManager,
    msg: &NewMessage,
    chat_id_override: Option<Uuid>,
    state: &AppState,
) {
    match msg.event {
        EventKind::Message => {
            let (chat_id, insert_msg) = if let Some(cid) = chat_id_override {
                // Operator path: chat already exists
                (Some(cid), msg.clone())
            } else {
                // Client path: verify sender exists (FK safety), then find_or_create_chat
                let verified_sender = match msg.sender_id {
                    Some(id) if crate::db::find_client_by_id(db, id).await.unwrap_or(false) => {
                        Some(id)
                    }
                    _ => None,
                };

                let chat_id = match verified_sender {
                    Some(sender_id) => {
                        match crate::db::find_or_create_chat(db, sender_id, msg.channel_id).await {
                            Ok(id) => {
                                crate::cache::publish_invalidation(&mut redis.clone(), "chat", id)
                                    .await;
                                Some(id)
                            }
                            Err(e) => {
                                tracing::error!("find_or_create_chat failed: {e}");
                                None
                            }
                        }
                    }
                    None => None,
                };

                let insert_msg = if verified_sender != msg.sender_id {
                    NewMessage {
                        sender_id: verified_sender,
                        ..msg.clone()
                    }
                } else {
                    msg.clone()
                };
                (chat_id, insert_msg)
            };

            match crate::db::insert_message(db, &insert_msg, chat_id).await {
                Ok(Some(db_msg)) => {
                    let sender = resolve_sender(
                        db_msg.sender_id.unwrap_or(Uuid::nil()),
                        &db_msg.sender_type,
                        db,
                        state,
                    )
                    .await;
                    let incoming = IncomingMessage {
                        id: db_msg.id,
                        external_message_id: db_msg.external_message_id,
                        channel_id: db_msg.channel_id,
                        chat_id: db_msg.chat_id,
                        text: db_msg.text,
                        status: db_msg.status,
                        created_at: db_msg.created_at,
                        sender,
                    };
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
                Ok(Some(db_edit)) => {
                    let sender = resolve_sender(
                        db_edit.sender_id.unwrap_or(Uuid::nil()),
                        &db_edit.sender_type,
                        db,
                        state,
                    )
                    .await;
                    let edit = IncomingEdit {
                        id: db_edit.id,
                        external_message_id: db_edit.external_message_id,
                        channel_id: db_edit.channel_id,
                        chat_id: db_edit.chat_id,
                        text: db_edit.text,
                        edited_at: db_edit.edited_at,
                        sender,
                    };
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
            match crate::db::mark_messages_read(
                db,
                msg.channel_id,
                &msg.external_message_id,
                &msg.sender_type,
            )
            .await
            {
                Ok(reads) if reads.is_empty() => {
                    tracing::warn!(
                        external_message_id = %msg.external_message_id,
                        "read receipt for unknown or already-read message"
                    );
                }
                Ok(reads) => {
                    tracing::info!(count = reads.len(), "processed read receipt");
                    for db_read in &reads {
                        // Resolve sender of Read event, not sender of messages readed
                        // HACK: just switch operator and client
                        let sender_type = if msg.sender_type == "client" {
                            "operator"
                        } else {
                            "client"
                        };
                        let sender = resolve_sender(Uuid::nil(), sender_type, db, state).await;
                        let read = IncomingRead {
                            id: db_read.id,
                            external_message_id: db_read.external_message_id.clone(),
                            channel_id: db_read.channel_id,
                            chat_id: db_read.chat_id,
                            sender,
                        };
                        publish_event(redis, &IncomingEvent::Read(read)).await;
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
    state: &AppState,
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

    // Read receipts: look up target message by UUID, bypass persist_and_publish.
    // No response on success; error only on DB failure.
    if matches!(event_kind, EventKind::Read) {
        let mut redis = state.redis.clone();
        match crate::db::mark_messages_read_by_id(&state.db, inbound.mid, "client").await {
            Ok(reads) => {
                for db_read in &reads {
                    let sender = resolve_sender(
                        db_read.sender_id.unwrap_or(Uuid::nil()),
                        &db_read.sender_type,
                        &state.db,
                        state,
                    )
                    .await;
                    let read = IncomingRead {
                        id: db_read.id,
                        external_message_id: db_read.external_message_id.clone(),
                        channel_id: db_read.channel_id,
                        chat_id: db_read.chat_id,
                        sender,
                    };
                    publish_event(&mut redis, &IncomingEvent::Read(read)).await;
                }
            }
            Err(e) => {
                tracing::error!("mark_messages_read failed: {e}");
                return send_outbound(
                    socket,
                    &WsOutbound::Error {
                        reason: "read receipt failed".into(),
                    },
                )
                .await;
            }
        }
        return true;
    }

    let msg = NewMessage {
        external_message_id: format!("widget:{}", inbound.mid),
        channel_id,
        sender_id: Some(client_id),
        sender_type: "client".into(),
        provider: ProviderKind::Widget,
        event: event_kind,
        text: inbound.text.clone(),
        raw: serde_json::to_value(&inbound).unwrap_or_default(),
    };

    let mut redis = state.redis.clone();
    persist_and_publish(&state.db, &mut redis, &msg, None, state).await;

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
    state: &AppState,
) -> bool {
    loop {
        match tokio::time::timeout_at(deadline, socket.recv()).await {
            Ok(Some(Ok(Message::Pong(_)))) => return true,
            Ok(Some(Ok(Message::Text(text)))) => {
                if !process_text_message(&text, channel_id, client_id, socket, state).await {
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

#[derive(Deserialize)]
pub struct WsTokenParams {
    pub token: Option<String>,
}

/// Resolve or create a client from an optional JWT token.
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

    let client_id = crate::db::create_client(db).await?;
    let token = crate::jwt::sign(client_id, jwt_secret.as_bytes());
    Ok((client_id, Some(token)))
}

/// Resolve or create an operator from an optional JWT token.
async fn resolve_operator(
    token: Option<&str>,
    jwt_secret: &str,
    db: &sqlx::PgPool,
) -> Result<(Uuid, Option<String>), sqlx::Error> {
    if let Some(operator_id) = token.and_then(|t| crate::jwt::verify(t, jwt_secret.as_bytes()))
        && crate::db::find_operator_by_id(db, operator_id)
            .await?
            .is_some()
    {
        return Ok((operator_id, None));
    }
    let operator_id = crate::db::create_operator(db).await?;
    let token = crate::jwt::sign(operator_id, jwt_secret.as_bytes());
    Ok((operator_id, Some(token)))
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
    let (conn_id, mut receiver) = state.registry.register(client_id, false);
    let count = state.registry.connection_count();
    tracing::info!(active_connections = count, channel_id = %channel_id, %client_id, "ws connected");
    let _guard = ConnectionGuard {
        entity_id: client_id,
        conn_id,
        state: state.clone(),
    };

    // Send auth message if this is a new client
    if let Some(token) = new_token {
        let auth = WsOutbound::Auth {
            token,
            operator_id: None,
        };
        if !send_outbound(&mut socket, &auth).await {
            tracing::warn!(%channel_id, %client_id, "auth send failed, disconnecting");
            return;
        }
    }

    loop {
        tokio::select! {
            msg = receiver.recv() => {
                match msg {
                    Some(payload) => {
                        match timeout(SEND_TIMEOUT, socket.send(Message::Text(payload.into()))).await {
                            Ok(Ok(())) => {}
                            _ => {
                                tracing::info!(%client_id, "ws send failed, disconnecting");
                                break;
                            }
                        }
                    }
                    None => break,
                }
            }
            result = timeout(IDLE_TIMEOUT, socket.recv()) => {
                match result {
                    Ok(Some(Ok(Message::Text(text)))) => {
                        if !process_text_message(&text, channel_id, client_id, &mut socket, &state).await {
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
                        if timeout(SEND_TIMEOUT, socket.send(Message::Ping(vec![1].into())))
                            .await
                            .is_err()
                        {
                            tracing::info!(%channel_id, %client_id, "idle client unreachable, disconnecting");
                            break;
                        }
                        let deadline = tokio::time::Instant::now() + PING_TIMEOUT;
                        if !await_pong(&mut socket, deadline, channel_id, client_id, &state).await {
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

// ── Operator endpoints ──────────────────────────────────────────────

pub async fn get_chats(
    State(state): State<Arc<AppState>>,
) -> Result<Json<Vec<crate::db::ChatSummary>>, WebhookError> {
    let chats = crate::db::list_active_chats(&state.db).await?;
    Ok(Json(chats))
}

pub async fn get_chat_messages(
    State(state): State<Arc<AppState>>,
    Path(chat_id): Path<Uuid>,
) -> Result<Json<Vec<crate::db::ChatMessage>>, WebhookError> {
    if !crate::db::chat_exists(&state.db, chat_id).await? {
        return Err(WebhookError::NotFound("chat not found".into()));
    }
    let messages = crate::db::get_chat_messages(&state.db, chat_id).await?;
    Ok(Json(messages))
}

pub async fn operator_ws(
    State(state): State<Arc<AppState>>,
    Query(params): Query<WsTokenParams>,
    ws: WebSocketUpgrade,
) -> Result<impl IntoResponse, WebhookError> {
    let (operator_id, new_token) = resolve_operator(
        params.token.as_deref(),
        &state.config.widget_jwt_secret,
        &state.db,
    )
    .await
    .map_err(|e| {
        tracing::error!("db error resolving operator: {e}");
        WebhookError::Internal("internal server error".into())
    })?;

    Ok(ws.on_upgrade(move |socket| handle_operator_socket(socket, operator_id, new_token, state)))
}

async fn handle_operator_socket(
    mut socket: WebSocket,
    operator_id: Uuid,
    new_token: Option<String>,
    state: Arc<AppState>,
) {
    let (conn_id, mut receiver) = state.registry.register(operator_id, true);
    let count = state.registry.connection_count();
    tracing::info!(active_connections = count, %operator_id, "operator ws connected");
    let _guard = ConnectionGuard {
        entity_id: operator_id,
        conn_id,
        state: state.clone(),
    };

    // Send auth
    if let Some(token) = new_token {
        let auth = WsOutbound::Auth {
            token,
            operator_id: Some(operator_id),
        };
        if !send_outbound(&mut socket, &auth).await {
            return;
        }
    }

    loop {
        tokio::select! {
            msg = receiver.recv() => {
                match msg {
                    Some(payload) => {
                        match timeout(SEND_TIMEOUT, socket.send(Message::Text(payload.into()))).await {
                            Ok(Ok(())) => {}
                            _ => { tracing::info!(%operator_id, "operator ws: send failed"); break; }
                        }
                    }
                    None => break,
                }
            }
            result = timeout(IDLE_TIMEOUT, socket.recv()) => {
                match result {
                    Ok(Some(Ok(Message::Text(text)))) => {
                        if !process_operator_message(&text, operator_id, &mut socket, &state).await {
                            break;
                        }
                    }
                    Ok(Some(Ok(Message::Close(_)))) | Ok(None) => {
                        tracing::info!(%operator_id, "operator ws: disconnected");
                        break;
                    }
                    Ok(Some(Err(e))) => {
                        tracing::warn!(%operator_id, "operator ws: recv error: {e}");
                        break;
                    }
                    Ok(Some(Ok(Message::Pong(_)))) => continue,
                    Ok(Some(Ok(_))) => continue,
                    Err(_) => {
                        // Idle timeout — ping
                        if timeout(SEND_TIMEOUT, socket.send(Message::Ping(vec![1].into()))).await.is_err() {
                            break;
                        }
                        let deadline = tokio::time::Instant::now() + PING_TIMEOUT;
                        match tokio::time::timeout_at(deadline, socket.recv()).await {
                            Ok(Some(Ok(Message::Pong(_)))) => continue,
                            _ => break,
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
    tracing::info!(%operator_id, "operator ws disconnected");
}

async fn process_operator_message(
    text: &str,
    operator_id: Uuid,
    socket: &mut WebSocket,
    state: &AppState,
) -> bool {
    let inbound: OperatorInbound = match serde_json::from_str(text) {
        Ok(v) => v,
        Err(e) => {
            let err = WsOutbound::Error {
                reason: format!("invalid message: {e}"),
            };
            return send_outbound(socket, &err).await;
        }
    };

    // Validate chat exists
    let chat_info = match state
        .chat_cache
        .get_chat_info(&state.db, inbound.chat_id)
        .await
    {
        Ok(Some(info)) => info,
        Ok(None) => {
            let err = WsOutbound::Error {
                reason: "chat not found".into(),
            };
            return send_outbound(socket, &err).await;
        }
        Err(e) => {
            tracing::error!("chat lookup failed: {e}");
            let err = WsOutbound::Error {
                reason: "internal error".into(),
            };
            return send_outbound(socket, &err).await;
        }
    };

    let event_kind: EventKind = inbound.action.into();

    // Read receipts: look up target message by UUID, bypass persist_and_publish
    if matches!(event_kind, EventKind::Read) {
        let message_id = match Uuid::parse_str(&inbound.mid) {
            Ok(id) => id,
            Err(_) => {
                return send_outbound(
                    socket,
                    &WsOutbound::Error {
                        reason: "invalid mid for read".into(),
                    },
                )
                .await;
            }
        };
        let mut redis = state.redis.clone();
        match crate::db::mark_messages_read_by_id(&state.db, message_id, "operator").await {
            Ok(reads) => {
                for db_read in &reads {
                    let sender = resolve_sender(
                        db_read.sender_id.unwrap_or(Uuid::nil()),
                        &db_read.sender_type,
                        &state.db,
                        state,
                    )
                    .await;
                    let read = IncomingRead {
                        id: db_read.id,
                        external_message_id: db_read.external_message_id.clone(),
                        channel_id: db_read.channel_id,
                        chat_id: db_read.chat_id,
                        sender,
                    };
                    publish_event(&mut redis, &IncomingEvent::Read(read)).await;
                }
            }
            Err(e) => {
                tracing::error!("mark_messages_read failed: {e}");
                return send_outbound(
                    socket,
                    &WsOutbound::Error {
                        reason: "read receipt failed".into(),
                    },
                )
                .await;
            }
        }
        return true;
    }

    let msg = NewMessage {
        external_message_id: format!("operator:{}", inbound.mid),
        channel_id: chat_info.channel_id,
        sender_id: Some(operator_id),
        sender_type: "operator".into(),
        provider: ProviderKind::Widget,
        event: event_kind,
        text: inbound.text.clone(),
        raw: serde_json::to_value(&inbound).unwrap_or_default(),
    };

    let mut redis = state.redis.clone();
    persist_and_publish(&state.db, &mut redis, &msg, Some(inbound.chat_id), state).await;

    // Outbound delivery stub for non-widget channels
    if matches!(event_kind, EventKind::Message) {
        let channel_provider =
            crate::db::find_channel_provider(&state.db, chat_info.channel_id).await;
        match channel_provider.as_ref().map(|o| o.as_deref()) {
            Ok(Some("instagram")) => {
                tracing::info!(
                    channel_id = %chat_info.channel_id,
                    mid = %inbound.mid,
                    "TODO: deliver to Instagram API"
                );
            }
            Ok(Some("telegram")) => {
                tracing::info!(
                    channel_id = %chat_info.channel_id,
                    mid = %inbound.mid,
                    "TODO: deliver to Telegram API"
                );
            }
            _ => {} // widget — delivered via shared listener
        }
    }

    // Ack
    let ack_id = Uuid::parse_str(&inbound.mid).unwrap_or(Uuid::nil());
    let ack = WsOutbound::Ack { message_id: ack_id };
    send_outbound(socket, &ack).await
}

// ── Shared Redis listener ───────────────────────────────────────────

/// Spawn the shared Redis listener that dispatches events to all connected
/// operators and the relevant client for each chat.
pub async fn spawn_message_listener(state: Arc<AppState>) {
    let client = redis::Client::open(state.config.redis_url.as_str())
        .expect("invalid REDIS_URL for message listener");
    let mut pubsub = client
        .get_async_pubsub()
        .await
        .expect("failed to create Redis pubsub for message listener");
    pubsub
        .subscribe(REDIS_CHANNEL)
        .await
        .expect("failed to subscribe to incoming_messages");

    let shutdown = state.shutdown.clone();
    tokio::spawn(async move {
        tracing::info!("shared message listener started");

        let mut msg_stream = pubsub.into_on_message();
        loop {
            let msg = tokio::select! {
                msg = msg_stream.next() => match msg {
                    Some(m) => m,
                    None => break,
                },
                _ = shutdown.cancelled() => {
                    tracing::info!("shared message listener shutting down");
                    break;
                }
            };
            let payload: String = match msg.get_payload() {
                Ok(p) => p,
                Err(e) => {
                    tracing::warn!("message listener: bad payload: {e}");
                    continue;
                }
            };

            let event: IncomingEvent = match serde_json::from_str(&payload) {
                Ok(v) => v,
                Err(e) => {
                    tracing::warn!("message listener: invalid JSON: {e}");
                    continue;
                }
            };

            let sender_id = event.sender_id();
            let Some(chat_id) = event.chat_id() else {
                tracing::warn!("message listener: missing chat id");
                continue;
            };
            let client_id =
                if let Ok(Some(info)) = state.chat_cache.get_chat_info(&state.db, chat_id).await {
                    info.client_id
                } else {
                    tracing::warn!("message listener: chat not found for chat_id {chat_id}");
                    continue;
                };

            tracing::info!(event = ?event, "message listener received event");

            if let IncomingEvent::Read(read) = &event {
                tracing::info!(
                    "message listener: read receipt for message_id {:?} in chat_id {:?}",
                    read.external_message_id,
                    read.chat_id
                );
                if sender_id == client_id {
                    state.registry.send_to(client_id, &payload);
                } else {
                    for op_id in state.registry.operator_ids() {
                        state.registry.send_to(op_id, &payload);
                    }
                }
            } else {
                // Deliver to the client for this chat
                if sender_id != client_id {
                    state.registry.send_to(client_id, &payload);
                }

                // Deliver to all connected operators (skip sender)
                for op_id in state.registry.operator_ids() {
                    if sender_id != op_id {
                        state.registry.send_to(op_id, &payload);
                    }
                }
            }
        }
        tracing::warn!("shared message listener ended");
    });
}
