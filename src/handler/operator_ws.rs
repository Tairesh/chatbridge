use std::sync::Arc;

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Query, State};
use axum::response::IntoResponse;
use tokio::time::timeout;
use uuid::Uuid;

use crate::config::AppState;
use crate::error::AppError;
use crate::model::{
    EventKind, IncomingEvent, IncomingRead, NewMessage, OperatorInbound, ProviderKind, WsOutbound,
};
use crate::pipeline::{persist_and_publish, publish_event, resolve_sender};

use super::{
    ConnectionGuard, IDLE_TIMEOUT, PING_TIMEOUT, SEND_TIMEOUT, WsTokenParams, resolve_operator,
    send_outbound,
};

pub async fn operator_ws(
    State(state): State<Arc<AppState>>,
    Query(params): Query<WsTokenParams>,
    ws: WebSocketUpgrade,
) -> Result<impl IntoResponse, AppError> {
    let (operator_id, new_token) = resolve_operator(
        params.token.as_deref(),
        &state.config.widget_jwt_secret,
        &state,
    )
    .await
    .map_err(|e| {
        tracing::error!("db error resolving operator: {e}");
        AppError::Internal("internal server error".into())
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
    state: &Arc<AppState>,
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
                if let Some(text) = inbound.text.clone().filter(|t| !t.is_empty()) {
                    spawn_telegram_delivery(
                        Arc::clone(state),
                        chat_info.channel_id,
                        chat_info.client_id,
                        operator_id,
                        text,
                    );
                }
            }
            _ => {} // widget — delivered via shared listener
        }
    }

    // Ack
    let ack_id = Uuid::parse_str(&inbound.mid).unwrap_or(Uuid::nil());
    let ack = WsOutbound::Ack { message_id: ack_id };
    send_outbound(socket, &ack).await
}

fn notify_operator_error(state: &AppState, operator_id: Uuid, reason: String) {
    let error = WsOutbound::Error { reason };
    if let Ok(json) = serde_json::to_string(&error) {
        state.registry.send_to(operator_id, &json);
    }
}

async fn deliver_to_telegram(
    state: &AppState,
    channel_id: Uuid,
    client_id: Uuid,
    text: &str,
) -> Result<(), String> {
    let channel = state
        .cache
        .get_channel_by_id(&state.db, channel_id)
        .await
        .map_err(|e| format!("channel lookup failed: {e}"))?
        .ok_or_else(|| "telegram channel not found".to_owned())?;

    let config: crate::model::TelegramConfig =
        serde_json::from_value(channel.config).map_err(|e| format!("bad telegram config: {e}"))?;

    let client = state
        .client_cache
        .get_client_by_uuid(&state.db, client_id)
        .await
        .map_err(|e| format!("client lookup failed: {e}"))?
        .ok_or_else(|| "client not found".to_owned())?;

    let chat_id = client
        .external_id
        .ok_or_else(|| "client has no external_id".to_owned())?;

    let message = crate::provider::telegram::OutboundMessage::Text {
        text: text.to_owned(),
    };
    crate::provider::telegram::send(
        &state.config.telegram_api_base,
        &config.bot_token,
        &chat_id,
        &message,
    )
    .await
}

fn spawn_telegram_delivery(
    state: Arc<AppState>,
    channel_id: Uuid,
    client_id: Uuid,
    operator_id: Uuid,
    text: String,
) {
    tokio::spawn(async move {
        if let Err(reason) = deliver_to_telegram(&state, channel_id, client_id, &text).await {
            tracing::error!(%channel_id, %operator_id, "telegram delivery failed: {reason}");
            notify_operator_error(
                &state,
                operator_id,
                format!("Telegram delivery failed: {reason}"),
            );
        }
    });
}
