use std::sync::Arc;

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use tokio::time::timeout;
use uuid::Uuid;

use super::{
    ConnectionGuard, IDLE_TIMEOUT, PING_TIMEOUT, SEND_TIMEOUT, WsTokenParams, resolve_client,
    send_outbound,
};
use crate::config::AppState;
use crate::model::{
    EventKind, IncomingEvent, IncomingRead, NewMessage, ProviderKind, WsInbound, WsOutbound,
};
use crate::pipeline::{persist_and_publish, publish_event, resolve_sender};

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
