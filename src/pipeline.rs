use redis::AsyncCommands;
use uuid::Uuid;

use crate::config::AppState;
use crate::model::{
    EventKind, IncomingEdit, IncomingEvent, IncomingMessage, IncomingRead, NewMessage, Sender,
};

pub(crate) const REDIS_CHANNEL: &str = "incoming_messages";

pub(crate) async fn publish_event(
    redis: &mut redis::aio::ConnectionManager,
    event: &IncomingEvent,
) {
    let payload = serde_json::to_string(event).expect("IncomingEvent serialization cannot fail");
    if let Err(e) = redis.publish::<_, _, ()>(REDIS_CHANNEL, &payload).await {
        tracing::error!("redis publish failed: {e}");
    }
}

/// Build a Sender object from caches.
pub(crate) async fn resolve_sender(
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
            let client = state
                .client_cache
                .get_client_by_uuid(db, sender_id)
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

pub(crate) async fn persist_and_publish(
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
                    Some(id)
                        if state
                            .client_cache
                            .get_client_by_uuid(db, id)
                            .await
                            .ok()
                            .flatten()
                            .is_some() =>
                    {
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
