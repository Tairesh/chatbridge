use redis::AsyncCommands;
use uuid::Uuid;

use crate::config::AppState;
use crate::model::{
    Conversation, EventKind, IncomingEdit, IncomingEvent, IncomingMessage, IncomingRead,
    NewMessage, Sender,
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
                name: op.map(|o| o.name.clone()),
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

/// Persist an event and publish it.
///
/// Returns the id of the row it inserted — `None` for every other outcome, including
/// a duplicate. The delivery path uses it to name the row it just sent, and the ack
/// uses it to give the panel an anchor that survives the provider renaming the
/// message.
pub(crate) async fn persist_and_publish(
    db: &sqlx::PgPool,
    redis: &mut redis::aio::ConnectionManager,
    msg: &NewMessage,
    state: &AppState,
) -> Option<Uuid> {
    match msg.event {
        EventKind::Message => {
            let (chat_id, insert_msg) = if let Some(Conversation::Chat(id)) = msg.conversation {
                // The panel named the chat, so there is nothing to resolve.
                (Some(id), msg.clone())
            } else {
                // The chat belongs to the customer. The author may be somebody else
                // entirely — or nobody, when the account owner wrote from the
                // provider's own app.
                let customer = match msg.conversation {
                    Some(Conversation::Customer(id)) => Some(id),
                    _ => None,
                };
                let verified_client = match customer {
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

                let chat_id = match verified_client {
                    Some(client_id) => {
                        match crate::db::find_or_create_chat(db, client_id, msg.channel_id).await {
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

                // An author we cannot resolve is not an author: `resolve_sender` must
                // never be handed an id with nothing behind it. An operator-side
                // message legitimately has no author and passes through as NULL.
                let insert_msg = if msg.sender_type == "client" && msg.sender_id != verified_client
                {
                    NewMessage {
                        sender_id: verified_client,
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
                    let id = incoming.id;
                    tracing::info!(message = ?incoming, "processed incoming message");
                    publish_event(redis, &IncomingEvent::Message(incoming)).await;
                    Some(id)
                }
                Ok(None) => {
                    tracing::debug!(
                        external_message_id = %msg.external_message_id,
                        "duplicate message, skipping"
                    );
                    None
                }
                Err(e) => {
                    tracing::error!("insert_message failed: {e}");
                    None
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
            None
        }
        EventKind::Read => {
            let mut reads = match crate::db::mark_messages_read(
                db,
                msg.channel_id,
                &msg.external_message_id,
                &msg.sender_type,
            )
            .await
            {
                Ok(rows) => rows,
                Err(e) => {
                    tracing::error!("mark_messages_read failed: {e}");
                    return None;
                }
            };

            if reads.is_empty() {
                // The anchor is unknown: the receipt overtook the id adoption, or the
                // message was sent from the provider's own app before we stored it.
                // Only a customer's chat can be swept this way; a read on a chat the
                // panel named is already anchored by `mark_messages_read_by_id`.
                if let Some(Conversation::Customer(client_id)) = msg.conversation {
                    match crate::db::mark_chat_read(db, msg.channel_id, client_id, &msg.sender_type)
                        .await
                    {
                        Ok(rows) => reads = rows,
                        Err(e) => tracing::error!("mark_chat_read failed: {e}"),
                    }
                }
            }

            if reads.is_empty() {
                tracing::warn!(
                    external_message_id = %msg.external_message_id,
                    conversation = ?msg.conversation,
                    "read receipt matched nothing — this reader had nothing unread"
                );
                return None;
            }

            tracing::info!(count = reads.len(), "processed read receipt");
            for db_read in &reads {
                // The sender of a read event is the *author* of the message that was
                // read — what listener.rs routes on, and what the widget and operator
                // paths already publish.
                let sender = resolve_sender(
                    db_read.sender_id.unwrap_or(Uuid::nil()),
                    &db_read.sender_type,
                    db,
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
                publish_event(redis, &IncomingEvent::Read(read)).await;
            }
            None
        }
        EventKind::Reaction | EventKind::Unknown => {
            tracing::info!(
                event = %msg.event,
                external_message_id = %msg.external_message_id,
                "event logged (not persisted)"
            );
            None
        }
    }
}
