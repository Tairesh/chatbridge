use std::sync::Arc;

use futures_util::StreamExt;

use crate::config::AppState;
use crate::model::IncomingEvent;
use crate::pipeline::REDIS_CHANNEL;

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
