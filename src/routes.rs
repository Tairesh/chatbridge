use std::sync::Arc;

use axum::Router;
use axum::routing::{get, patch, post};

use crate::config::AppState;
use crate::handler::{api, channels, operator_ws, webhook, widget_ws};

pub fn build(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/webhook/instagram", get(webhook::meta_verify))
        .route("/webhook/instagram", post(webhook::instagram_ingest))
        .route(
            "/webhook/telegram/{channel_id}",
            post(webhook::telegram_ingest),
        )
        .route("/api/channels", get(channels::list).post(channels::create))
        .route(
            "/api/channels/{id}",
            patch(channels::update).delete(channels::delete),
        )
        .route(
            "/api/channels/{id}/webhook",
            get(channels::webhook_status).post(channels::webhook_register),
        )
        .route("/api/chats", get(api::get_chats))
        .route("/api/chats/{chat_id}", get(api::get_chat_messages))
        .route("/ws/operator", get(operator_ws::operator_ws))
        .route("/ws/{widget_id}", get(widget_ws::widget_ws))
        .with_state(state)
}
