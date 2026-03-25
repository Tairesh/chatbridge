use std::sync::Arc;

use axum::Router;
use axum::routing::{get, post};

use crate::config::AppState;
use crate::handler::{api, operator_ws, webhook, widget_ws};

pub fn build(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/webhook/instagram", get(webhook::meta_verify))
        .route("/webhook/instagram", post(webhook::instagram_ingest))
        .route(
            "/webhook/telegram/{channel_id}",
            post(webhook::telegram_ingest),
        )
        .route("/api/chats", get(api::get_chats))
        .route("/api/chats/{chat_id}", get(api::get_chat_messages))
        .route("/ws/operator", get(operator_ws::operator_ws))
        .route("/ws/{widget_id}", get(widget_ws::widget_ws))
        .with_state(state)
}
