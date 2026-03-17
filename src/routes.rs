use std::sync::Arc;

use axum::Router;
use axum::routing::{get, post};

use crate::config::AppState;
use crate::handler;

pub fn build(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/webhook/instagram", get(handler::meta_verify))
        .route("/webhook/instagram", post(handler::instagram_ingest))
        .route(
            "/webhook/telegram/{channel_id}",
            post(handler::telegram_ingest),
        )
        .route("/api/chats", get(handler::get_chats))
        .route("/api/chats/{chat_id}", get(handler::get_chat_messages))
        .route("/ws/operator", get(handler::operator_ws))
        .route("/ws/{widget_id}", get(handler::widget_ws))
        .with_state(state)
}
