use crate::config::AppState;
use crate::error::WebhookError;
use axum::Json;
use axum::extract::{Path, State};
use std::sync::Arc;
use uuid::Uuid;

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
