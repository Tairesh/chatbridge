pub mod api;
pub mod channels;
pub mod operator_ws;
pub mod webhook;
pub mod widget_ws;

use std::sync::Arc;

use axum::extract::ws::{Message, WebSocket};
use serde::Deserialize;
use std::time::Duration;
use tokio::time::timeout;
use uuid::Uuid;

use crate::config::AppState;
use crate::model::WsOutbound;

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

/// Send a WsOutbound message to the socket. Returns false if the send fails.
async fn send_outbound(socket: &mut WebSocket, msg: &WsOutbound) -> bool {
    let text = serde_json::to_string(msg).expect("WsOutbound serialization cannot fail");
    matches!(
        timeout(SEND_TIMEOUT, socket.send(Message::Text(text.into()))).await,
        Ok(Ok(()))
    )
}

#[derive(Deserialize)]
pub struct WsTokenParams {
    pub token: Option<String>,
}

/// Resolve or create a client from an optional JWT token.
async fn resolve_client(
    token: Option<&str>,
    jwt_secret: &str,
    state: &AppState,
) -> Result<(Uuid, Option<String>), sqlx::Error> {
    if let Some(client_id) = token.and_then(|t| crate::jwt::verify(t, jwt_secret.as_bytes()))
        && state
            .client_cache
            .get_client_by_uuid(&state.db, client_id)
            .await?
            .is_some()
    {
        return Ok((client_id, None));
    }

    let client_id = crate::db::create_client(&state.db).await?;
    let token = crate::jwt::sign(client_id, jwt_secret.as_bytes());
    Ok((client_id, Some(token)))
}

/// Resolve or create an operator from an optional JWT token.
async fn resolve_operator(
    token: Option<&str>,
    jwt_secret: &str,
    state: &AppState,
) -> Result<(Uuid, Option<String>), sqlx::Error> {
    if let Some(operator_id) = token.and_then(|t| crate::jwt::verify(t, jwt_secret.as_bytes()))
        && state
            .operator_cache
            .get_operator(&state.db, operator_id)
            .await?
            .is_some()
    {
        return Ok((operator_id, None));
    }
    let operator_id = crate::db::create_operator(&state.db).await?;
    let token = crate::jwt::sign(operator_id, jwt_secret.as_bytes());
    Ok((operator_id, Some(token)))
}
