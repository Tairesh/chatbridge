use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ProviderKind {
    Instagram,
    Telegram,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EventKind {
    Message,
    Edit,
    Read,
    Reaction,
}

#[derive(Debug, Clone, Serialize)]
pub struct InternalMessage {
    pub message_id: String,
    pub channel_id: Uuid,
    pub provider: ProviderKind,
    pub event: EventKind,
    pub timestamp: i64,
    pub raw: serde_json::Value,
}

impl std::fmt::Display for ProviderKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ProviderKind::Instagram => write!(f, "instagram"),
            ProviderKind::Telegram => write!(f, "telegram"),
        }
    }
}
