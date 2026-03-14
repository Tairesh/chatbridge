use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ProviderKind {
    Instagram,
    Telegram,
    Widget,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EventKind {
    Message,
    Edit,
    Read,
    Reaction,
    Unknown,
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
            ProviderKind::Widget => write!(f, "widget"),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct WsInbound {
    pub text: String,
    #[serde(default)]
    pub attachments: Vec<Uuid>,
}

#[derive(Debug, Serialize)]
pub struct WsAck {
    pub status: &'static str,
    pub message_id: Uuid,
}

#[derive(Debug, Serialize)]
pub struct WsError {
    pub status: &'static str,
    pub reason: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ws_inbound_deserialize_with_attachments() {
        let json = r#"{"text": "Hello", "attachments": ["550e8400-e29b-41d4-a716-446655440000"]}"#;
        let msg: WsInbound = serde_json::from_str(json).unwrap();
        assert_eq!(msg.text, "Hello");
        assert_eq!(msg.attachments.len(), 1);
    }

    #[test]
    fn ws_inbound_deserialize_without_attachments() {
        let json = r#"{"text": "Hello"}"#;
        let msg: WsInbound = serde_json::from_str(json).unwrap();
        assert_eq!(msg.text, "Hello");
        assert!(msg.attachments.is_empty());
    }

    #[test]
    fn ws_inbound_missing_text_fails() {
        let json = r#"{"attachments": []}"#;
        assert!(serde_json::from_str::<WsInbound>(json).is_err());
    }

    #[test]
    fn ws_inbound_invalid_uuid_attachment_fails() {
        let json = r#"{"text": "Hi", "attachments": ["not-a-uuid"]}"#;
        assert!(serde_json::from_str::<WsInbound>(json).is_err());
    }

    #[test]
    fn ws_ack_serializes_correctly() {
        let ack = WsAck {
            status: "ok",
            message_id: Uuid::nil(),
        };
        let json = serde_json::to_value(&ack).unwrap();
        assert_eq!(json["status"], "ok");
        assert_eq!(json["message_id"], "00000000-0000-0000-0000-000000000000");
    }

    #[test]
    fn ws_error_serializes_correctly() {
        let err = WsError {
            status: "error",
            reason: "bad input".into(),
        };
        let json = serde_json::to_value(&err).unwrap();
        assert_eq!(json["status"], "error");
        assert_eq!(json["reason"], "bad input");
    }

    #[test]
    fn provider_kind_widget_display() {
        assert_eq!(ProviderKind::Widget.to_string(), "widget");
    }
}
