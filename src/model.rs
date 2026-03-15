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
    pub action: WsActionKind,
    pub mid: Uuid,
    pub text: String,
    #[serde(default)]
    pub attachments: Vec<Uuid>,
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Copy, Clone)]
#[serde(rename_all = "snake_case")]
pub enum WsActionKind {
    Send,
    Edit,
    Read,
}

impl From<WsActionKind> for EventKind {
    fn from(action: WsActionKind) -> Self {
        match action {
            WsActionKind::Send => EventKind::Message,
            WsActionKind::Edit => EventKind::Edit,
            WsActionKind::Read => EventKind::Read,
        }
    }
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

    const TEST_UUID: &str = "550e8400-e29b-41d4-a716-446655440000";

    #[test]
    fn ws_inbound_deserialize_send_with_attachments() {
        let json = format!(
            r#"{{"action": "send", "mid": "{TEST_UUID}", "text": "Hello", "attachments": ["{TEST_UUID}"]}}"#
        );
        let msg: WsInbound = serde_json::from_str(&json).unwrap();
        assert_eq!(msg.action, WsActionKind::Send);
        assert_eq!(msg.mid.to_string(), TEST_UUID);
        assert_eq!(msg.text, "Hello");
        assert_eq!(msg.attachments.len(), 1);
    }

    #[test]
    fn ws_inbound_deserialize_send_without_attachments() {
        let json = format!(r#"{{"action": "send", "mid": "{TEST_UUID}", "text": "Hello"}}"#);
        let msg: WsInbound = serde_json::from_str(&json).unwrap();
        assert_eq!(msg.action, WsActionKind::Send);
        assert_eq!(msg.text, "Hello");
        assert!(msg.attachments.is_empty());
    }

    #[test]
    fn ws_inbound_deserialize_edit() {
        let json = format!(r#"{{"action": "edit", "mid": "{TEST_UUID}", "text": "Updated"}}"#);
        let msg: WsInbound = serde_json::from_str(&json).unwrap();
        assert_eq!(msg.action, WsActionKind::Edit);
        assert_eq!(msg.mid.to_string(), TEST_UUID);
        assert_eq!(msg.text, "Updated");
    }

    #[test]
    fn ws_inbound_missing_action_fails() {
        let json = format!(r#"{{"mid": "{TEST_UUID}", "text": "Hello"}}"#);
        assert!(serde_json::from_str::<WsInbound>(&json).is_err());
    }

    #[test]
    fn ws_inbound_missing_mid_fails() {
        let json = r#"{"action": "send", "text": "Hello"}"#;
        assert!(serde_json::from_str::<WsInbound>(json).is_err());
    }

    #[test]
    fn ws_inbound_invalid_mid_fails() {
        let json = r#"{"action": "send", "mid": "not-a-uuid", "text": "Hi"}"#;
        assert!(serde_json::from_str::<WsInbound>(json).is_err());
    }

    #[test]
    fn ws_inbound_invalid_uuid_attachment_fails() {
        let json = format!(
            r#"{{"action": "send", "mid": "{TEST_UUID}", "text": "Hi", "attachments": ["not-a-uuid"]}}"#
        );
        assert!(serde_json::from_str::<WsInbound>(&json).is_err());
    }

    #[test]
    fn ws_ack_serializes_correctly() {
        let mid: Uuid = TEST_UUID.parse().unwrap();
        let ack = WsAck {
            status: "ok",
            message_id: mid,
        };
        let json = serde_json::to_value(&ack).unwrap();
        assert_eq!(json["status"], "ok");
        assert_eq!(json["message_id"], TEST_UUID);
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
