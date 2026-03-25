use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ProviderKind {
    Instagram,
    Telegram,
    Widget,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum EventKind {
    Message,
    Edit,
    Read,
    Reaction,
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Sender {
    pub id: Uuid,
    #[serde(rename = "type")]
    pub sender_type: String,
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IncomingMessage {
    pub id: Uuid,
    pub external_message_id: String,
    pub channel_id: Uuid,
    pub chat_id: Option<Uuid>,
    pub text: Option<String>,
    pub status: String,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub sender: Sender,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IncomingEdit {
    pub id: Uuid,
    pub external_message_id: String,
    pub channel_id: Uuid,
    pub chat_id: Option<Uuid>,
    pub text: Option<String>,
    pub edited_at: chrono::DateTime<chrono::Utc>,
    pub sender: Sender,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IncomingRead {
    pub id: Uuid,
    pub external_message_id: String,
    pub channel_id: Uuid,
    pub chat_id: Option<Uuid>,
    pub sender: Sender,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum IncomingEvent {
    Message(IncomingMessage),
    Edit(IncomingEdit),
    Read(IncomingRead),
}

impl IncomingEvent {
    pub fn sender_id(&self) -> Uuid {
        match self {
            IncomingEvent::Message(m) => m.sender.id,
            IncomingEvent::Edit(e) => e.sender.id,
            IncomingEvent::Read(r) => r.sender.id,
        }
    }

    pub fn chat_id(&self) -> Option<Uuid> {
        match self {
            IncomingEvent::Message(m) => m.chat_id,
            IncomingEvent::Edit(e) => e.chat_id,
            IncomingEvent::Read(r) => r.chat_id,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct NewMessage {
    pub external_message_id: String,
    pub channel_id: Uuid,
    pub sender_id: Option<Uuid>,
    pub sender_type: String,
    pub provider: ProviderKind,
    pub event: EventKind,
    pub text: Option<String>,
    pub raw: serde_json::Value,
}

impl std::str::FromStr for ProviderKind {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "instagram" => Ok(Self::Instagram),
            "telegram" => Ok(Self::Telegram),
            "widget" => Ok(Self::Widget),
            other => Err(format!("unknown provider: {other}")),
        }
    }
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

impl std::fmt::Display for EventKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EventKind::Message => write!(f, "message"),
            EventKind::Edit => write!(f, "edit"),
            EventKind::Read => write!(f, "read"),
            EventKind::Reaction => write!(f, "reaction"),
            EventKind::Unknown => write!(f, "unknown"),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct WsInbound {
    pub action: WsActionKind,
    pub mid: Uuid,
    pub text: Option<String>,
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

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum WsOutbound {
    Auth {
        token: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        operator_id: Option<Uuid>,
    },
    Ack {
        message_id: Uuid,
    },
    Error {
        reason: String,
    },
}

#[derive(Debug, Serialize, Deserialize)]
pub struct OperatorInbound {
    pub action: WsActionKind,
    pub chat_id: Uuid,
    pub mid: String,
    pub text: Option<String>,
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
        assert_eq!(msg.text, Some("Hello".into()));
        assert_eq!(msg.attachments.len(), 1);
    }

    #[test]
    fn ws_inbound_deserialize_send_without_attachments() {
        let json = format!(r#"{{"action": "send", "mid": "{TEST_UUID}", "text": "Hello"}}"#);
        let msg: WsInbound = serde_json::from_str(&json).unwrap();
        assert_eq!(msg.action, WsActionKind::Send);
        assert_eq!(msg.text, Some("Hello".into()));
        assert!(msg.attachments.is_empty());
    }

    #[test]
    fn ws_inbound_deserialize_edit() {
        let json = format!(r#"{{"action": "edit", "mid": "{TEST_UUID}", "text": "Updated"}}"#);
        let msg: WsInbound = serde_json::from_str(&json).unwrap();
        assert_eq!(msg.action, WsActionKind::Edit);
        assert_eq!(msg.mid.to_string(), TEST_UUID);
        assert_eq!(msg.text, Some("Updated".into()));
    }

    #[test]
    fn ws_inbound_deserialize_read_no_text() {
        let json = format!(r#"{{"action": "read", "mid": "{TEST_UUID}"}}"#);
        let msg: WsInbound = serde_json::from_str(&json).unwrap();
        assert_eq!(msg.action, WsActionKind::Read);
        assert_eq!(msg.text, None);
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
    fn ws_outbound_auth_serializes_correctly() {
        let msg = WsOutbound::Auth {
            token: "eyJ.test.token".into(),
            operator_id: None,
        };
        let json = serde_json::to_value(&msg).unwrap();
        assert_eq!(json["action"], "auth");
        assert_eq!(json["token"], "eyJ.test.token");
        assert!(json.get("operator_id").is_none());
    }

    #[test]
    fn ws_outbound_auth_with_operator_id() {
        let op_id: Uuid = TEST_UUID.parse().unwrap();
        let msg = WsOutbound::Auth {
            token: "eyJ.test.token".into(),
            operator_id: Some(op_id),
        };
        let json = serde_json::to_value(&msg).unwrap();
        assert_eq!(json["action"], "auth");
        assert_eq!(json["operator_id"], TEST_UUID);
    }

    #[test]
    fn ws_outbound_ack_serializes_correctly() {
        let mid: Uuid = TEST_UUID.parse().unwrap();
        let msg = WsOutbound::Ack { message_id: mid };
        let json = serde_json::to_value(&msg).unwrap();
        assert_eq!(json["action"], "ack");
        assert_eq!(json["message_id"], TEST_UUID);
    }

    #[test]
    fn ws_outbound_error_serializes_correctly() {
        let msg = WsOutbound::Error {
            reason: "bad input".into(),
        };
        let json = serde_json::to_value(&msg).unwrap();
        assert_eq!(json["action"], "error");
        assert_eq!(json["reason"], "bad input");
    }

    #[test]
    fn provider_kind_widget_display() {
        assert_eq!(ProviderKind::Widget.to_string(), "widget");
    }

    #[test]
    fn sender_serializes_operator() {
        let sender = Sender {
            id: TEST_UUID.parse().unwrap(),
            sender_type: "operator".into(),
            name: Some("Alice".into()),
            username: None,
        };
        let json = serde_json::to_value(&sender).unwrap();
        assert_eq!(json["id"], TEST_UUID);
        assert_eq!(json["type"], "operator");
        assert_eq!(json["name"], "Alice");
        assert!(json.get("username").is_none());
    }

    #[test]
    fn sender_serializes_client_with_username() {
        let sender = Sender {
            id: TEST_UUID.parse().unwrap(),
            sender_type: "client".into(),
            name: Some("John".into()),
            username: Some("john123".into()),
        };
        let json = serde_json::to_value(&sender).unwrap();
        assert_eq!(json["type"], "client");
        assert_eq!(json["username"], "john123");
    }

    #[test]
    fn incoming_message_with_sender_serializes() {
        let event = IncomingEvent::Message(IncomingMessage {
            id: TEST_UUID.parse().unwrap(),
            external_message_id: "widget:123".into(),
            channel_id: TEST_UUID.parse().unwrap(),
            chat_id: Some(TEST_UUID.parse().unwrap()),
            text: Some("hello".into()),
            status: "new".into(),
            created_at: chrono::Utc::now(),
            sender: Sender {
                id: TEST_UUID.parse().unwrap(),
                sender_type: "client".into(),
                name: Some("John".into()),
                username: None,
            },
        });
        let json = serde_json::to_value(&event).unwrap();
        assert_eq!(json["type"], "message");
        assert_eq!(json["sender"]["type"], "client");
        assert!(json.get("sender_id").is_none());
    }

    #[test]
    fn operator_inbound_deserialize_send() {
        let json = format!(
            r#"{{"action": "send", "chat_id": "{TEST_UUID}", "mid": "{TEST_UUID}", "text": "hello"}}"#
        );
        let msg: OperatorInbound = serde_json::from_str(&json).unwrap();
        assert_eq!(msg.action, WsActionKind::Send);
        assert_eq!(msg.text, Some("hello".into()));
    }

    #[test]
    fn operator_inbound_deserialize_read_no_text() {
        let json =
            format!(r#"{{"action": "read", "chat_id": "{TEST_UUID}", "mid": "{TEST_UUID}"}}"#);
        let msg: OperatorInbound = serde_json::from_str(&json).unwrap();
        assert_eq!(msg.action, WsActionKind::Read);
        assert_eq!(msg.text, None);
    }
}
