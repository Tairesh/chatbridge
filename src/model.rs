use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ProviderKind {
    Instagram,
    Telegram,
    Widget,
}

/// Stored contents of `channels.config` for a telegram channel.
///
/// These structs deliberately carry no provider tag: `channels.provider` is the
/// single source of truth, so read a config by matching on that column and then
/// deserializing into the matching struct.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TelegramConfig {
    pub bot_token: String,
    pub bot_secret: String,
}

/// Stored contents of `channels.config` for an instagram channel.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstagramConfig {
    pub access_token: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub refresh_time: Option<chrono::DateTime<chrono::Utc>>,
}

/// Stored contents of `channels.config` for a widget channel. A widget channel
/// is fully described by its `external_key`, so there is nothing to store.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WidgetConfig {}

/// Wire format for creating or editing a channel: the body of
/// `POST /api/channels` and the `spec` field of `PATCH /api/channels/{id}`.
///
/// Tagged on the wire (unlike the stored configs) so one endpoint can accept
/// every provider, following the same style as `IncomingEvent` and `WsOutbound`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "provider", rename_all = "snake_case")]
pub enum ChannelSpec {
    Widget {
        widget_id: String,
    },
    Telegram {
        bot_token: String,
    },
    Instagram {
        user_id: String,
        access_token: String,
    },
}

impl ChannelSpec {
    pub fn provider(&self) -> ProviderKind {
        match self {
            ChannelSpec::Widget { .. } => ProviderKind::Widget,
            ChannelSpec::Telegram { .. } => ProviderKind::Telegram,
            ChannelSpec::Instagram { .. } => ProviderKind::Instagram,
        }
    }
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
    /// Sent once on connect when the client already has a chat, so the frontend
    /// can fetch its history. `status` is the raw `chats.status` value; anything
    /// other than "new" means the chat is archived and read-only.
    Chat {
        chat_id: Uuid,
        status: String,
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
    fn ws_outbound_chat_serializes_correctly() {
        let chat_id: Uuid = TEST_UUID.parse().unwrap();
        let msg = WsOutbound::Chat {
            chat_id,
            status: "new".into(),
        };
        let json = serde_json::to_value(&msg).unwrap();
        assert_eq!(json["action"], "chat");
        assert_eq!(json["chat_id"], TEST_UUID);
        assert_eq!(json["status"], "new");
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

    #[test]
    fn channel_spec_deserializes_each_provider() {
        let widget: ChannelSpec =
            serde_json::from_value(serde_json::json!({"provider": "widget", "widget_id": "acme"}))
                .unwrap();
        assert_eq!(widget.provider(), ProviderKind::Widget);

        let telegram: ChannelSpec = serde_json::from_value(
            serde_json::json!({"provider": "telegram", "bot_token": "123:AA"}),
        )
        .unwrap();
        assert_eq!(telegram.provider(), ProviderKind::Telegram);

        let instagram: ChannelSpec = serde_json::from_value(serde_json::json!({
            "provider": "instagram", "user_id": "17841", "access_token": "tok"
        }))
        .unwrap();
        assert_eq!(instagram.provider(), ProviderKind::Instagram);
    }

    #[test]
    fn channel_spec_serializes_with_the_provider_tag() {
        // The frontend hand-builds these objects, so the tag name and field names
        // are a contract in both directions, not just on the way in.
        let widget = ChannelSpec::Widget {
            widget_id: "acme".into(),
        };
        assert_eq!(
            serde_json::to_value(&widget).unwrap(),
            serde_json::json!({"provider": "widget", "widget_id": "acme"})
        );

        let telegram = ChannelSpec::Telegram {
            bot_token: "123:AA".into(),
        };
        assert_eq!(
            serde_json::to_value(&telegram).unwrap(),
            serde_json::json!({"provider": "telegram", "bot_token": "123:AA"})
        );

        let instagram = ChannelSpec::Instagram {
            user_id: "17841".into(),
            access_token: "tok".into(),
        };
        assert_eq!(
            serde_json::to_value(&instagram).unwrap(),
            serde_json::json!({"provider": "instagram", "user_id": "17841", "access_token": "tok"})
        );
    }

    #[test]
    fn channel_spec_rejects_unknown_provider_and_missing_fields() {
        assert!(
            serde_json::from_value::<ChannelSpec>(
                serde_json::json!({"provider": "whatsapp", "phone": "1"})
            )
            .is_err()
        );
        assert!(
            serde_json::from_value::<ChannelSpec>(serde_json::json!({"provider": "widget"}))
                .is_err()
        );
        assert!(
            serde_json::from_value::<ChannelSpec>(
                serde_json::json!({"provider": "instagram", "user_id": "1"})
            )
            .is_err()
        );
    }

    #[test]
    fn telegram_config_round_trips_through_json() {
        let value = serde_json::json!({"bot_token": "123:AA", "bot_secret": "s3cr3t"});
        let cfg: TelegramConfig = serde_json::from_value(value).unwrap();
        assert_eq!(cfg.bot_token, "123:AA");
        assert_eq!(cfg.bot_secret, "s3cr3t");

        let back = serde_json::to_value(&cfg).unwrap();
        assert_eq!(back["bot_token"], "123:AA");
        assert_eq!(back["bot_secret"], "s3cr3t");
    }

    #[test]
    fn telegram_config_missing_secret_fails() {
        let value = serde_json::json!({"bot_token": "123:AA"});
        assert!(serde_json::from_value::<TelegramConfig>(value).is_err());
    }

    #[test]
    fn instagram_config_accepts_absent_refresh_time() {
        let value = serde_json::json!({"access_token": "tok"});
        let cfg: InstagramConfig = serde_json::from_value(value).unwrap();
        assert_eq!(cfg.access_token, "tok");
        assert!(cfg.refresh_time.is_none());
    }

    #[test]
    fn instagram_config_parses_refresh_time() {
        let value = serde_json::json!({
            "access_token": "tok",
            "refresh_time": "2026-03-14T00:00:00+00:00"
        });
        let cfg: InstagramConfig = serde_json::from_value(value).unwrap();
        assert!(cfg.refresh_time.is_some());
    }

    #[test]
    fn widget_config_parses_empty_object() {
        let cfg: WidgetConfig = serde_json::from_value(serde_json::json!({})).unwrap();
        assert_eq!(serde_json::to_value(&cfg).unwrap(), serde_json::json!({}));
    }
}
