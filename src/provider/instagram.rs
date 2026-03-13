use axum::http::HeaderMap;
use hmac::{Hmac, Mac};
use serde::Deserialize;
use sha2::Sha256;
use sqlx::PgPool;
use uuid::Uuid;

use crate::db;
use crate::error::WebhookError;
use crate::model::{EventKind, InternalMessage, ProviderKind};
use crate::provider::WebhookProvider;

pub struct InstagramProvider {
    app_secret: String,
}

impl InstagramProvider {
    pub fn new(app_secret: &str) -> Self {
        Self {
            app_secret: app_secret.to_owned(),
        }
    }
}

impl WebhookProvider for InstagramProvider {
    fn verify(&self, headers: &HeaderMap, body: &[u8]) -> Result<(), WebhookError> {
        let signature = headers
            .get("X-Hub-Signature-256")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("sha256="))
            .ok_or_else(|| WebhookError::Forbidden("missing X-Hub-Signature-256".into()))?;

        let mut mac = Hmac::<Sha256>::new_from_slice(self.app_secret.as_bytes())
            .map_err(|e| WebhookError::Internal(e.to_string()))?;
        mac.update(body);
        let computed = hex::encode(mac.finalize().into_bytes());

        if computed != signature {
            return Err(WebhookError::Forbidden("signature mismatch".into()));
        }

        Ok(())
    }

    async fn parse(&self, body: &[u8], db: &PgPool) -> Result<Vec<InternalMessage>, WebhookError> {
        let payload: MetaWebhookPayload =
            serde_json::from_slice(body).map_err(|e| WebhookError::BadRequest(e.to_string()))?;

        if payload.object != "instagram" {
            return Err(WebhookError::BadRequest(format!(
                "unexpected object: {}",
                payload.object
            )));
        }

        let mut messages = Vec::new();

        for entry in &payload.entry {
            let Some(ref messaging) = entry.messaging else {
                continue;
            };

            for event in messaging {
                let (event_kind, mid) = classify_event(event);

                let sender_id = event.sender.as_ref().map(|s| s.id.as_str());
                let recipient_id = event.recipient.as_ref().map(|r| r.id.as_str());

                // Check both sender and recipient against instagram_channels
                let mut matched_channel_ids: Vec<Uuid> = Vec::new();

                for ig_user_id in [sender_id, recipient_id].into_iter().flatten() {
                    if let Some(ch) = db::find_instagram_channels_by_user_id(db, ig_user_id).await?
                        && !matched_channel_ids.contains(&ch.id)
                    {
                        matched_channel_ids.push(ch.id);
                    }
                }

                if matched_channel_ids.is_empty() {
                    tracing::warn!(
                        sender = sender_id,
                        recipient = recipient_id,
                        "no channel found for instagram event"
                    );
                    continue;
                }

                let raw = serde_json::to_value(event).unwrap_or(serde_json::Value::Null);

                for channel_id in matched_channel_ids {
                    let message_id = format!("instagram:{}", mid.unwrap_or(&entry.id));
                    messages.push(InternalMessage {
                        message_id,
                        channel_id,
                        provider: ProviderKind::Instagram,
                        event: event_kind.clone(),
                        timestamp: event.timestamp.unwrap_or(entry.time),
                        raw: raw.clone(),
                    });
                }
            }
        }

        Ok(messages)
    }
}

fn classify_event(event: &MessagingEvent) -> (EventKind, Option<&String>) {
    if let Some(ref msg) = event.message {
        return (EventKind::Message, Some(&msg.mid));
    }
    if let Some(ref edit) = event.message_edit {
        return (EventKind::Edit, Some(&edit.mid));
    }
    if event.read.is_some() {
        return (EventKind::Read, None);
    }
    if event.reaction.is_some() {
        return (EventKind::Reaction, None);
    }
    // Default to Message for unknown event types
    (EventKind::Message, None)
}

// --- Meta webhook payload types ---

#[derive(Debug, Deserialize)]
pub struct MetaWebhookPayload {
    pub object: String,
    pub entry: Vec<Entry>,
}

#[derive(Debug, Deserialize)]
pub struct Entry {
    pub id: String,
    pub time: i64,
    pub messaging: Option<Vec<MessagingEvent>>,
}

#[derive(Debug, Deserialize, serde::Serialize)]
pub struct MessagingEvent {
    pub sender: Option<Participant>,
    pub recipient: Option<Participant>,
    pub timestamp: Option<i64>,
    pub message: Option<Message>,
    pub message_edit: Option<MessageEdit>,
    pub read: Option<serde_json::Value>,
    pub reaction: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize, serde::Serialize)]
pub struct Participant {
    pub id: String,
}

#[derive(Debug, Deserialize, serde::Serialize)]
pub struct Message {
    pub mid: String,
    pub text: Option<String>,
    pub attachments: Option<Vec<serde_json::Value>>,
}

#[derive(Debug, Deserialize, serde::Serialize)]
pub struct MessageEdit {
    pub mid: String,
    pub text: Option<String>,
    pub num_edit: Option<i32>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::provider::WebhookProvider;
    use hmac::{Hmac, Mac};
    use sha2::Sha256;

    const TEST_SECRET: &str = "test_secret_key";

    fn sign(secret: &str, body: &[u8]) -> String {
        let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).expect("valid key length");
        mac.update(body);
        hex::encode(mac.finalize().into_bytes())
    }

    #[test]
    fn verify_valid_signature() {
        let provider = InstagramProvider::new(TEST_SECRET);
        let body = b"test body";
        let sig = sign(TEST_SECRET, body);

        let mut headers = HeaderMap::new();
        headers.insert(
            "X-Hub-Signature-256",
            format!("sha256={sig}").parse().unwrap(),
        );

        assert!(provider.verify(&headers, body).is_ok());
    }

    #[test]
    fn verify_invalid_signature() {
        let provider = InstagramProvider::new(TEST_SECRET);
        let body = b"test body";

        let mut headers = HeaderMap::new();
        headers.insert(
            "X-Hub-Signature-256",
            "sha256=0000000000000000000000000000000000000000000000000000000000000000"
                .parse()
                .unwrap(),
        );

        let err = provider.verify(&headers, body).unwrap_err();
        assert!(err.to_string().contains("signature mismatch"));
    }

    #[test]
    fn verify_missing_header() {
        let provider = InstagramProvider::new(TEST_SECRET);
        let headers = HeaderMap::new();

        let err = provider.verify(&headers, b"body").unwrap_err();
        assert!(err.to_string().contains("missing X-Hub-Signature-256"));
    }

    #[test]
    fn verify_missing_sha256_prefix() {
        let provider = InstagramProvider::new(TEST_SECRET);
        let body = b"test body";
        let sig = sign(TEST_SECRET, body);

        let mut headers = HeaderMap::new();
        headers.insert("X-Hub-Signature-256", sig.parse().unwrap());

        let err = provider.verify(&headers, body).unwrap_err();
        assert!(err.to_string().contains("missing X-Hub-Signature-256"));
    }

    #[test]
    fn classify_message_event() {
        let event = MessagingEvent {
            sender: None,
            recipient: None,
            timestamp: Some(123),
            message: Some(Message {
                mid: "mid_001".into(),
                text: Some("hello".into()),
                attachments: None,
            }),
            message_edit: None,
            read: None,
            reaction: None,
        };
        let (kind, mid) = classify_event(&event);
        assert!(matches!(kind, EventKind::Message));
        assert_eq!(mid.unwrap(), "mid_001");
    }

    #[test]
    fn classify_edit_event() {
        let event = MessagingEvent {
            sender: None,
            recipient: None,
            timestamp: Some(123),
            message: None,
            message_edit: Some(MessageEdit {
                mid: "mid_002".into(),
                text: Some("edited".into()),
                num_edit: Some(1),
            }),
            read: None,
            reaction: None,
        };
        let (kind, mid) = classify_event(&event);
        assert!(matches!(kind, EventKind::Edit));
        assert_eq!(mid.unwrap(), "mid_002");
    }

    #[test]
    fn classify_read_event() {
        let event = MessagingEvent {
            sender: None,
            recipient: None,
            timestamp: Some(123),
            message: None,
            message_edit: None,
            read: Some(serde_json::json!({"watermark": 123})),
            reaction: None,
        };
        let (kind, mid) = classify_event(&event);
        assert!(matches!(kind, EventKind::Read));
        assert!(mid.is_none());
    }

    #[test]
    fn classify_reaction_event() {
        let event = MessagingEvent {
            sender: None,
            recipient: None,
            timestamp: Some(123),
            message: None,
            message_edit: None,
            read: None,
            reaction: Some(serde_json::json!({"reaction": "love", "mid": "mid_003"})),
        };
        let (kind, mid) = classify_event(&event);
        assert!(matches!(kind, EventKind::Reaction));
        assert!(mid.is_none());
    }

    #[test]
    fn parse_meta_payload_structure() {
        let payload = r#"{
            "object": "instagram",
            "entry": [{
                "time": 1773347860136,
                "id": "17841448717199999",
                "messaging": [{
                    "sender": {"id": "836189122827510"},
                    "recipient": {"id": "17841448717199999"},
                    "timestamp": 1773347859458,
                    "message": {"mid": "aWdf_abc", "text": "hello"}
                }]
            }]
        }"#;

        let parsed: MetaWebhookPayload = serde_json::from_str(payload).unwrap();
        assert_eq!(parsed.object, "instagram");
        assert_eq!(parsed.entry.len(), 1);
        let entry = &parsed.entry[0];
        assert_eq!(entry.id, "17841448717199999");
        let events = entry.messaging.as_ref().unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].sender.as_ref().unwrap().id, "836189122827510");
        assert_eq!(
            events[0].recipient.as_ref().unwrap().id,
            "17841448717199999"
        );
        assert_eq!(events[0].message.as_ref().unwrap().mid, "aWdf_abc");
        assert_eq!(
            events[0].message.as_ref().unwrap().text.as_deref(),
            Some("hello")
        );
    }
}
