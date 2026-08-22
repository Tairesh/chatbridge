use std::sync::{Arc, LazyLock};

use axum::http::HeaderMap;
use hmac::{Hmac, KeyInit, Mac};
use serde::Deserialize;
use sha2::Sha256;
use sqlx::PgPool;
use uuid::Uuid;

use crate::cache::{ChannelCache, ClientCache};
use crate::error::AppError;
use crate::model::{Conversation, EventKind, NewMessage, ProviderKind};
use crate::provider::WebhookProvider;

static HTTP_CLIENT: LazyLock<reqwest::Client> = LazyLock::new(|| {
    reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()
        .expect("failed to build reqwest client")
});

#[derive(Debug, Deserialize)]
struct InstagramProfile {
    name: Option<String>,
    username: Option<String>,
}

pub struct InstagramProvider {
    app_secret: String,
    /// Same base the OAuth module uses. Held here so the profile lookup does not
    /// hardcode a host that tests then cannot redirect.
    graph_base: String,
    cache: Arc<ChannelCache>,
    client_cache: Arc<ClientCache>,
}

impl InstagramProvider {
    pub fn new(
        app_secret: &str,
        graph_base: &str,
        cache: Arc<ChannelCache>,
        client_cache: Arc<ClientCache>,
    ) -> Self {
        Self {
            app_secret: app_secret.to_owned(),
            graph_base: graph_base.to_owned(),
            cache,
            client_cache,
        }
    }
}

impl WebhookProvider for InstagramProvider {
    fn verify(&self, headers: &HeaderMap, body: &[u8]) -> Result<(), AppError> {
        let signature = headers
            .get("X-Hub-Signature-256")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("sha256="))
            .ok_or_else(|| AppError::Forbidden("missing X-Hub-Signature-256".into()))?;

        let mut mac = Hmac::<Sha256>::new_from_slice(self.app_secret.as_bytes())
            .map_err(|e| AppError::Internal(e.to_string()))?;
        mac.update(body);

        let sig_bytes = hex::decode(signature)
            .map_err(|_| AppError::Forbidden("invalid signature hex".into()))?;

        mac.verify_slice(&sig_bytes)
            .map_err(|_| AppError::Forbidden("signature mismatch".into()))?;

        Ok(())
    }

    async fn parse(
        &self,
        body: &[u8],
        db: &PgPool,
        redis: redis::aio::ConnectionManager,
    ) -> Result<Vec<NewMessage>, AppError> {
        let payload: MetaWebhookPayload =
            serde_json::from_slice(body).map_err(|e| AppError::BadRequest(e.to_string()))?;

        if payload.object != "instagram" {
            return Err(AppError::BadRequest(format!(
                "unexpected object: {}",
                payload.object
            )));
        }

        let mut messages = Vec::new();

        for entry in &payload.entry {
            // Both delivery shapes, in one list. Which one Meta uses depends on the
            // Graph API version the app is pinned to, and an app can see the old
            // shape for a while after the new one appears.
            let mut events: Vec<&MessagingEvent> = Vec::new();
            if let Some(ref messaging) = entry.messaging {
                events.extend(messaging.iter());
            }
            if let Some(ref changes) = entry.changes {
                for change in changes {
                    // Gated on the *shape*, not on the field name: a change whose
                    // value carries none of message / message_edit / read / reaction
                    // is something else entirely (a comment, a dashboard test send),
                    // and turning it into an Unknown message would invent an inbox
                    // entry out of nothing.
                    if matches!(classify_event(&change.value).0, EventKind::Unknown) {
                        tracing::warn!(
                            entry_id = %entry.id,
                            field = %change.field,
                            "ignoring an instagram change this app cannot interpret"
                        );
                        continue;
                    }
                    events.push(&change.value);
                }
            }

            if events.is_empty() {
                // Never silent. Key names only: the values carry message text.
                let keys: Vec<&str> = entry.other.keys().map(String::as_str).collect();
                tracing::warn!(
                    entry_id = %entry.id,
                    "instagram entry produced no events. Unrecognised keys: {:?}",
                    keys
                );
                continue;
            }

            for event in events {
                let (event_kind, mid) = classify_event(event);

                // An echo is the mirror of an inbound event: the account is the
                // sender and the customer is the recipient. Routed the normal way it
                // matches no channel, which is why it used to be dropped — and with
                // it, every message the owner sent from the Instagram app, which is
                // the only record of those we will ever have.
                let is_echo = event.message.as_ref().is_some_and(|m| m.is_echo);
                let (account, customer) = if is_echo {
                    (event.sender.as_ref(), event.recipient.as_ref())
                } else {
                    (event.recipient.as_ref(), event.sender.as_ref())
                };

                let Some(account_id) = account.map(|p| p.id.as_str()) else {
                    tracing::warn!(is_echo, "instagram event names no account");
                    continue;
                };

                let Some(channel) = self
                    .cache
                    .get_channel_by_external_key(db, ProviderKind::Instagram, account_id)
                    .await?
                else {
                    tracing::warn!(?account_id, "no channel found for instagram event");
                    continue;
                };

                let config: crate::model::InstagramConfig =
                    match serde_json::from_value(channel.config.clone()) {
                        Ok(cfg) => cfg,
                        Err(e) => {
                            tracing::error!(channel_id = %channel.id, "bad instagram config: {e}");
                            continue;
                        }
                    };

                let client_id = resolve_instagram_client(
                    db,
                    &self.client_cache,
                    customer,
                    &config.access_token,
                    &self.graph_base,
                    redis.clone(),
                )
                .await;

                let raw = serde_json::to_value(event).unwrap_or(serde_json::Value::Null);
                let external_message_id = crate::external_id::instagram(mid.unwrap_or(&entry.id));
                let text = match event_kind {
                    EventKind::Message => event.message.as_ref().and_then(|m| m.text.clone()),
                    EventKind::Edit => event.message_edit.as_ref().and_then(|e| e.text.clone()),
                    _ => None,
                };
                // Nobody in `operators` typed an echo, so it has no author — only a
                // side.
                let (sender_id, sender_type) = if is_echo {
                    (None, "operator")
                } else {
                    (client_id, "client")
                };

                messages.push(NewMessage {
                    external_message_id,
                    channel_id: channel.id,
                    conversation: client_id.map(Conversation::Customer),
                    sender_id,
                    sender_type: sender_type.into(),
                    provider: ProviderKind::Instagram,
                    event: event_kind,
                    text,
                    raw,
                });
            }
        }

        if messages.is_empty() {
            tracing::warn!(
                entries = payload.entry.len(),
                "instagram webhook produced no messages — nothing will reach the inbox"
            );
        }

        Ok(messages)
    }
}

/// Meta sends the event timestamp as an integer in one payload shape and as a
/// decimal string in the other.
fn deserialize_flexible_timestamp<'de, D>(deserializer: D) -> Result<Option<i64>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Flexible {
        Int(i64),
        Str(String),
    }

    Ok(match Option::<Flexible>::deserialize(deserializer)? {
        None => None,
        Some(Flexible::Int(n)) => Some(n),
        // A timestamp we cannot parse is not worth failing the whole payload for:
        // nothing routes on it, it only rides along in `raw`.
        Some(Flexible::Str(s)) => s.parse().ok(),
    })
}

fn classify_event(event: &MessagingEvent) -> (EventKind, Option<&String>) {
    if let Some(ref msg) = event.message {
        return (EventKind::Message, Some(&msg.mid));
    }
    if let Some(ref edit) = event.message_edit {
        return (EventKind::Edit, Some(&edit.mid));
    }
    if let Some(ref read) = event.read {
        return (EventKind::Read, read.mid.as_ref());
    }
    if let Some(ref reaction) = event.reaction {
        return (EventKind::Reaction, Some(&reaction.mid));
    }
    // Default to Message for unknown event types
    (EventKind::Unknown, None)
}

/// Look up or create a client from the sender field, spawning a background
/// task to fetch the Instagram profile and upsert the client row.
async fn resolve_instagram_client(
    db: &PgPool,
    client_cache: &ClientCache,
    sender: Option<&Participant>,
    access_token: &str,
    graph_base: &str,
    redis: redis::aio::ConnectionManager,
) -> Option<Uuid> {
    let sid = sender?.id.as_str();
    match client_cache
        .get_client(db, ProviderKind::Instagram, sid)
        .await
    {
        Ok(Some(client)) => {
            let age = chrono::Utc::now() - client.updated_at;
            if age > chrono::TimeDelta::hours(24) {
                spawn_instagram_upsert(db.clone(), client.id, sid, access_token, graph_base, redis);
            }
            Some(client.id)
        }
        Ok(None) => {
            // Insert the row SYNCHRONOUSLY. The message is persisted immediately after
            // this returns, and `persist_and_publish` refuses to set `sender_id` — and
            // therefore cannot create the chat — unless the client already exists. So
            // deferring this write to the spawned task loses the chat for the first
            // message of every new conversation, which is the only message that
            // matters for a conversation appearing in the inbox at all.
            //
            // Only the profile lookup stays asynchronous, because that one is a network
            // call to Meta. `upsert_client` returns the effective id, so two messages
            // racing on the same new sender converge on one row.
            let client_id = match crate::db::upsert_client(
                db,
                Uuid::new_v4(),
                ProviderKind::Instagram,
                sid,
                None,
                None,
            )
            .await
            {
                Ok(id) => id,
                Err(e) => {
                    tracing::error!(%sid, "instagram client insert failed: {e}");
                    return None;
                }
            };
            spawn_instagram_upsert(db.clone(), client_id, sid, access_token, graph_base, redis);
            Some(client_id)
        }
        Err(e) => {
            tracing::error!("instagram client lookup failed: {e}");
            None
        }
    }
}

fn spawn_instagram_upsert(
    db: PgPool,
    client_id: Uuid,
    sid: &str,
    access_token: &str,
    graph_base: &str,
    redis: redis::aio::ConnectionManager,
) {
    let sid = sid.to_owned();
    let token = access_token.to_owned();
    let graph_base = graph_base.to_owned();
    tokio::spawn(fetch_and_upsert_instagram_client(
        db, client_id, sid, token, graph_base, redis,
    ));
}

/// Fetch Instagram profile and upsert client. Always creates the client row,
/// even if the API call fails (with name/username as NULL).
async fn fetch_and_upsert_instagram_client(
    db: PgPool,
    client_id: Uuid,
    sender_id: String,
    access_token: String,
    graph_base: String,
    mut redis: redis::aio::ConnectionManager,
) {
    let url = format!("{graph_base}/{sender_id}?fields=username,name&access_token={access_token}");

    let (name, username) = match HTTP_CLIENT.get(&url).send().await {
        Ok(resp) if resp.status().is_success() => match resp.json::<InstagramProfile>().await {
            Ok(profile) => (profile.name, profile.username),
            Err(e) => {
                tracing::warn!(%sender_id, "failed to parse instagram profile: {e}");
                (None, None)
            }
        },
        Ok(resp) => {
            tracing::warn!(%sender_id, status = %resp.status(), "instagram profile API error");
            (None, None)
        }
        Err(e) => {
            tracing::warn!(%sender_id, "instagram profile API request failed: {e}");
            (None, None)
        }
    };

    if let Err(e) = crate::db::upsert_client(
        &db,
        client_id,
        ProviderKind::Instagram,
        &sender_id,
        name.as_deref(),
        username.as_deref(),
    )
    .await
    {
        tracing::error!(%sender_id, "instagram client upsert failed: {e}");
    } else {
        crate::cache::publish_invalidation(&mut redis, "client", client_id).await;
    }
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
    /// Graph API v25.0 and earlier deliver messaging events here.
    pub messaging: Option<Vec<MessagingEvent>>,
    /// v26.0 delivers the *same* event objects here instead, each wrapped in a
    /// `{field, value}` change. Both are accepted: the sibling PHP integration runs
    /// on v25.0 and still receives `messaging`, so this is not a migration.
    pub changes: Option<Vec<Change>>,
    /// Whatever else the entry carried. Captured so an unhandled payload shape can
    /// be *named* in the log instead of vanishing — a bare `continue` on an
    /// unrecognised shape is indistinguishable from Meta sending nothing at all.
    #[serde(flatten)]
    pub other: serde_json::Map<String, serde_json::Value>,
}

/// A v26.0 change. `field` names the subscribed field that produced it
/// (`messages`, `message_edit`, …) and `value` is shaped exactly like an element
/// of the older `messaging` array.
#[derive(Debug, Deserialize, serde::Serialize)]
pub struct Change {
    pub field: String,
    pub value: MessagingEvent,
}

#[derive(Debug, Deserialize, serde::Serialize)]
pub struct MessagingEvent {
    pub sender: Option<Participant>,
    pub recipient: Option<Participant>,
    /// A number under `messaging`, a string under `changes[].value`. Accept both:
    /// rejecting one makes the whole payload fail to deserialize.
    #[serde(default, deserialize_with = "deserialize_flexible_timestamp")]
    pub timestamp: Option<i64>,
    pub message: Option<Message>,
    pub message_edit: Option<MessageEdit>,
    pub read: Option<ReadReceipt>,
    pub reaction: Option<Reaction>,
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
    /// `true` when the event is a copy of a message this app itself sent. Routing
    /// already drops these — an echo's `recipient` is the customer, so no channel
    /// matches — but that path logs a warning, and an operator's own replies coming
    /// back as "no channel found" is noise that hides real misroutes.
    #[serde(default)]
    pub is_echo: bool,
}

#[derive(Debug, Deserialize, serde::Serialize)]
pub struct MessageEdit {
    pub mid: String,
    pub text: Option<String>,
    pub num_edit: Option<i32>,
}

/// Instagram sends `mid`; a receipt without one is not worth failing the whole
/// payload for — the pipeline falls back to the chat.
#[derive(Debug, Deserialize, serde::Serialize)]
pub struct ReadReceipt {
    pub mid: Option<String>,
}

#[derive(Debug, Deserialize, serde::Serialize)]
pub struct Reaction {
    pub mid: String,
    pub action: String,
    pub reaction: Option<String>,
    pub emoji: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_read_receipt_without_a_mid_is_still_a_read() {
        // A required `mid` would fail `from_slice` for the *whole* payload, taking
        // every event batched alongside it down with it.
        let event: MessagingEvent = serde_json::from_value(serde_json::json!({
            "sender": {"id": "customer"},
            "recipient": {"id": "account"},
            "read": {}
        }))
        .unwrap();
        let (kind, mid) = classify_event(&event);
        assert!(matches!(kind, EventKind::Read));
        assert!(
            mid.is_none(),
            "no anchor — the pipeline falls back to the chat"
        );
    }
    use crate::cache::{ChannelCache, ClientCache};
    use crate::provider::WebhookProvider;
    use hmac::{Hmac, Mac};
    use sha2::Sha256;

    const TEST_SECRET: &str = "test_secret_key";

    fn test_provider() -> InstagramProvider {
        InstagramProvider::new(
            TEST_SECRET,
            "http://127.0.0.1:1",
            Arc::new(ChannelCache::new()),
            Arc::new(ClientCache::new()),
        )
    }

    fn sign(secret: &str, body: &[u8]) -> String {
        let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).expect("valid key length");
        mac.update(body);
        hex::encode(mac.finalize().into_bytes())
    }

    #[test]
    fn verify_valid_signature() {
        let provider = test_provider();
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
        let provider = test_provider();
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
        let provider = test_provider();
        let headers = HeaderMap::new();

        let err = provider.verify(&headers, b"body").unwrap_err();
        assert!(err.to_string().contains("missing X-Hub-Signature-256"));
    }

    #[test]
    fn verify_missing_sha256_prefix() {
        let provider = test_provider();
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
                is_echo: false,
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
    fn an_echo_is_recognised_from_the_payload() {
        let payload = r#"{
            "object": "instagram",
            "entry": [{
                "id": "17841448717199999",
                "time": 1,
                "messaging": [{
                    "sender": {"id": "17841448717199999"},
                    "recipient": {"id": "836189122827510"},
                    "message": {"mid": "m", "text": "our own reply", "is_echo": true}
                }]
            }]
        }"#;
        let parsed: MetaWebhookPayload = serde_json::from_str(payload).unwrap();
        let event = &parsed.entry[0].messaging.as_ref().unwrap()[0];
        assert!(event.message.as_ref().unwrap().is_echo);
    }

    #[test]
    fn an_absent_is_echo_defaults_to_false() {
        // Every real inbound message omits the field entirely.
        let msg: Message =
            serde_json::from_value(serde_json::json!({"mid": "m", "text": "hi"})).unwrap();
        assert!(!msg.is_echo);
    }

    #[test]
    fn classify_read_event() {
        let event = MessagingEvent {
            sender: None,
            recipient: None,
            timestamp: Some(123),
            message: None,
            message_edit: None,
            read: Some(ReadReceipt {
                mid: Some("mid_005".into()),
            }),
            reaction: None,
        };
        let (kind, mid) = classify_event(&event);
        assert!(matches!(kind, EventKind::Read));
        assert_eq!(mid.unwrap(), "mid_005");
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
            reaction: Some(Reaction {
                mid: "mid_003".into(),
                action: "react".into(),
                reaction: Some("love".into()),
                emoji: Some("\u{2764}".into()),
            }),
        };
        let (kind, mid) = classify_event(&event);
        assert!(matches!(kind, EventKind::Reaction));
        assert_eq!(mid.unwrap(), "mid_003");
    }

    #[test]
    fn classify_unreact_event() {
        let event = MessagingEvent {
            sender: None,
            recipient: None,
            timestamp: Some(123),
            message: None,
            message_edit: None,
            read: None,
            reaction: Some(Reaction {
                mid: "mid_004".into(),
                action: "unreact".into(),
                reaction: None,
                emoji: None,
            }),
        };
        let (kind, mid) = classify_event(&event);
        assert!(matches!(kind, EventKind::Reaction));
        assert_eq!(mid.unwrap(), "mid_004");
    }

    #[test]
    fn a_v26_change_carries_the_same_event_object_as_messaging() {
        // v26.0 stopped sending `messaging` and started sending the identical event
        // under `changes[].value`, with `timestamp` as a *string*. Parsing only the
        // old shape is why a real DM produced nothing at all.
        let payload = r#"{
            "object": "instagram",
            "entry": [{
                "id": "17841448717199999",
                "time": 1773347860136,
                "changes": [{
                    "field": "messages",
                    "value": {
                        "sender": {"id": "12334"},
                        "recipient": {"id": "23245"},
                        "timestamp": "1527459824",
                        "message": {"mid": "random_mid", "text": "random_text"}
                    }
                }]
            }]
        }"#;

        let parsed: MetaWebhookPayload = serde_json::from_str(payload).unwrap();
        let entry = &parsed.entry[0];
        assert!(entry.messaging.is_none(), "v26 sends no messaging array");
        let change = &entry.changes.as_ref().unwrap()[0];
        assert_eq!(change.field, "messages");
        assert_eq!(change.value.sender.as_ref().unwrap().id, "12334");
        assert_eq!(change.value.recipient.as_ref().unwrap().id, "23245");
        assert_eq!(
            change.value.timestamp,
            Some(1527459824),
            "a string timestamp must not fail the payload"
        );

        let (kind, mid) = classify_event(&change.value);
        assert!(matches!(kind, EventKind::Message));
        assert_eq!(mid.unwrap(), "random_mid");
    }

    #[test]
    fn a_numeric_timestamp_still_parses() {
        let event: MessagingEvent = serde_json::from_value(serde_json::json!({
            "sender": {"id": "1"}, "recipient": {"id": "2"}, "timestamp": 1527459824i64,
            "message": {"mid": "m", "text": "t"}
        }))
        .unwrap();
        assert_eq!(event.timestamp, Some(1527459824));
    }

    #[test]
    fn an_uninterpretable_change_is_named_not_dropped_silently() {
        // A comment, or the App Dashboard's Test button: the value carries none of
        // message / message_edit / read / reaction, so it must not become an inbox
        // entry — but the field name has to reach the log.
        let payload = r#"{
            "object": "instagram",
            "entry": [{
                "id": "17841448717199999",
                "time": 1773347860136,
                "changes": [{"field": "comments", "value": {"foo": "bar"}}]
            }]
        }"#;

        let parsed: MetaWebhookPayload = serde_json::from_str(payload).unwrap();
        let change = &parsed.entry[0].changes.as_ref().unwrap()[0];
        assert_eq!(change.field, "comments");
        assert!(
            matches!(classify_event(&change.value).0, EventKind::Unknown),
            "nothing classifiable, so the parser must skip it"
        );
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
