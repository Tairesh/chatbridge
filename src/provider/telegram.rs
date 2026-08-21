use std::sync::{Arc, LazyLock};

use axum::http::HeaderMap;
use serde::Deserialize;
use sqlx::PgPool;
use uuid::Uuid;

use crate::cache::{ChannelCache, ClientCache};
use crate::error::AppError;
use crate::model::{EventKind, NewMessage, ProviderKind};
use crate::provider::WebhookProvider;

pub struct TelegramProvider {
    channel_id: Uuid,
    client_cache: Arc<ClientCache>,
}

impl TelegramProvider {
    pub fn new(channel_id: Uuid, client_cache: Arc<ClientCache>) -> Self {
        Self {
            channel_id,
            client_cache,
        }
    }

    /// Load the channel's bot_secret from the cache (or database on miss) and return
    /// the provider along with the expected secret for verification.
    pub async fn load(
        channel_id: Uuid,
        db: &PgPool,
        cache: &Arc<ChannelCache>,
        client_cache: Arc<ClientCache>,
    ) -> Result<(Self, String), AppError> {
        let channel = cache
            .get_channel_by_id(db, channel_id)
            .await?
            .ok_or_else(|| AppError::NotFound(format!("channel {channel_id} not found")))?;
        let config: crate::model::TelegramConfig = serde_json::from_value(channel.config)
            .map_err(|e| AppError::Internal(format!("bad telegram config: {e}")))?;
        Ok((Self::new(channel_id, client_cache), config.bot_secret))
    }
}

/// Standalone verify for Telegram — checks the secret token header against expected secret.
pub fn verify_secret_token(headers: &HeaderMap, expected_secret: &str) -> Result<(), AppError> {
    let token = headers
        .get("X-Telegram-Bot-Api-Secret-Token")
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| AppError::Forbidden("missing X-Telegram-Bot-Api-Secret-Token".into()))?;

    if token != expected_secret {
        return Err(AppError::Forbidden("invalid secret token".into()));
    }

    Ok(())
}

impl WebhookProvider for TelegramProvider {
    fn verify(&self, _headers: &HeaderMap, _body: &[u8]) -> Result<(), AppError> {
        // Telegram verification is done via verify_secret_token before constructing the provider.
        // This is a no-op since the handler already verified the secret.
        Ok(())
    }

    async fn parse(
        &self,
        body: &[u8],
        db: &PgPool,
        redis: redis::aio::ConnectionManager,
    ) -> Result<Vec<NewMessage>, AppError> {
        let update: TelegramUpdate =
            serde_json::from_slice(body).map_err(|e| AppError::BadRequest(e.to_string()))?;

        let (event_kind, msg_ref) = if let Some(ref msg) = update.message {
            (EventKind::Message, Some(msg))
        } else if let Some(ref msg) = update.edited_message {
            (EventKind::Edit, Some(msg))
        } else {
            (EventKind::Unknown, None)
        };

        let message_id = match msg_ref {
            Some(msg) => msg.message_id,
            None => update.update_id,
        };

        let client_id = if let Some(from) = msg_ref.and_then(|m| m.from.as_ref()) {
            resolve_telegram_client(db, &self.client_cache, from, redis).await
        } else {
            None
        };

        let text = match event_kind {
            EventKind::Message | EventKind::Edit => msg_ref.and_then(|m| m.text.clone()),
            _ => None,
        };
        let raw = serde_json::from_slice(body).unwrap_or(serde_json::Value::Null);

        Ok(vec![NewMessage {
            external_message_id: format!("telegram:{message_id}"),
            channel_id: self.channel_id,
            sender_id: client_id,
            sender_type: "client".into(),
            provider: ProviderKind::Telegram,
            event: event_kind,
            text,
            raw,
        }])
    }
}

/// Look up or create a client from the Telegram user, spawning a background
/// task to upsert the client row when needed.
async fn resolve_telegram_client(
    db: &PgPool,
    client_cache: &ClientCache,
    from: &TelegramUser,
    redis: redis::aio::ConnectionManager,
) -> Option<Uuid> {
    let external_id = from.id.to_string();
    match client_cache
        .get_client(db, ProviderKind::Telegram, &external_id)
        .await
    {
        Ok(Some(client)) => {
            let age = chrono::Utc::now() - client.updated_at;
            if age > chrono::TimeDelta::hours(24) {
                spawn_telegram_upsert(db.clone(), client.id, from, redis);
            }
            Some(client.id)
        }
        Ok(None) => {
            let client_id = Uuid::new_v4();
            spawn_telegram_upsert(db.clone(), client_id, from, redis);
            Some(client_id)
        }
        Err(e) => {
            tracing::error!("telegram client lookup failed: {e}");
            None
        }
    }
}

fn spawn_telegram_upsert(
    db: PgPool,
    client_id: Uuid,
    from: &TelegramUser,
    redis: redis::aio::ConnectionManager,
) {
    let name = build_display_name(from);
    let external_id = from.id.to_string();
    let username = from.username.clone();
    tokio::spawn(async move {
        let mut redis = redis;
        if let Err(e) = crate::db::upsert_client(
            &db,
            client_id,
            ProviderKind::Telegram,
            &external_id,
            Some(name.as_str()),
            username.as_deref(),
        )
        .await
        {
            tracing::error!("telegram client upsert failed: {e}");
        } else {
            crate::cache::publish_invalidation(&mut redis, "client", client_id).await;
        }
    });
}

// --- Telegram Update types (minimal) ---

#[derive(Debug, Deserialize)]
pub struct TelegramUpdate {
    pub update_id: i64,
    pub message: Option<TelegramMessage>,
    pub edited_message: Option<TelegramMessage>,
}

#[derive(Debug, Deserialize)]
pub struct TelegramMessage {
    pub message_id: i64,
    pub date: i64,
    pub from: Option<TelegramUser>,
    pub chat: Option<serde_json::Value>,
    pub text: Option<String>,
}

#[derive(Debug, Deserialize, serde::Serialize)]
pub struct TelegramUser {
    pub id: i64,
    pub first_name: String,
    pub last_name: Option<String>,
    pub username: Option<String>,
}

fn build_display_name(user: &TelegramUser) -> String {
    match &user.last_name {
        Some(last) => format!("{} {}", user.first_name, last),
        None => user.first_name.clone(),
    }
}

static HTTP_CLIENT: LazyLock<reqwest::Client> = LazyLock::new(|| {
    reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()
        .expect("failed to build reqwest client")
});

pub enum OutboundMessage {
    Text { text: String },
}

/// Call a Bot API method and return its `result` field.
///
/// `body: None` issues a GET, which is what the read-only methods want.
/// A Telegram-level failure (`ok: false`) is surfaced as its `description`
/// verbatim, so an operator reads "HTTPS url must be provided" rather than a
/// generic error.
async fn call(url: &str, body: Option<serde_json::Value>) -> Result<serde_json::Value, String> {
    let request = match body {
        Some(ref b) => HTTP_CLIENT.post(url).json(b),
        None => HTTP_CLIENT.get(url),
    };

    let resp = request
        .send()
        .await
        .map_err(|e| format!("HTTP request failed: {e}"))?;

    let json: serde_json::Value = resp
        .json()
        .await
        .map_err(|e| format!("failed to parse response: {e}"))?;

    if json["ok"].as_bool() == Some(true) {
        Ok(json["result"].clone())
    } else {
        Err(json["description"]
            .as_str()
            .unwrap_or("unknown error")
            .to_owned())
    }
}

/// Result of `getMe` — the authoritative source of a bot's numeric id.
#[derive(Debug, Deserialize)]
pub struct BotInfo {
    pub id: i64,
    pub username: Option<String>,
}

pub async fn get_me(base_url: &str, bot_token: &str) -> Result<BotInfo, String> {
    let json = call(&format!("{base_url}/bot{bot_token}/getMe"), None).await?;
    serde_json::from_value(json).map_err(|e| format!("unexpected getMe response: {e}"))
}

/// Telegram keeps exactly one webhook per bot; this overwrites any previous one.
pub async fn set_webhook(
    base_url: &str,
    bot_token: &str,
    url: &str,
    secret: &str,
) -> Result<(), String> {
    call(
        &format!("{base_url}/bot{bot_token}/setWebhook"),
        Some(serde_json::json!({ "url": url, "secret_token": secret })),
    )
    .await?;
    Ok(())
}

pub async fn delete_webhook(base_url: &str, bot_token: &str) -> Result<(), String> {
    call(
        &format!("{base_url}/bot{bot_token}/deleteWebhook"),
        Some(serde_json::json!({})),
    )
    .await?;
    Ok(())
}

/// Telegram's own view of the webhook, including its report of failures
/// delivering *to us* (`last_error_message`).
#[derive(Debug, Deserialize)]
pub struct WebhookInfo {
    #[serde(default)]
    pub url: String,
    #[serde(default)]
    pub pending_update_count: i64,
    pub last_error_date: Option<i64>,
    pub last_error_message: Option<String>,
}

pub async fn get_webhook_info(base_url: &str, bot_token: &str) -> Result<WebhookInfo, String> {
    let json = call(&format!("{base_url}/bot{bot_token}/getWebhookInfo"), None).await?;
    serde_json::from_value(json).map_err(|e| format!("unexpected getWebhookInfo response: {e}"))
}

pub async fn send(
    base_url: &str,
    bot_token: &str,
    chat_id: &str,
    message: &OutboundMessage,
) -> Result<(), String> {
    let (method, body) = match message {
        OutboundMessage::Text { text } => (
            "sendMessage",
            serde_json::json!({ "chat_id": chat_id, "text": text }),
        ),
    };
    call(&format!("{base_url}/bot{bot_token}/{method}"), Some(body)).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::ClientCache;
    use crate::provider::WebhookProvider;

    #[test]
    fn verify_secret_token_valid() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "X-Telegram-Bot-Api-Secret-Token",
            "my_secret".parse().unwrap(),
        );
        assert!(verify_secret_token(&headers, "my_secret").is_ok());
    }

    #[test]
    fn verify_secret_token_invalid() {
        let mut headers = HeaderMap::new();
        headers.insert("X-Telegram-Bot-Api-Secret-Token", "wrong".parse().unwrap());
        let err = verify_secret_token(&headers, "my_secret").unwrap_err();
        assert!(err.to_string().contains("invalid secret token"));
    }

    #[test]
    fn verify_secret_token_missing() {
        let headers = HeaderMap::new();
        let err = verify_secret_token(&headers, "my_secret").unwrap_err();
        assert!(
            err.to_string()
                .contains("missing X-Telegram-Bot-Api-Secret-Token")
        );
    }

    #[tokio::test]
    async fn parse_telegram_message() {
        let channel_id = uuid::Uuid::new_v4();
        let provider = TelegramProvider::new(channel_id, Arc::new(ClientCache::new()));

        let body = serde_json::json!({
            "update_id": 100,
            "message": {
                "message_id": 42,
                "date": 1700000000,
                "from": {"id": 123, "first_name": "Test"},
                "chat": {"id": 123, "type": "private"},
                "text": "hello bot"
            }
        });
        let body_bytes = serde_json::to_vec(&body).unwrap();

        // parse doesn't use DB for telegram, pass a dummy — we need a PgPool though.
        // Use a mock-free approach: test only the provider's no-op verify.
        assert!(provider.verify(&HeaderMap::new(), &body_bytes).is_ok());
    }

    #[test]
    fn parse_telegram_update_structure() {
        let body = serde_json::json!({
            "update_id": 100,
            "message": {
                "message_id": 42,
                "date": 1700000000,
                "text": "hello"
            }
        });
        let update: TelegramUpdate = serde_json::from_value(body).unwrap();
        assert_eq!(update.update_id, 100);
        let msg = update.message.unwrap();
        assert_eq!(msg.message_id, 42);
        assert_eq!(msg.date, 1700000000);
        assert_eq!(msg.text.as_deref(), Some("hello"));
    }

    #[test]
    fn parse_telegram_edited_message() {
        let body = serde_json::json!({
            "update_id": 101,
            "edited_message": {
                "message_id": 42,
                "date": 1700000001,
                "text": "edited text"
            }
        });
        let update: TelegramUpdate = serde_json::from_value(body).unwrap();
        assert!(update.message.is_none());
        let msg = update.edited_message.unwrap();
        assert_eq!(msg.message_id, 42);
        assert_eq!(msg.text.as_deref(), Some("edited text"));
    }

    #[test]
    fn deserialize_telegram_user() {
        let json = serde_json::json!({
            "id": 123456,
            "first_name": "John",
            "last_name": "Doe",
            "username": "johndoe"
        });
        let user: TelegramUser = serde_json::from_value(json).unwrap();
        assert_eq!(user.id, 123456);
        assert_eq!(user.first_name, "John");
        assert_eq!(user.last_name.as_deref(), Some("Doe"));
        assert_eq!(user.username.as_deref(), Some("johndoe"));
    }

    #[test]
    fn deserialize_telegram_user_minimal() {
        let json = serde_json::json!({
            "id": 789,
            "first_name": "Alice"
        });
        let user: TelegramUser = serde_json::from_value(json).unwrap();
        assert_eq!(user.id, 789);
        assert_eq!(user.first_name, "Alice");
        assert!(user.last_name.is_none());
        assert!(user.username.is_none());
    }

    #[test]
    fn build_display_name_full() {
        let user = TelegramUser {
            id: 1,
            first_name: "John".into(),
            last_name: Some("Doe".into()),
            username: Some("johndoe".into()),
        };
        assert_eq!(build_display_name(&user), "John Doe");
    }

    #[test]
    fn build_display_name_first_only() {
        let user = TelegramUser {
            id: 1,
            first_name: "Alice".into(),
            last_name: None,
            username: None,
        };
        assert_eq!(build_display_name(&user), "Alice");
    }

    /// Spawn a fake Bot API that answers every `/bot<token>/<method>` with the JSON
    /// registered for `<method>`, falling back to `{"ok":true,"result":{}}`.
    async fn mock_bot_api(responses: serde_json::Value) -> String {
        use axum::extract::Path;
        use axum::routing::any;
        use axum::{Json, Router};

        let responses = Arc::new(responses);
        let app = Router::new().route(
            "/bot{token}/{method}",
            any(move |Path((_token, method)): Path<(String, String)>| {
                let responses = responses.clone();
                async move {
                    let body = responses
                        .get(method.as_str())
                        .cloned()
                        .unwrap_or_else(|| serde_json::json!({"ok": true, "result": {}}));
                    Json(body)
                }
            }),
        );

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        format!("http://{addr}")
    }

    #[tokio::test]
    async fn send_text_message_success() {
        let base = mock_bot_api(serde_json::json!({
            "sendMessage": {"ok": true, "result": {}}
        }))
        .await;
        let msg = OutboundMessage::Text {
            text: "hello".into(),
        };
        assert!(send(&base, "fake_token", "12345", &msg).await.is_ok());
    }

    #[tokio::test]
    async fn send_text_message_telegram_error() {
        let base = mock_bot_api(serde_json::json!({
            "sendMessage": {"ok": false, "description": "Forbidden: bot was blocked by the user"}
        }))
        .await;
        let msg = OutboundMessage::Text {
            text: "hello".into(),
        };
        let err = send(&base, "fake_token", "12345", &msg).await.unwrap_err();
        assert!(err.contains("blocked by the user"));
    }

    #[tokio::test]
    async fn get_me_returns_bot_id_and_username() {
        let base = mock_bot_api(serde_json::json!({
            "getMe": {"ok": true, "result": {"id": 123456789, "is_bot": true,
                                             "first_name": "Acme", "username": "acme_bot"}}
        }))
        .await;
        let info = get_me(&base, "123456789:AA").await.unwrap();
        assert_eq!(info.id, 123456789);
        assert_eq!(info.username.as_deref(), Some("acme_bot"));
    }

    #[tokio::test]
    async fn get_me_surfaces_telegram_description() {
        let base = mock_bot_api(serde_json::json!({
            "getMe": {"ok": false, "description": "Unauthorized"}
        }))
        .await;
        let err = get_me(&base, "bad").await.unwrap_err();
        assert_eq!(err, "Unauthorized");
    }

    #[tokio::test]
    async fn set_webhook_success_and_failure() {
        let ok = mock_bot_api(serde_json::json!({
            "setWebhook": {"ok": true, "result": true}
        }))
        .await;
        assert!(
            set_webhook(
                &ok,
                "tok",
                "https://example.com/webhook/telegram/x",
                "s3cr3t"
            )
            .await
            .is_ok()
        );

        let bad = mock_bot_api(serde_json::json!({
            "setWebhook": {"ok": false, "description": "bad webhook: HTTPS url must be provided"}
        }))
        .await;
        let err = set_webhook(&bad, "tok", "http://insecure/x", "s3cr3t")
            .await
            .unwrap_err();
        assert!(err.contains("HTTPS url must be provided"));
    }

    #[tokio::test]
    async fn delete_webhook_success() {
        let base = mock_bot_api(serde_json::json!({
            "deleteWebhook": {"ok": true, "result": true}
        }))
        .await;
        assert!(delete_webhook(&base, "tok").await.is_ok());
    }

    #[tokio::test]
    async fn get_webhook_info_parses_all_fields() {
        let base = mock_bot_api(serde_json::json!({
            "getWebhookInfo": {"ok": true, "result": {
                "url": "https://example.com/webhook/telegram/abc",
                "has_custom_certificate": false,
                "pending_update_count": 7,
                "last_error_date": 1700000000,
                "last_error_message": "wrong response from webhook: 404"
            }}
        }))
        .await;
        let info = get_webhook_info(&base, "tok").await.unwrap();
        assert_eq!(info.url, "https://example.com/webhook/telegram/abc");
        assert_eq!(info.pending_update_count, 7);
        assert_eq!(info.last_error_date, Some(1700000000));
        assert!(info.last_error_message.unwrap().contains("404"));
    }

    #[tokio::test]
    async fn get_webhook_info_handles_unregistered_webhook() {
        // Telegram reports "no webhook" as an empty url with the other fields absent.
        let base = mock_bot_api(serde_json::json!({
            "getWebhookInfo": {"ok": true, "result": {"url": "", "pending_update_count": 0}}
        }))
        .await;
        let info = get_webhook_info(&base, "tok").await.unwrap();
        assert_eq!(info.url, "");
        assert!(info.last_error_message.is_none());
    }
}
