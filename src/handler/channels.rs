//! Channel CRUD for the settings panel.

use std::sync::Arc;

use axum::Json;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use uuid::Uuid;

use crate::config::AppState;
use crate::db::{self, Channel};
use crate::error::WebhookError;
use crate::model::{ChannelSpec, ProviderKind, TelegramConfig};

const WIDGET_ID_MAX: usize = 64;

/// `widget_id` is substituted into the `/ws/{widget_id}` route, so it has to be a
/// safe single path segment. A value with spaces, slashes or non-ASCII characters
/// would create a channel the widget could never connect to — better to refuse it
/// here than to hand back something that looks fine and silently does not work.
pub fn validate_widget_id(widget_id: &str) -> Result<(), WebhookError> {
    if widget_id.is_empty() || widget_id.len() > WIDGET_ID_MAX {
        return Err(WebhookError::BadRequest(format!(
            "widget_id must be 1-{WIDGET_ID_MAX} characters"
        )));
    }
    if !widget_id
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
    {
        return Err(WebhookError::BadRequest(
            "widget_id may contain only letters, digits, '-' and '_'".into(),
        ));
    }
    Ok(())
}

/// A Telegram bot token is `<numeric bot id>:<secret>`. Checking the shape here
/// turns an obvious typo into a clear 400 instead of a round trip to Telegram.
pub fn validate_bot_token(bot_token: &str) -> Result<(), WebhookError> {
    let malformed =
        || WebhookError::BadRequest("bot_token must look like <bot_id>:<secret>".to_owned());
    let (id, secret) = bot_token.split_once(':').ok_or_else(malformed)?;
    if id.is_empty() || secret.is_empty() || !id.bytes().all(|b| b.is_ascii_digit()) {
        return Err(malformed());
    }
    Ok(())
}

/// The address the provider talks to for this channel. Derived, never stored.
pub fn endpoint_for(base: &str, provider: &str, id: Uuid, external_key: &str) -> String {
    match provider {
        "telegram" => format!("{base}/webhook/telegram/{id}"),
        "widget" => {
            // Only one of these two can match, so the order is irrelevant.
            let ws_base = base
                .replacen("https://", "wss://", 1)
                .replacen("http://", "ws://", 1);
            format!("{ws_base}/ws/{external_key}")
        }
        _ => format!("{base}/webhook/instagram"),
    }
}

/// A channel as the settings panel sees it. `config` is returned in full,
/// secrets included — a deliberate decision recorded in docs/tech_debt.md.
#[derive(Debug, Serialize)]
pub struct ChannelView {
    pub id: Uuid,
    pub provider: String,
    pub name: String,
    pub external_key: String,
    pub config: serde_json::Value,
    /// Derived from `id` and `external_key`, never stored.
    pub endpoint: String,
    pub deleted_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
}

impl ChannelView {
    pub fn new(public_base_url: &str, ch: Channel) -> Self {
        let endpoint = endpoint_for(public_base_url, &ch.provider, ch.id, &ch.external_key);
        Self {
            id: ch.id,
            provider: ch.provider,
            name: ch.name,
            external_key: ch.external_key,
            config: ch.config,
            endpoint,
            deleted_at: ch.deleted_at,
            created_at: ch.created_at,
        }
    }
}

/// Every channel, live ones first. Deleted channels are included so the panel
/// can show them dimmed with a Restore button rather than lying about the database.
pub async fn list(
    State(state): State<Arc<AppState>>,
) -> Result<Json<Vec<ChannelView>>, WebhookError> {
    let channels = db::list_channels(&state.db).await?;
    let base = &state.config.public_base_url;
    Ok(Json(
        channels
            .into_iter()
            .map(|c| ChannelView::new(base, c))
            .collect(),
    ))
}

/// Body of `POST /api/channels`: a `ChannelSpec` with an optional display name
/// flattened alongside it.
#[derive(Debug, Deserialize)]
pub struct CreateChannel {
    #[serde(default)]
    pub name: Option<String>,
    #[serde(flatten)]
    pub spec: ChannelSpec,
}

/// Build the 409 body for a create that collided with the unique index.
/// Distinguishes a live collision from a soft-deleted one, because the panel
/// offers "Restore" for the second and only highlights the row for the first.
async fn conflict(db: &PgPool, provider: ProviderKind, external_key: &str) -> WebhookError {
    match db::find_channel_by_external_key(db, provider, external_key).await {
        Ok(Some(existing)) => {
            let code = if existing.deleted_at.is_some() {
                "channel_deleted"
            } else {
                "channel_exists"
            };
            WebhookError::Conflict(serde_json::json!({
                "error": code,
                "channel_id": existing.id,
                "name": existing.name,
                "deleted_at": existing.deleted_at,
            }))
        }
        // The index rejected the insert, so a row must exist. If it does not, the
        // only explanation is a concurrent hard delete, which is not a client error.
        Ok(None) => WebhookError::Internal("conflicting channel disappeared".into()),
        Err(e) => WebhookError::from(e),
    }
}

/// Insert, translating the unique-index violation into a 409. The index is the
/// sole arbiter of duplicates: a pre-check `SELECT` would be racy, and for
/// telegram it would also let two concurrent creators both call `setWebhook`.
async fn insert_or_conflict(
    db: &PgPool,
    id: Uuid,
    provider: ProviderKind,
    name: &str,
    external_key: &str,
    config: &serde_json::Value,
) -> Result<Channel, WebhookError> {
    match db::insert_channel(db, id, provider.clone(), name, external_key, config).await {
        Ok(channel) => Ok(channel),
        Err(sqlx::Error::Database(ref e)) if e.is_unique_violation() => {
            Err(conflict(db, provider, external_key).await)
        }
        Err(e) => Err(e.into()),
    }
}

/// Evict the channel from this replica's cache and tell the others to do the same.
async fn invalidate_everywhere(state: &AppState, channel_id: Uuid) {
    state.cache.invalidate(channel_id);
    let mut redis = state.redis.clone();
    crate::cache::publish_invalidation(&mut redis, "channel", channel_id).await;
}

pub async fn create(
    State(state): State<Arc<AppState>>,
    Json(body): Json<CreateChannel>,
) -> Result<(StatusCode, Json<ChannelView>), WebhookError> {
    let channel = match body.spec {
        ChannelSpec::Widget { widget_id } => {
            validate_widget_id(&widget_id)?;
            let name = body.name.unwrap_or_else(|| widget_id.clone());
            insert_or_conflict(
                &state.db,
                Uuid::new_v4(),
                ProviderKind::Widget,
                &name,
                &widget_id,
                &serde_json::json!({}),
            )
            .await?
        }
        ChannelSpec::Instagram {
            user_id,
            access_token,
        } => {
            if user_id.trim().is_empty() || access_token.trim().is_empty() {
                return Err(WebhookError::BadRequest(
                    "user_id and access_token must not be empty".into(),
                ));
            }
            let name = body.name.unwrap_or_else(|| format!("instagram:{user_id}"));
            insert_or_conflict(
                &state.db,
                Uuid::new_v4(),
                ProviderKind::Instagram,
                &name,
                &user_id,
                &serde_json::json!({"access_token": access_token}),
            )
            .await?
        }
        ChannelSpec::Telegram { bot_token } => {
            create_telegram(&state, body.name, bot_token).await?
        }
    };

    invalidate_everywhere(&state, channel.id).await;
    Ok((
        StatusCode::CREATED,
        Json(ChannelView::new(&state.config.public_base_url, channel)),
    ))
}

/// Create a telegram channel.
///
/// The order matters. `INSERT` comes before `setWebhook` so the unique index is
/// the sole arbiter of duplicates: Telegram keeps exactly one webhook per bot and
/// `setWebhook` overwrites it silently, so a check-then-register order would let
/// two concurrent creators both register, repointing a live channel's webhook
/// before one of them lost at the index.
async fn create_telegram(
    state: &AppState,
    name: Option<String>,
    bot_token: String,
) -> Result<Channel, WebhookError> {
    validate_bot_token(&bot_token)?;

    // getMe validates the token and is the authoritative source of the bot id.
    let info = crate::provider::telegram::get_me(&state.config.telegram_api_base, &bot_token)
        .await
        .map_err(WebhookError::BadRequest)?;
    let external_key = info.id.to_string();
    let name = name.unwrap_or_else(|| match info.username {
        Some(ref username) => format!("@{username}"),
        None => format!("telegram:{external_key}"),
    });

    // Generated up front: the webhook URL embeds the channel id.
    let channel_id = Uuid::new_v4();
    // 32 hex chars, 122 bits, inside Telegram's permitted secret_token charset.
    let bot_secret = Uuid::new_v4().simple().to_string();
    let config = serde_json::json!({ "bot_token": bot_token, "bot_secret": bot_secret });

    let channel = insert_or_conflict(
        &state.db,
        channel_id,
        ProviderKind::Telegram,
        &name,
        &external_key,
        &config,
    )
    .await?;

    let url = endpoint_for(
        &state.config.public_base_url,
        "telegram",
        channel_id,
        &external_key,
    );
    if let Err(reason) = crate::provider::telegram::set_webhook(
        &state.config.telegram_api_base,
        &bot_token,
        &url,
        &bot_secret,
    )
    .await
    {
        // Roll back PHYSICALLY. The row is milliseconds old, so no chats or
        // messages can reference it, and the unique index has no deleted_at
        // filter — a soft delete would occupy this bot's identity forever and
        // force the operator to "restore" a channel that never worked.
        if let Err(e) = db::hard_delete_channel(&state.db, channel_id).await {
            tracing::error!(
                %channel_id,
                "setWebhook failed AND the rollback failed: {e}. The channel is live \
                 with no webhook — re-register it from the settings panel."
            );
        }
        return Err(WebhookError::BadGateway(format!(
            "setWebhook failed: {reason}"
        )));
    }

    Ok(channel)
}

/// Body of `PATCH /api/channels/{id}`.
///
/// `restore` rather than a writable `deleted_at`: a client expresses intent, it
/// does not write timestamp columns.
#[derive(Debug, Deserialize)]
pub struct UpdateChannel {
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub restore: bool,
    #[serde(default)]
    pub spec: Option<ChannelSpec>,
}

pub async fn update(
    State(state): State<Arc<AppState>>,
    Path(channel_id): Path<Uuid>,
    Json(body): Json<UpdateChannel>,
) -> Result<Json<ChannelView>, WebhookError> {
    let existing = db::find_channel_by_id(&state.db, channel_id)
        .await?
        .ok_or_else(|| WebhookError::NotFound("channel not found".into()))?;

    let mut new_key: Option<String> = None;
    let mut new_config: Option<serde_json::Value> = None;
    // Set when telegram needs its webhook (re)registered after this update.
    let mut register: Option<(String, String)> = None;

    if let Some(spec) = body.spec {
        let provider = spec.provider();
        if provider.to_string() != existing.provider {
            return Err(WebhookError::BadRequest(format!(
                "channel provider is '{}' and cannot be changed to '{provider}'",
                existing.provider
            )));
        }

        match spec {
            ChannelSpec::Widget { widget_id } => {
                validate_widget_id(&widget_id)?;
                if widget_id != existing.external_key {
                    new_key = Some(widget_id);
                }
            }
            ChannelSpec::Instagram {
                user_id,
                access_token,
            } => {
                if user_id.trim().is_empty() || access_token.trim().is_empty() {
                    return Err(WebhookError::BadRequest(
                        "user_id and access_token must not be empty".into(),
                    ));
                }
                if user_id != existing.external_key {
                    new_key = Some(user_id);
                }
                // Merge rather than replace. The stored blob also carries
                // `refresh_time`, which the form never shows, so a wholesale
                // rewrite would silently discard it on every Save.
                let mut merged = match existing.config.clone() {
                    serde_json::Value::Object(map) => map,
                    _ => serde_json::Map::new(),
                };
                merged.insert(
                    "access_token".into(),
                    serde_json::Value::String(access_token),
                );
                new_config = Some(serde_json::Value::Object(merged));
            }
            ChannelSpec::Telegram { bot_token } => {
                validate_bot_token(&bot_token)?;
                let info =
                    crate::provider::telegram::get_me(&state.config.telegram_api_base, &bot_token)
                        .await
                        .map_err(WebhookError::BadRequest)?;
                if info.id.to_string() != existing.external_key {
                    return Err(WebhookError::BadRequest(
                        "that token belongs to a different bot; create a separate channel \
                         instead, because this channel's chats and messages belong to the \
                         current one"
                            .into(),
                    ));
                }

                let old: TelegramConfig = serde_json::from_value(existing.config.clone())
                    .map_err(|e| WebhookError::Internal(format!("bad telegram config: {e}")))?;

                // Best effort: drop the replaced token's webhook. A token that was
                // already revoked fails here, which must not fail the edit.
                // Written as a let-chain (edition 2024) because the nested form trips
                // `clippy::collapsible_if`.
                if old.bot_token != bot_token
                    && let Err(e) = crate::provider::telegram::delete_webhook(
                        &state.config.telegram_api_base,
                        &old.bot_token,
                    )
                    .await
                {
                    tracing::warn!(%channel_id, "deleteWebhook on the replaced token failed: {e}");
                }

                // Keep the existing secret: rotating it would be churn for nothing.
                new_config = Some(serde_json::json!({
                    "bot_token": bot_token, "bot_secret": old.bot_secret
                }));
                // Only arm a webhook for a channel that ends this request alive.
                // Editing a deleted channel's token without restoring it is allowed,
                // but registering its webhook would have Telegram deliver updates to
                // `/webhook/telegram/{id}`, where `find_live_channel_by_id` returns
                // None -> 404, so Telegram queues them and reports permanent delivery
                // errors against a channel the operator believes is deleted.
                if body.restore || existing.deleted_at.is_none() {
                    register = Some((bot_token, old.bot_secret));
                }
            }
        }
    } else if body.restore && existing.provider == "telegram" {
        // Restoring without a new spec still needs the webhook back: DELETE removed it.
        let cfg: TelegramConfig = serde_json::from_value(existing.config.clone())
            .map_err(|e| WebhookError::Internal(format!("bad telegram config: {e}")))?;
        register = Some((cfg.bot_token, cfg.bot_secret));
    }

    let updated = match db::update_channel(
        &state.db,
        channel_id,
        body.name.as_deref(),
        new_key.as_deref(),
        new_config.as_ref(),
        body.restore,
    )
    .await
    {
        Ok(Some(channel)) => channel,
        Ok(None) => return Err(WebhookError::NotFound("channel not found".into())),
        // Same rule as create: the unique index arbitrates, no pre-check SELECT.
        Err(sqlx::Error::Database(ref e)) if e.is_unique_violation() => {
            let provider = existing
                .provider
                .parse::<ProviderKind>()
                .map_err(WebhookError::Internal)?;
            let key = new_key.as_deref().unwrap_or(&existing.external_key);
            return Err(conflict(&state.db, provider, key).await);
        }
        Err(e) => return Err(e.into()),
    };

    if let Some((bot_token, bot_secret)) = register {
        let url = endpoint_for(
            &state.config.public_base_url,
            "telegram",
            updated.id,
            &updated.external_key,
        );
        if let Err(reason) = crate::provider::telegram::set_webhook(
            &state.config.telegram_api_base,
            &bot_token,
            &url,
            &bot_secret,
        )
        .await
        {
            // The row is already committed, so the cache has to be dropped even on
            // the error path. Otherwise this replica keeps verifying inbound updates
            // against the pre-PATCH config while the database holds the new one —
            // and no invalidation is published, so the other replicas never learn.
            // Unlike create, there is nothing to roll back here: the old webhook was
            // already deleted, so the honest end state is "new config, no webhook",
            // which the status check surfaces and "Re-register" fixes.
            invalidate_everywhere(&state, updated.id).await;
            return Err(WebhookError::BadGateway(format!(
                "setWebhook failed: {reason}"
            )));
        }
    }

    invalidate_everywhere(&state, updated.id).await;
    Ok(Json(ChannelView::new(
        &state.config.public_base_url,
        updated,
    )))
}

/// Soft delete. Idempotent, and deliberately `204` even for an unknown id:
/// DELETE states a desired end state, and "this channel is not active" already
/// holds. There is no hard delete in the API — `chats` and `messages` reference
/// `channels(id)`, and the history is worth keeping.
pub async fn delete(
    State(state): State<Arc<AppState>>,
    Path(channel_id): Path<Uuid>,
) -> Result<StatusCode, WebhookError> {
    let Some(channel) = db::soft_delete_channel(&state.db, channel_id).await? else {
        return Ok(StatusCode::NO_CONTENT);
    };

    if channel.provider == "telegram" {
        // Best effort. A channel whose token was revoked must stay deletable, so a
        // failure here is logged and the delete still succeeds.
        match serde_json::from_value::<TelegramConfig>(channel.config.clone()) {
            Ok(cfg) => {
                if let Err(e) = crate::provider::telegram::delete_webhook(
                    &state.config.telegram_api_base,
                    &cfg.bot_token,
                )
                .await
                {
                    tracing::warn!(%channel_id, "deleteWebhook failed during delete: {e}");
                }
            }
            Err(e) => {
                tracing::warn!(%channel_id, "bad telegram config, skipping deleteWebhook: {e}")
            }
        }
    }

    invalidate_everywhere(&state, channel_id).await;
    Ok(StatusCode::NO_CONTENT)
}

/// Telegram's view of a channel's webhook, plus the one fact an operator needs.
#[derive(Debug, Serialize)]
pub struct WebhookStatus {
    pub registered_url: String,
    pub expected_url: String,
    /// Computed here rather than in the browser: URL comparison should not be
    /// reimplemented in the frontend.
    pub matches: bool,
    pub pending_update_count: i64,
    pub last_error_date: Option<i64>,
    pub last_error_message: Option<String>,
}

impl WebhookStatus {
    fn matches_url(registered: &str, expected: &str) -> bool {
        !registered.is_empty() && registered == expected
    }
}

/// Load a live telegram channel and its config, or explain why it is neither.
async fn telegram_channel(
    state: &AppState,
    channel_id: Uuid,
) -> Result<(Channel, TelegramConfig), WebhookError> {
    let channel = db::find_live_channel_by_id(&state.db, channel_id)
        .await?
        .ok_or_else(|| WebhookError::NotFound("channel not found".into()))?;
    if channel.provider != "telegram" {
        return Err(WebhookError::BadRequest(format!(
            "channel provider is '{}'; only telegram channels have a webhook",
            channel.provider
        )));
    }
    let config = serde_json::from_value(channel.config.clone())
        .map_err(|e| WebhookError::Internal(format!("bad telegram config: {e}")))?;
    Ok((channel, config))
}

async fn status_of(
    state: &AppState,
    channel: &Channel,
    config: &TelegramConfig,
) -> Result<WebhookStatus, WebhookError> {
    let info = crate::provider::telegram::get_webhook_info(
        &state.config.telegram_api_base,
        &config.bot_token,
    )
    .await
    .map_err(|e| WebhookError::BadGateway(format!("getWebhookInfo failed: {e}")))?;

    let expected_url = endpoint_for(
        &state.config.public_base_url,
        "telegram",
        channel.id,
        &channel.external_key,
    );
    Ok(WebhookStatus {
        matches: WebhookStatus::matches_url(&info.url, &expected_url),
        registered_url: info.url,
        expected_url,
        pending_update_count: info.pending_update_count,
        last_error_date: info.last_error_date,
        last_error_message: info.last_error_message,
    })
}

/// Not fetched automatically by the panel: N telegram channels would mean N
/// Telegram round trips per page render, tying the settings page's load time to
/// Telegram's availability and rate limits. The panel calls this per channel,
/// on demand.
pub async fn webhook_status(
    State(state): State<Arc<AppState>>,
    Path(channel_id): Path<Uuid>,
) -> Result<Json<WebhookStatus>, WebhookError> {
    let (channel, config) = telegram_channel(&state, channel_id).await?;
    Ok(Json(status_of(&state, &channel, &config).await?))
}

/// Re-register and report the fresh status in the same response, so the panel
/// updates in one round trip. Idempotent, and available even when the status
/// already matches — it is what fixes a channel after a domain change.
pub async fn webhook_register(
    State(state): State<Arc<AppState>>,
    Path(channel_id): Path<Uuid>,
) -> Result<Json<WebhookStatus>, WebhookError> {
    let (channel, config) = telegram_channel(&state, channel_id).await?;
    let url = endpoint_for(
        &state.config.public_base_url,
        "telegram",
        channel.id,
        &channel.external_key,
    );
    crate::provider::telegram::set_webhook(
        &state.config.telegram_api_base,
        &config.bot_token,
        &url,
        &config.bot_secret,
    )
    .await
    .map_err(|e| WebhookError::BadGateway(format!("setWebhook failed: {e}")))?;

    Ok(Json(status_of(&state, &channel, &config).await?))
}

#[cfg(test)]
mod tests {
    use super::*;

    const ID: &str = "550e8400-e29b-41d4-a716-446655440000";

    #[test]
    fn endpoint_for_telegram_is_the_webhook_path() {
        let id: Uuid = ID.parse().unwrap();
        assert_eq!(
            endpoint_for("https://example.com", "telegram", id, "123456789"),
            format!("https://example.com/webhook/telegram/{ID}")
        );
    }

    #[test]
    fn endpoint_for_widget_upgrades_https_to_wss() {
        let id: Uuid = ID.parse().unwrap();
        assert_eq!(
            endpoint_for("https://example.com", "widget", id, "acme"),
            "wss://example.com/ws/acme"
        );
    }

    #[test]
    fn endpoint_for_widget_upgrades_http_to_ws() {
        let id: Uuid = ID.parse().unwrap();
        assert_eq!(
            endpoint_for("http://localhost:3800", "widget", id, "acme"),
            "ws://localhost:3800/ws/acme"
        );
    }

    #[test]
    fn endpoint_for_instagram_is_the_shared_webhook() {
        let id: Uuid = ID.parse().unwrap();
        assert_eq!(
            endpoint_for("https://example.com", "instagram", id, "17841400000000000"),
            "https://example.com/webhook/instagram"
        );
    }

    #[test]
    fn webhook_status_matches_only_on_an_exact_url() {
        let expected = "https://example.com/webhook/telegram/abc";
        assert!(WebhookStatus::matches_url(expected, expected));
        assert!(!WebhookStatus::matches_url("", expected));
        assert!(!WebhookStatus::matches_url(
            "https://old.example.com/webhook/telegram/abc",
            expected
        ));
        assert!(
            !WebhookStatus::matches_url("https://example.com/webhook/telegram/abc/", expected),
            "a trailing slash is a different URL to Telegram"
        );
    }

    #[test]
    fn widget_id_accepts_url_safe_values() {
        for good in [
            "acme",
            "acme-support",
            "acme_support_1",
            "A1",
            &"x".repeat(64),
        ] {
            assert!(
                validate_widget_id(good).is_ok(),
                "{good} should be accepted"
            );
        }
    }

    #[test]
    fn widget_id_rejects_values_that_break_the_ws_route() {
        // widget_id is substituted into the /ws/{widget_id} path, so anything that
        // is not a safe path segment must be refused at the boundary.
        for bad in [
            "",
            " ",
            "acme support",
            "acme/support",
            "\u{430}\u{43a}\u{43c}\u{435}",
            "a?b",
            "a#b",
            &"x".repeat(65),
        ] {
            assert!(
                validate_widget_id(bad).is_err(),
                "{bad:?} should be rejected"
            );
        }
    }

    #[test]
    fn bot_token_accepts_the_telegram_shape() {
        assert!(validate_bot_token("123456789:AAHqwertyuiop").is_ok());
    }

    #[test]
    fn bot_token_rejects_malformed_values() {
        for bad in [
            "",
            "nocolon",
            ":secret",
            "123456789:",
            "abc:secret",
            "12a34:secret",
        ] {
            assert!(
                validate_bot_token(bad).is_err(),
                "{bad:?} should be rejected"
            );
        }
    }
}
