//! "Is this channel actually wired up at its provider, and can I fix it?"
//!
//! Provider-generic on purpose: a telegram channel has a webhook, an instagram
//! channel has a field subscription and a token with a lifetime, and a widget
//! channel has nothing. One endpoint, one response shape, one button in the panel.

use std::sync::Arc;

use axum::Json;
use axum::extract::{Path, State};
use serde::Serialize;
use uuid::Uuid;

use crate::config::AppState;
use crate::db::{self, Channel};
use crate::error::AppError;
use crate::model::{InstagramConfig, ProviderKind, TelegramConfig};

/// `details` is untyped so a provider can add a field without a frontend change;
/// the panel renders it as a key/value list. `summary` is composed here rather than
/// in the browser — URL comparison and day arithmetic should not be reimplemented
/// in JavaScript.
#[derive(Debug, Serialize)]
pub struct ConnectionStatus {
    pub provider: String,
    pub ok: bool,
    pub summary: String,
    pub details: serde_json::Value,
}

fn matches_url(registered: &str, expected: &str) -> bool {
    !registered.is_empty() && registered == expected
}

fn telegram_summary(
    registered: &str,
    expected: &str,
    pending: i64,
    last_error: Option<&str>,
) -> String {
    let mut summary = if matches_url(registered, expected) {
        format!("Webhook registered at {expected}.")
    } else if registered.is_empty() {
        "No webhook is registered for this bot.".to_owned()
    } else {
        format!("Webhook points at {registered} instead of {expected}.")
    };
    if pending > 0 {
        summary.push_str(&format!(" {pending} update(s) pending."));
    }
    if let Some(error) = last_error {
        summary.push_str(&format!(" Last delivery error: {error}"));
    }
    summary
}

fn expiry_phrase(expires_in_days: Option<i64>) -> String {
    match expires_in_days {
        Some(days) if days > 0 => format!(" Token expires in {days} day(s)."),
        Some(0) => " Token expires within a day.".to_owned(),
        Some(_) => " Token has expired — reconnect the account.".to_owned(),
        None => " Token expiry unknown; the refresher fills it in on its next pass.".to_owned(),
    }
}

fn instagram_summary(fields: &[String], expires_in_days: Option<i64>) -> String {
    let mut summary = if fields.is_empty() {
        "Not subscribed to any message events.".to_owned()
    } else {
        format!("Subscribed to {}.", fields.join(", "))
    };
    summary.push_str(&expiry_phrase(expires_in_days));
    summary
}

/// A deleted channel has nothing to manage: restore it first.
async fn live_channel(state: &AppState, channel_id: Uuid) -> Result<Channel, AppError> {
    db::find_live_channel_by_id(&state.db, channel_id)
        .await?
        .ok_or_else(|| AppError::NotFound("channel not found".into()))
}

async fn telegram_status(
    state: &AppState,
    channel: &Channel,
) -> Result<ConnectionStatus, AppError> {
    let config: TelegramConfig = serde_json::from_value(channel.config.clone())
        .map_err(|e| AppError::Internal(format!("bad telegram config: {e}")))?;
    let info = crate::provider::telegram::get_webhook_info(
        &state.config.telegram_api_base,
        &config.bot_token,
    )
    .await
    .map_err(|e| AppError::BadGateway(format!("getWebhookInfo failed: {e}")))?;

    let expected_url = crate::handler::channels::endpoint_for(
        &state.config.public_base_url,
        "telegram",
        channel.id,
        &channel.external_key,
    );
    Ok(ConnectionStatus {
        provider: channel.provider.clone(),
        ok: matches_url(&info.url, &expected_url),
        summary: telegram_summary(
            &info.url,
            &expected_url,
            info.pending_update_count,
            info.last_error_message.as_deref(),
        ),
        details: serde_json::json!({
            "registered_url": info.url,
            "expected_url": expected_url,
            "pending_update_count": info.pending_update_count,
            "last_error_date": info.last_error_date,
            "last_error_message": info.last_error_message,
        }),
    })
}

async fn instagram_status(
    state: &AppState,
    channel: &Channel,
) -> Result<ConnectionStatus, AppError> {
    let config: InstagramConfig = serde_json::from_value(channel.config.clone())
        .map_err(|e| AppError::Internal(format!("bad instagram config: {e}")))?;

    // `GET /me/subscribed_apps` is documented for Facebook Pages but not for Instagram
    // Business Login, and the sibling PHP integration never reads the subscription
    // back. If Meta refuses the read, say the subscription is unknown — a 502 here
    // would make the button useless for every Instagram channel on a deployment where
    // the GET is simply unsupported.
    let fields = match crate::oauth::instagram::subscribed_fields(
        &state.config.instagram,
        &config.access_token,
    )
    .await
    {
        Ok(fields) => Some(fields),
        Err(e) => {
            tracing::warn!(channel_id = %channel.id, "could not read the subscription: {e}");
            None
        }
    };

    let now = chrono::Utc::now();
    let expires_in_days = config.token_expires_at.map(|at| (at - now).num_days());
    // Compared against the timestamp, NOT against `expires_in_days`: `num_days`
    // truncates, so eighteen hours left yields 0, and calling that "expired —
    // reconnect the account" would be a lie told at exactly the moment the operator
    // acts on it. The day count is for the wording only.
    let token_alive = config.token_expires_at.is_none_or(|at| at > now);

    let (subscribed, summary) = match fields {
        // `messages` is the one field without which the channel is dead; the others
        // only enrich what arrives.
        Some(ref fields) => (
            fields.iter().any(|f| f == "messages"),
            instagram_summary(fields, expires_in_days),
        ),
        None => (
            false,
            format!(
                "Could not read the subscription from Instagram.{}",
                expiry_phrase(expires_in_days)
            ),
        ),
    };

    Ok(ConnectionStatus {
        provider: channel.provider.clone(),
        ok: subscribed && token_alive,
        summary,
        details: serde_json::json!({
            "subscribed_fields": fields,
            "token_expires_at": config.token_expires_at,
            "expires_in_days": expires_in_days,
        }),
    })
}

async fn status_of(state: &AppState, channel: &Channel) -> Result<ConnectionStatus, AppError> {
    match channel.provider.as_str() {
        "telegram" => telegram_status(state, channel).await,
        "instagram" => instagram_status(state, channel).await,
        // A widget channel is fully described by its `external_key`; there is nothing
        // registered anywhere. Reporting that honestly beats a 400.
        other => Ok(ConnectionStatus {
            provider: other.to_owned(),
            ok: true,
            summary: "Widget channels register nothing with a provider.".to_owned(),
            details: serde_json::json!({}),
        }),
    }
}

/// Not fetched automatically by the panel: N channels would mean N provider round
/// trips per page render, tying the settings page's load time to Meta's and
/// Telegram's availability and rate limits. The panel calls this per channel, on
/// demand.
pub async fn status(
    State(state): State<Arc<AppState>>,
    Path(channel_id): Path<Uuid>,
) -> Result<Json<ConnectionStatus>, AppError> {
    let channel = live_channel(&state, channel_id).await?;
    Ok(Json(status_of(&state, &channel).await?))
}

/// Arm the channel and report the fresh status in the same response, so the panel
/// updates in one round trip. Idempotent, and available even when the status already
/// looks fine — it is what fixes a channel after a domain change.
pub async fn register(
    State(state): State<Arc<AppState>>,
    Path(channel_id): Path<Uuid>,
) -> Result<Json<ConnectionStatus>, AppError> {
    let channel = live_channel(&state, channel_id).await?;
    match channel.provider.as_str() {
        "telegram" => {
            let config: TelegramConfig = serde_json::from_value(channel.config.clone())
                .map_err(|e| AppError::Internal(format!("bad telegram config: {e}")))?;
            let url = crate::handler::channels::endpoint_for(
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
            .map_err(|e| AppError::BadGateway(format!("setWebhook failed: {e}")))?;
        }
        "instagram" => {
            crate::oauth::subscribe(ProviderKind::Instagram, &state.config, &channel.config)
                .await
                .map_err(|e| AppError::BadGateway(format!("could not subscribe to events: {e}")))?;
        }
        // Widget: nothing to arm, and the panel does not show the button.
        _ => {}
    }
    Ok(Json(status_of(&state, &channel).await?))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_webhook_matches_only_on_an_exact_url() {
        let expected = "https://example.com/webhook/telegram/abc";
        assert!(matches_url(expected, expected));
        assert!(!matches_url("", expected));
        assert!(!matches_url(
            "https://old.example.com/webhook/telegram/abc",
            expected
        ));
        assert!(
            !matches_url("https://example.com/webhook/telegram/abc/", expected),
            "a trailing slash is a different URL to Telegram"
        );
    }

    #[test]
    fn a_telegram_summary_distinguishes_absent_from_hijacked() {
        let expected = "https://example.com/webhook/telegram/abc";
        assert!(telegram_summary("", expected, 0, None).contains("No webhook"));
        let hijacked = telegram_summary("https://evil.example.com/x", expected, 0, None);
        assert!(hijacked.contains("instead of"), "{hijacked}");
        assert!(telegram_summary(expected, expected, 0, None).contains("registered at"));
    }

    #[test]
    fn a_telegram_summary_reports_pending_updates_and_delivery_errors() {
        let expected = "https://example.com/webhook/telegram/abc";
        let s = telegram_summary(expected, expected, 12, Some("wrong response: 404"));
        assert!(s.contains("12 update(s) pending"), "{s}");
        assert!(s.contains("404"), "{s}");
    }

    #[test]
    fn an_instagram_summary_names_the_fields_and_the_days_left() {
        let s = instagram_summary(&["messages".into(), "message_edit".into()], Some(58));
        assert!(s.contains("messages, message_edit"), "{s}");
        assert!(s.contains("58 day"), "{s}");
    }

    #[test]
    fn an_instagram_summary_calls_out_an_expired_token() {
        let s = instagram_summary(&["messages".into()], Some(-1));
        assert!(s.contains("expired"), "{s}");
    }

    #[test]
    fn a_token_with_hours_left_is_not_called_expired() {
        // num_days truncates: eighteen hours left is 0 days, and 0 must not read as
        // "expired". The wording comes from the day count, the verdict does not.
        let s = instagram_summary(&["messages".into()], Some(0));
        assert!(!s.contains("expired"), "{s}");
    }

    #[test]
    fn an_instagram_summary_calls_out_a_missing_subscription() {
        let s = instagram_summary(&[], None);
        assert!(s.contains("Not subscribed"), "{s}");
        assert!(s.contains("expiry"), "{s}");
    }
}
