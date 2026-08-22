//! Keeps Instagram's long-lived tokens alive.
//!
//! A long-lived token lasts about 60 days. Once it has expired it cannot be
//! refreshed at all — the account has to be connected again — so the refresh has to
//! be proactive rather than reactive.

use std::sync::Arc;

use crate::config::AppState;
use crate::model::{InstagramConfig, ProviderKind};

/// Refresh this many days before expiry — the same threshold the sibling PHP
/// integration runs in production.
pub const REFRESH_THRESHOLD_DAYS: i64 = 3;

/// Hourly, also matching production. With a three-day threshold that is about
/// seventy attempts before a token dies instead of three, and it is what lets a
/// token that was rejected for being under 24 hours old succeed later the same day.
const REFRESH_INTERVAL: std::time::Duration = std::time::Duration::from_secs(60 * 60);

pub fn is_due(config: &InstagramConfig, now: chrono::DateTime<chrono::Utc>) -> bool {
    match config.token_expires_at {
        // Absent means "unknown", which is what a hand-pasted token looks like.
        // Treating it as due is how it acquires a real expiry.
        None => true,
        Some(at) => at <= now + chrono::TimeDelta::days(REFRESH_THRESHOLD_DAYS),
    }
}

/// Which channels this pass should touch, and their parsed configs.
///
/// Filtered in Rust rather than with a JSONB cast in SQL: one malformed blob would
/// abort the whole pass, and the row count here is small. A blob that will not parse
/// is dropped with an error rather than counted as due — there is nothing to refresh.
pub fn due_channels(
    channels: Vec<crate::db::Channel>,
    now: chrono::DateTime<chrono::Utc>,
) -> Vec<(crate::db::Channel, InstagramConfig)> {
    channels
        .into_iter()
        .filter_map(|channel| {
            match serde_json::from_value::<InstagramConfig>(channel.config.clone()) {
                Ok(config) if is_due(&config, now) => Some((channel, config)),
                Ok(_) => None,
                Err(e) => {
                    tracing::error!(channel_id = %channel.id, "bad instagram config: {e}");
                    None
                }
            }
        })
        .collect()
}

/// Refresh one channel's token and store it.
///
/// Separate from the pass so a test can drive it against a channel it owns:
/// `refresh_due_tokens` touches every live Instagram channel in the database, and
/// the integration tests share one.
pub async fn refresh_channel(
    state: &AppState,
    channel: &crate::db::Channel,
    config: &InstagramConfig,
) -> Result<(), String> {
    let fresh =
        crate::oauth::instagram::refresh_token(&state.config.instagram, &config.access_token)
            .await?;

    let updated = InstagramConfig {
        access_token: fresh.access_token,
        token_expires_at: Some(chrono::Utc::now() + chrono::TimeDelta::seconds(fresh.expires_in)),
        username: config.username.clone(),
    };
    let blob = serde_json::to_value(&updated).map_err(|e| format!("serialize failed: {e}"))?;

    crate::db::update_channel(&state.db, channel.id, None, None, Some(&blob), false)
        .await
        .map_err(|e| format!("could not store the refreshed token: {e}"))?;
    crate::handler::channels::invalidate_everywhere(state, channel.id).await;
    Ok(())
}

/// One pass. Returns `(refreshed, failed)`.
pub async fn refresh_due_tokens(state: &AppState) -> (usize, usize) {
    let channels =
        match crate::db::list_live_channels_by_provider(&state.db, ProviderKind::Instagram).await {
            Ok(channels) => channels,
            Err(e) => {
                tracing::error!("token refresh could not list channels: {e}");
                return (0, 0);
            }
        };

    let (mut refreshed, mut failed) = (0, 0);
    for (channel, config) in due_channels(channels, chrono::Utc::now()) {
        match refresh_channel(state, &channel, &config).await {
            Ok(()) => refreshed += 1,
            Err(e) => {
                failed += 1;
                // Not retried inside the pass: an expired token cannot be refreshed
                // at any number of attempts. The operator sees it as "token has
                // expired" in the connection check and fixes it with Reconnect, which
                // is why there is no `needs_reconnect` column.
                //
                // A channel with no known expiry is a different case: Meta refuses to
                // refresh a token less than 24 hours old, so a hand-pasted token is
                // *expected* to be turned away on the first pass and to succeed within
                // the day. That is a warning, not an error.
                if config.token_expires_at.is_none() {
                    tracing::warn!(
                        channel_id = %channel.id,
                        "refresh of a token with unknown age was refused (Meta requires 24h): {e}"
                    );
                } else {
                    tracing::error!(channel_id = %channel.id, "instagram token refresh failed: {e}");
                }
            }
        }
    }

    (refreshed, failed)
}

/// Spawned from `main` only — never from `build_state`, or the integration tests
/// would call the real Graph API.
pub fn spawn_token_refresher(state: Arc<AppState>) {
    tokio::spawn(async move {
        loop {
            // Immediately on startup, then hourly. The first pass is what fills in the
            // expiry of any token that was pasted in by hand.
            let (refreshed, failed) = refresh_due_tokens(&state).await;
            if refreshed > 0 || failed > 0 {
                tracing::info!(refreshed, failed, "instagram token refresh pass complete");
            }
            tokio::select! {
                _ = tokio::time::sleep(REFRESH_INTERVAL) => {}
                _ = state.shutdown.cancelled() => {
                    tracing::info!("token refresher shutting down");
                    return;
                }
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(expires_at: Option<chrono::DateTime<chrono::Utc>>) -> InstagramConfig {
        InstagramConfig {
            access_token: "tok".into(),
            token_expires_at: expires_at,
            username: None,
        }
    }

    #[test]
    fn a_token_without_a_known_expiry_is_due() {
        // This is how a hand-pasted token gets its expiry filled in.
        assert!(is_due(&config(None), chrono::Utc::now()));
    }

    #[test]
    fn a_token_inside_the_threshold_is_due() {
        let now = chrono::Utc::now();
        assert!(is_due(&config(Some(now + chrono::TimeDelta::days(2))), now));
    }

    #[test]
    fn a_healthy_token_is_left_alone() {
        let now = chrono::Utc::now();
        assert!(!is_due(
            &config(Some(now + chrono::TimeDelta::days(30))),
            now
        ));
    }

    #[test]
    fn an_already_expired_token_is_still_attempted() {
        // The attempt fails and says so, which is what surfaces "reconnect" in the
        // panel. Skipping it would leave the channel silently broken instead.
        let now = chrono::Utc::now();
        assert!(is_due(&config(Some(now - chrono::TimeDelta::days(1))), now));
    }

    #[test]
    fn the_threshold_matches_the_production_integration() {
        // Same three days the sibling PHP integration uses, at the same hourly cadence.
        assert_eq!(REFRESH_THRESHOLD_DAYS, 3);
    }

    #[test]
    fn due_channels_skips_the_healthy_and_the_malformed() {
        // The filter is the part of the pass a test can own; the loop around it is
        // three lines. Without this, deleting the `is_due` check would go unnoticed.
        fn channel(config: serde_json::Value) -> crate::db::Channel {
            crate::db::Channel {
                id: uuid::Uuid::new_v4(),
                provider: "instagram".into(),
                name: "n".into(),
                external_key: uuid::Uuid::new_v4().to_string(),
                config,
                deleted_at: None,
                created_at: chrono::Utc::now(),
            }
        }
        let now = chrono::Utc::now();
        let healthy = channel(serde_json::json!({
            "access_token": "t",
            "token_expires_at": (now + chrono::TimeDelta::days(30)).to_rfc3339()
        }));
        let expiring = channel(serde_json::json!({
            "access_token": "t",
            "token_expires_at": (now + chrono::TimeDelta::days(2)).to_rfc3339()
        }));
        let unknown = channel(serde_json::json!({"access_token": "t"}));
        let malformed = channel(serde_json::json!({"nonsense": true}));

        let expiring_id = expiring.id;
        let unknown_id = unknown.id;
        let due = due_channels(vec![healthy, expiring, unknown, malformed], now);
        let ids: Vec<_> = due.iter().map(|(c, _)| c.id).collect();
        assert_eq!(ids, vec![expiring_id, unknown_id]);
    }
}
