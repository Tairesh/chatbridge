//! The OAuth login routes. Provider-agnostic: everything provider-specific is
//! behind `crate::oauth`.

use std::sync::Arc;

use axum::Json;
use axum::extract::{Path, Query, State};
use axum::response::Redirect;
use serde::Deserialize;
use uuid::Uuid;

use crate::config::AppState;
use crate::error::AppError;
use crate::model::ProviderKind;
use crate::oauth::{self, ProviderDescriptor};

/// Every provider the panel can offer a login for. The panel hardcodes nothing
/// about a provider, so this is where it learns that Instagram exists at all.
pub async fn providers(State(state): State<Arc<AppState>>) -> Json<Vec<ProviderDescriptor>> {
    Json(oauth::descriptors(&state.config))
}

/// Parse a path segment into a provider that actually has a login. Anything else is
/// a 404: an unknown provider and a provider without OAuth are the same fact from
/// the caller's side — there is no such login route.
fn oauth_provider(provider: &str) -> Result<ProviderKind, AppError> {
    let kind: ProviderKind = provider
        .parse()
        .map_err(|_| AppError::NotFound(format!("no OAuth login for '{provider}'")))?;
    if !oauth::supports(kind) {
        return Err(AppError::NotFound(format!(
            "no OAuth login for '{provider}'"
        )));
    }
    Ok(kind)
}

#[derive(Debug, Deserialize)]
pub struct StartQuery {
    /// Set when the popup was opened from a channel's "Reconnect" button. The
    /// callback then refuses to touch anything if a different account comes back.
    #[serde(default)]
    pub channel_id: Option<Uuid>,
}

/// Redirects rather than returning the URL as JSON: the popup opens this path
/// directly, so the frontend never assembles an authorize URL and there is one
/// round trip fewer to drift out of sync.
pub async fn start(
    State(state): State<Arc<AppState>>,
    Path(provider): Path<String>,
    Query(query): Query<StartQuery>,
) -> Result<Redirect, AppError> {
    let kind = oauth_provider(&provider)?;
    let token = oauth::sign_state(
        state.config.app_jwt_secret.as_bytes(),
        kind,
        query.channel_id,
    );
    let url = oauth::authorize_url(kind, &state.config, &token).map_err(AppError::Internal)?;
    Ok(Redirect::temporary(&url))
}

/// What the callback tells the panel. Serialized straight into the page.
#[derive(Debug, serde::Serialize)]
struct Outcome {
    /// Carried for the second provider's sake; today's panel ignores it.
    provider: String,
    ok: bool,
    channel_id: Option<Uuid>,
    name: Option<String>,
    created: bool,
    restored: bool,
    /// Connected, but something the operator should look at did not work.
    #[serde(skip_serializing_if = "Option::is_none")]
    warning: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

impl Outcome {
    fn failed(provider: &str, message: String) -> Self {
        Self {
            provider: provider.to_owned(),
            ok: false,
            channel_id: None,
            name: None,
            created: false,
            restored: false,
            warning: None,
            error: Some(message),
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct CallbackQuery {
    #[serde(default)]
    pub code: Option<String>,
    #[serde(default)]
    pub state: Option<String>,
    #[serde(default)]
    pub error: Option<String>,
    #[serde(default)]
    pub error_description: Option<String>,
}

/// Always answers `200`, even on failure. This is a page for a browser, not an API
/// response: the popup reads the outcome from the `postMessage` payload, and a 4xx
/// would only invite the browser to render its own error page instead.
pub async fn callback(
    State(state): State<Arc<AppState>>,
    Path(provider): Path<String>,
    Query(query): Query<CallbackQuery>,
) -> axum::response::Html<String> {
    let outcome = match connect(&state, &provider, query).await {
        Ok(outcome) => {
            tracing::info!(
                %provider,
                channel_id = ?outcome.channel_id,
                created = outcome.created,
                restored = outcome.restored,
                warning = ?outcome.warning,
                "oauth login connected an account"
            );
            outcome
        }
        Err(message) => {
            // A failed login renders into the popup and the popup closes — so without
            // this line the failure leaves no trace anywhere, and "the login worked
            // but nothing changed" becomes unanswerable. That cost hours once.
            tracing::warn!(%provider, "oauth login failed: {message}");
            Outcome::failed(&provider, message)
        }
    };
    page(&outcome)
}

async fn connect(
    state: &AppState,
    provider: &str,
    query: CallbackQuery,
) -> Result<Outcome, String> {
    // 1. The user cancelled, or the provider refused before we were involved.
    if let Some(error) = query.error {
        let detail = query.error_description.unwrap_or_else(|| error.clone());
        return Err(format!("login was not completed: {detail}"));
    }

    // 2. The state proves this callback follows a `start` this deployment issued.
    let kind = provider
        .parse::<ProviderKind>()
        .map_err(|_| format!("no OAuth login for '{provider}'"))?;
    if !oauth::supports(kind) {
        return Err(format!("no OAuth login for '{provider}'"));
    }
    let token = query.state.ok_or("callback carried no state")?;
    let claims = oauth::verify_state(state.config.app_jwt_secret.as_bytes(), &token, kind)?;
    let code = query.code.ok_or("callback carried no code")?;

    // 3. Exchange. Nothing is written before this succeeds.
    let connected = oauth::exchange(kind, &state.config, &code).await?;

    // 4. A popup opened from a channel's row may only touch that channel. The
    //    identity of a channel cannot be swapped: it owns its chats and messages.
    if let Some(pinned) = claims.ch {
        let existing = crate::db::find_channel_by_id(&state.db, pinned)
            .await
            .map_err(|e| format!("database error: {e}"))?
            .ok_or("the channel this login was started from no longer exists")?;
        if existing.provider != kind.to_string() {
            return Err(format!(
                "that channel is a '{}' channel, not '{kind}'",
                existing.provider
            ));
        }
        if existing.external_key != connected.external_key {
            // Both keys in the message: an app-scoped `user_id` changes when the Meta
            // app changes, so "a different account" and "the same account under a new
            // app" look identical here. Naming both is what tells them apart.
            return Err(format!(
                "the login returned account {} with id {}, but this channel is \
                 connected to id {}. If you changed the Meta app, that is the same \
                 Instagram account under a new app-scoped id — connect it as a new \
                 channel instead of reconnecting this one.",
                connected.display_name, connected.external_key, existing.external_key
            ));
        }
    }

    // 5. Upsert. `restored` is for the wording of the message only, so this read does
    //    not need to share the upsert's atomicity — but `inserted` does, and it comes
    //    back from the statement itself.
    let before = crate::db::find_channel_by_external_key(&state.db, kind, &connected.external_key)
        .await
        .map_err(|e| format!("database error: {e}"))?;
    let restored = before.as_ref().is_some_and(|c| c.deleted_at.is_some());

    let (channel, inserted) = crate::db::upsert_channel_by_external_key(
        &state.db,
        before.as_ref().map_or_else(Uuid::new_v4, |c| c.id),
        kind,
        &connected.display_name,
        &connected.external_key,
        &connected.config,
    )
    .await
    .map_err(|e| format!("database error: {e}"))?;

    // 6. Subscribe, which is what actually makes events flow.
    let mut warning = None;
    if let Err(reason) = oauth::subscribe(kind, &state.config, &channel.config).await {
        // `inserted`, never the pre-read: a concurrent login inserting the row between
        // the SELECT and the upsert would otherwise have us hard-delete a live channel
        // and its whole history.
        if inserted {
            // Roll back PHYSICALLY. The row is milliseconds old so nothing can
            // reference it, and the unique index has no `deleted_at` filter — a soft
            // delete would occupy this account's identity forever.
            if let Err(e) = crate::db::hard_delete_channel(&state.db, channel.id).await {
                tracing::error!(
                    channel_id = %channel.id,
                    "subscribe failed AND the rollback failed: {e}. The channel is live \
                     with no subscription — fix it with Check connection in the panel."
                );
            }
            return Err(format!("could not subscribe to events: {reason}"));
        }
        // An existing channel keeps its row: the new token is strictly better than the
        // one it replaced, and the honest end state is "connected, not subscribed".
        warning = Some(format!(
            "connected, but subscribing to events failed: {reason}. Use \"Check connection\"."
        ));
    }

    crate::handler::channels::invalidate_everywhere(state, channel.id).await;

    Ok(Outcome {
        provider: kind.to_string(),
        ok: true,
        channel_id: Some(channel.id),
        name: Some(channel.name),
        created: inserted,
        restored,
        warning,
        error: None,
    })
}

/// The popup's last act: hand the outcome to the panel and close.
///
/// `targetOrigin` is `window.location.origin`, read in the browser rather than
/// rendered from config — the page is served by this deployment, so its own origin
/// is by definition the right target, and there is no configuration value left to
/// get wrong. Never `'*'`.
///
/// The payload is JSON with every `<` rewritten to its `\u003c` escape. That escape
/// is the only thing between an attacker-chosen Instagram username (or a Meta error
/// message) and script injection into the panel's own origin.
///
/// The link is `/frontend/settings.html`: nginx serves the frontend under
/// `/frontend/`, and a bare `/settings.html` is proxied to the application, which
/// has no such route.
fn page(outcome: &Outcome) -> axum::response::Html<String> {
    let payload = serde_json::to_string(outcome)
        .unwrap_or_else(|_| r#"{"ok":false,"error":"could not render the outcome"}"#.into())
        .replace('<', r"\u003c");

    axum::response::Html(format!(
        r#"<!DOCTYPE html>
<html lang="en">
<head><meta charset="utf-8"><title>Login</title></head>
<body style="font-family: system-ui, sans-serif; padding: 24px">
<p id="msg">Finishing up…</p>
<p><a href="/frontend/settings.html">Back to the settings panel</a></p>
<script>
const payload = {payload};
document.getElementById('msg').textContent =
  payload.ok ? (payload.warning || 'Connected. You can close this window.')
             : (payload.error || 'The login did not complete.');
if (window.opener) {{
  window.opener.postMessage({{source: 'chatbridge-oauth', ...payload}}, window.location.origin);
  window.close();
}}
</script>
</body></html>"#
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn outcome_named(name: &str) -> Outcome {
        Outcome {
            provider: "instagram".into(),
            ok: true,
            channel_id: Some(Uuid::nil()),
            name: Some(name.to_owned()),
            created: true,
            restored: false,
            warning: None,
            error: None,
        }
    }

    #[test]
    fn a_hostile_username_cannot_close_the_script_element() {
        // An Instagram handle is attacker-chosen and lands in the panel's own origin.
        //
        // Counting the closing tags is what makes this test able to fail: an earlier
        // version walked from `<script>` to the *first* `</script>` and asserted no
        // `<` inside, which is trivially true when the injected tag IS that first
        // match. The page must contain exactly the one closing tag it renders itself.
        let hostile = "</script><img src=x onerror=alert(1)>";
        let body = page(&outcome_named(hostile)).0;
        assert_eq!(
            body.matches("</script>").count(),
            1,
            "the payload closed the script element: {body}"
        );
        assert!(
            !body.contains(hostile),
            "the hostile handle survived unescaped: {body}"
        );
        assert!(
            body.contains(r"\u003c/script>"),
            "it should be present, escaped: {body}"
        );
    }

    #[test]
    fn the_target_origin_is_the_pages_own_and_never_a_wildcard() {
        let body = page(&outcome_named("biz")).0;
        assert!(body.contains("window.location.origin"), "{body}");
        assert!(!body.contains("'*'"), "{body}");
    }

    #[test]
    fn the_page_links_to_where_nginx_actually_serves_the_panel() {
        assert!(
            page(&outcome_named("biz"))
                .0
                .contains("/frontend/settings.html")
        );
    }
}
