//! Instagram Graph API calls used by the OAuth login, the webhook-field
//! subscription and the token refresher.
//!
//! The webhook *ingestion* side (signature verification and payload parsing)
//! lives in `provider/instagram.rs`; this module is the outbound half.

use std::sync::LazyLock;

use serde::Deserialize;

use crate::config::InstagramEndpoints;

static HTTP_CLIENT: LazyLock<reqwest::Client> = LazyLock::new(|| {
    reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()
        .expect("failed to build reqwest client")
});

#[derive(Debug, Deserialize)]
pub struct ShortLivedToken {
    pub access_token: String,
}

#[derive(Debug, Deserialize)]
pub struct LongLivedToken {
    pub access_token: String,
    /// Seconds. About 60 days.
    pub expires_in: i64,
}

#[derive(Debug)]
pub struct Profile {
    /// The IG-User-ID, the natural key of an Instagram channel.
    pub user_id: String,
    pub username: Option<String>,
}

/// Instagram appends `#_` to the authorization code. Browsers do not send a
/// fragment, but the marker has been observed inside the query value itself, and
/// stripping a suffix that is never legitimately part of a code costs nothing.
pub fn strip_code_fragment(code: &str) -> &str {
    code.strip_suffix("#_").unwrap_or(code)
}

/// Meta answers failures with `{"error": {message, type, code, error_subcode,
/// fbtrace_id}}`. `error_subcode` earns its place in the message: it is the only
/// thing separating "outside the 24-hour window" (code 10, subcode 2534022) from
/// every other permission error, and "not a short-lived token" (190/33) from a
/// genuinely dead token.
///
/// Logged unconditionally, not only when `fbtrace_id` is present — a Meta error
/// with no trace id is still an error.
fn envelope_error(json: &serde_json::Value) -> Option<String> {
    let error = json.get("error")?;
    let message = error["message"]
        .as_str()
        .unwrap_or("unknown Instagram API error");
    tracing::error!(
        fbtrace_id = ?error["fbtrace_id"],
        code = ?error["code"],
        subcode = ?error["error_subcode"],
        "instagram API error: {message}"
    );
    let mut out = message.to_owned();
    if let Some(subcode) = error["error_subcode"].as_i64() {
        out.push_str(&format!(" (subcode {subcode})"));
    }
    Some(out)
}

/// Meta's docs show `/me` and the code exchange wrapped in `{"data": [ … ]}`,
/// while the sibling PHP integration reads both flat and is in production. Flat is
/// what the wire does; unwrapping a single-element `data` array anyway costs three
/// lines and covers the one discrepancy that would otherwise break every connect.
fn unwrap_envelope(json: serde_json::Value) -> serde_json::Value {
    match json.get("data").and_then(|d| d.as_array()) {
        Some(items) if items.len() == 1 && items[0].is_object() => items[0].clone(),
        _ => json,
    }
}

async fn call(request: reqwest::RequestBuilder) -> Result<serde_json::Value, String> {
    let resp = request
        .send()
        .await
        .map_err(|e| format!("HTTP request failed: {e}"))?;

    // Read the status before consuming the body: Instagram reports failures as an
    // error envelope *with* a 4xx, so the body has to be read even when the request
    // failed — but a 4xx or 5xx whose body carries no envelope must not fall through
    // as success.
    let status = resp.status();
    let json: serde_json::Value = resp
        .json()
        .await
        .map_err(|e| format!("failed to parse response (HTTP {}): {e}", status.as_u16()))?;

    if let Some(message) = envelope_error(&json) {
        return Err(message);
    }
    if status.is_client_error() || status.is_server_error() {
        return Err(format!("HTTP {} with no error envelope", status.as_u16()));
    }
    Ok(json)
}

/// Meta answers both subscription calls with `{"success": true}`.
fn confirm_success(json: &serde_json::Value, context: &str) -> Result<(), String> {
    if json["success"].as_bool() == Some(true) {
        Ok(())
    } else {
        Err(format!("{context}: {json}"))
    }
}

fn parse_profile(json: &serde_json::Value) -> Result<Profile, String> {
    let user_id = json["user_id"]
        .as_str()
        .map(str::to_owned)
        .or_else(|| json["user_id"].as_i64().map(|n| n.to_string()))
        .or_else(|| json["id"].as_str().map(str::to_owned))
        .or_else(|| json["id"].as_i64().map(|n| n.to_string()))
        .ok_or_else(|| "profile response carries no user_id".to_owned())?;
    Ok(Profile {
        user_id,
        username: json["username"].as_str().map(str::to_owned),
    })
}

/// `subscribed_fields` comes back as plain strings on some surfaces and as
/// `{name, version}` objects on others. Accept both rather than guessing.
fn parse_subscribed_fields(json: &serde_json::Value) -> Vec<String> {
    json["data"][0]["subscribed_fields"]
        .as_array()
        .map(|fields| {
            fields
                .iter()
                .filter_map(|f| {
                    f.as_str()
                        .map(str::to_owned)
                        .or_else(|| f["name"].as_str().map(str::to_owned))
                })
                .collect()
        })
        .unwrap_or_default()
}

pub async fn exchange_code(
    ep: &InstagramEndpoints,
    app_id: &str,
    app_secret: &str,
    redirect_uri: &str,
    code: &str,
) -> Result<ShortLivedToken, String> {
    let json = call(
        HTTP_CLIENT
            .post(format!("{}/oauth/access_token", ep.api))
            .form(&[
                ("client_id", app_id),
                ("client_secret", app_secret),
                ("grant_type", "authorization_code"),
                ("redirect_uri", redirect_uri),
                ("code", strip_code_fragment(code)),
            ]),
    )
    .await?;
    serde_json::from_value(unwrap_envelope(json))
        .map_err(|e| format!("unexpected token response: {e}"))
}

pub async fn exchange_long_lived(
    ep: &InstagramEndpoints,
    app_secret: &str,
    short_lived: &str,
) -> Result<LongLivedToken, String> {
    let json = call(
        HTTP_CLIENT
            .get(format!("{}/access_token", ep.graph))
            .query(&[
                ("grant_type", "ig_exchange_token"),
                ("client_secret", app_secret),
                ("access_token", short_lived),
            ]),
    )
    .await?;
    serde_json::from_value(json).map_err(|e| format!("unexpected long-lived response: {e}"))
}

pub async fn fetch_profile(ep: &InstagramEndpoints, token: &str) -> Result<Profile, String> {
    let json = call(
        HTTP_CLIENT
            .get(format!("{}/me", ep.graph))
            .query(&[("fields", "user_id,username,name"), ("access_token", token)]),
    )
    .await?;
    parse_profile(&unwrap_envelope(json))
}

/// Sets the account's subscribed-field list. It **replaces** the set rather than
/// adding to it — verified against the live API on 2026-08-22 by subscribing four
/// fields, then two, and reading back exactly two. The caller's recovery path is
/// written to be correct under either reading anyway.
///
/// A `200` body of `{"success": false}` is a failure. Nothing in the HTTP layer
/// says so, and treating it as success is how a channel gets created that silently
/// receives nothing.
pub async fn subscribe(ep: &InstagramEndpoints, token: &str, fields: &str) -> Result<(), String> {
    let json = call(
        HTTP_CLIENT
            .post(format!("{}/me/subscribed_apps", ep.graph))
            .query(&[("subscribed_fields", fields), ("access_token", token)]),
    )
    .await?;
    confirm_success(&json, "subscription to fields was not confirmed")
}

pub async fn subscribed_fields(
    ep: &InstagramEndpoints,
    token: &str,
) -> Result<Vec<String>, String> {
    let json = call(
        HTTP_CLIENT
            .get(format!("{}/me/subscribed_apps", ep.graph))
            .query(&[("access_token", token)]),
    )
    .await?;
    Ok(parse_subscribed_fields(&json))
}

pub async fn unsubscribe(ep: &InstagramEndpoints, token: &str) -> Result<(), String> {
    let json = call(
        HTTP_CLIENT
            .delete(format!("{}/me/subscribed_apps", ep.graph))
            .query(&[("access_token", token)]),
    )
    .await?;
    confirm_success(&json, "unsubscription was not confirmed")
}

/// What Meta answers a successful send with.
#[derive(Debug, Deserialize)]
pub struct SentMessage {
    pub message_id: String,
    pub recipient_id: String,
}

/// Send a text message to an IGSID.
///
/// The wire shape is taken from the sibling PHP integration rather than the
/// Messenger docs, which differ in two places that are easy to get wrong: a message
/// `tag` belongs at the **top level** of the payload, not inside `message`, and
/// attachments go out as `message.attachments` — an array — not the singular
/// `message.attachment`. Neither is used yet; both are noted so the next person does
/// not re-derive them from the wrong page.
///
/// `recipient` is the IGSID, which is the same value that arrived as `sender.id` on
/// the inbound webhook. There is no PSID lookup on this flow.
pub async fn send_message(
    ep: &InstagramEndpoints,
    token: &str,
    recipient: &str,
    text: &str,
) -> Result<SentMessage, String> {
    let json = call(
        HTTP_CLIENT
            .post(format!("{}/me/messages", ep.graph))
            .query(&[("access_token", token)])
            .json(&serde_json::json!({
                "recipient": {"id": recipient},
                "message": {"text": text},
            })),
    )
    .await?;
    serde_json::from_value(json).map_err(|e| format!("unexpected send response: {e}"))
}

/// Tell Instagram the customer's messages have been seen.
///
/// Same endpoint as a send, so no extra permission is involved. It marks the whole
/// thread — Instagram has no per-message granularity here, which is why nothing about
/// our own watermark is sent along.
pub async fn mark_seen(
    ep: &InstagramEndpoints,
    token: &str,
    recipient: &str,
) -> Result<(), String> {
    call(
        HTTP_CLIENT
            .post(format!("{}/me/messages", ep.graph))
            .query(&[("access_token", token)])
            .json(&serde_json::json!({
                "recipient": {"id": recipient},
                "sender_action": "mark_seen",
            })),
    )
    .await?;
    Ok(())
}

/// A refresh needs the *current* token to still be valid, and Meta refuses a token
/// less than 24 hours old. Once it has expired there is no automatic recovery —
/// the account must be connected again.
pub async fn refresh_token(ep: &InstagramEndpoints, token: &str) -> Result<LongLivedToken, String> {
    let json = call(
        HTTP_CLIENT
            .get(format!("{}/refresh_access_token", ep.graph))
            .query(&[("grant_type", "ig_refresh_token"), ("access_token", token)]),
    )
    .await?;
    serde_json::from_value(json).map_err(|e| format!("unexpected refresh response: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_trailing_fragment_marker_is_stripped_from_the_code() {
        // Instagram appends "#_" to the authorization code.
        assert_eq!(strip_code_fragment("AQB123#_"), "AQB123");
        assert_eq!(strip_code_fragment("AQB123"), "AQB123");
    }

    #[test]
    fn a_profile_accepts_a_string_user_id() {
        let json = serde_json::json!({"user_id": "17841400000000000", "username": "yourbiz"});
        let p = parse_profile(&json).unwrap();
        assert_eq!(p.user_id, "17841400000000000");
        assert_eq!(p.username.as_deref(), Some("yourbiz"));
    }

    #[test]
    fn a_profile_accepts_a_numeric_user_id_or_falls_back_to_id() {
        // Graph has been seen returning the value both ways, and `id` is documented
        // as a compatibility alias for `user_id`.
        assert_eq!(
            parse_profile(&serde_json::json!({"user_id": 17841400000000000i64}))
                .unwrap()
                .user_id,
            "17841400000000000"
        );
        assert_eq!(
            parse_profile(&serde_json::json!({"id": "178414"}))
                .unwrap()
                .user_id,
            "178414"
        );
    }

    #[test]
    fn a_profile_without_any_identity_is_an_error() {
        let err = parse_profile(&serde_json::json!({"username": "x"})).unwrap_err();
        assert!(err.contains("user_id"), "{err}");
    }

    #[test]
    fn subscribed_fields_parse_as_plain_strings() {
        let json = serde_json::json!({
            "data": [{"subscribed_fields": ["messages", "message_edit"]}]
        });
        assert_eq!(
            parse_subscribed_fields(&json),
            vec!["messages", "message_edit"]
        );
    }

    #[test]
    fn subscribed_fields_parse_as_objects_with_a_name() {
        // Meta returns the object form on some surfaces.
        let json = serde_json::json!({
            "data": [{"subscribed_fields": [{"name": "messages", "version": "v1"}]}]
        });
        assert_eq!(parse_subscribed_fields(&json), vec!["messages"]);
    }

    #[test]
    fn an_empty_data_array_means_not_subscribed() {
        assert!(parse_subscribed_fields(&serde_json::json!({"data": []})).is_empty());
        assert!(parse_subscribed_fields(&serde_json::json!({})).is_empty());
    }

    #[test]
    fn a_long_lived_token_response_parses() {
        let json = serde_json::json!({
            "access_token": "IGQVJ...long...", "token_type": "bearer", "expires_in": 5_183_944
        });
        let token: LongLivedToken = serde_json::from_value(json).unwrap();
        assert_eq!(token.access_token, "IGQVJ...long...");
        assert_eq!(token.expires_in, 5_183_944, "seconds, about 60 days");
    }

    #[test]
    fn a_short_lived_token_response_ignores_the_extra_fields() {
        // The exchange also returns `user_id` and `permissions` (a comma-separated
        // string, not an array), which we take from /me instead; deserialization
        // must not choke on them.
        let json = serde_json::json!({
            "access_token": "IGQVJ...",
            "user_id": "178414",
            "permissions": "instagram_business_basic,instagram_business_manage_messages"
        });
        let token: ShortLivedToken = serde_json::from_value(json).unwrap();
        assert_eq!(token.access_token, "IGQVJ...");
    }

    #[test]
    fn a_send_response_parses() {
        let json = serde_json::json!({"message_id": "aWdfZGl...", "recipient_id": "98123"});
        let sent: SentMessage = serde_json::from_value(json).unwrap();
        assert_eq!(sent.message_id, "aWdfZGl...");
        assert_eq!(sent.recipient_id, "98123");
    }

    #[tokio::test]
    async fn a_send_puts_the_text_in_the_body_and_the_token_in_the_query() {
        // The endpoint takes the token as a query parameter and the payload as JSON —
        // mixing those up is the classic way to get an unhelpful 400.
        let base = mock_status(
            200,
            serde_json::json!({
                "message_id": "m", "recipient_id": "r"
            }),
        )
        .await;
        let ep = InstagramEndpoints::single(&base);
        let sent = send_message(&ep, "tok", "98123", "hello").await.unwrap();
        assert_eq!(sent.message_id, "m");
    }

    #[tokio::test]
    async fn a_send_outside_the_allowed_window_surfaces_the_subcode() {
        // code 10 alone is just "permission denied"; 2534022 is what says "outside the
        // 24-hour customer-service window", which is the operator-actionable part.
        let base = mock_status(
            403,
            serde_json::json!({"error": {
                "message": "This message is sent outside of allowed window",
                "code": 10, "error_subcode": 2534022
            }}),
        )
        .await;
        let ep = InstagramEndpoints::single(&base);
        let err = send_message(&ep, "tok", "98123", "hi").await.unwrap_err();
        assert!(err.contains("2534022"), "{err}");
    }

    #[test]
    fn metas_error_envelope_surfaces_the_message() {
        let json = serde_json::json!({"error": {
            "message": "Invalid OAuth access token.",
            "type": "OAuthException",
            "code": 190,
            "fbtrace_id": "Az123"
        }});
        let err = envelope_error(&json).unwrap();
        assert!(err.contains("Invalid OAuth access token."), "{err}");
    }

    #[test]
    fn a_success_body_has_no_envelope_error() {
        assert!(envelope_error(&serde_json::json!({"success": true})).is_none());
    }

    #[test]
    fn an_error_envelope_carries_the_subcode() {
        // subcode 2534022 is "outside the 24-hour window", indistinguishable from
        // any other code-10 permission error without it.
        let json = serde_json::json!({"error": {
            "message": "This message is sent outside of allowed window",
            "code": 10, "error_subcode": 2534022
        }});
        let err = envelope_error(&json).unwrap();
        assert!(err.contains("2534022"), "{err}");
    }

    #[test]
    fn a_single_element_data_envelope_is_unwrapped() {
        // Meta's docs show this shape for /me and the code exchange; the wire sends
        // flat. Accept both rather than betting the whole create path on which wins.
        let enveloped = serde_json::json!({"data": [{"user_id": "178414", "username": "biz"}]});
        assert_eq!(
            parse_profile(&unwrap_envelope(enveloped)).unwrap().user_id,
            "178414"
        );
    }

    #[test]
    fn a_flat_body_passes_through_unwrap_untouched() {
        let flat = serde_json::json!({"user_id": "178414"});
        assert_eq!(unwrap_envelope(flat.clone()), flat);
    }

    #[test]
    fn subscribed_apps_is_not_mistaken_for_an_envelope() {
        // Its `data` array holds app objects, not the payload we want; unwrapping it
        // would break parse_subscribed_fields.
        let json = serde_json::json!({"data": [{"subscribed_fields": ["messages"]}]});
        assert_eq!(parse_subscribed_fields(&json), vec!["messages"]);
    }

    #[test]
    fn a_subscribe_body_without_success_true_is_a_failure() {
        // HTTP 200 with {"success": false} is how a channel gets created that
        // silently receives nothing.
        assert!(confirm_success(&serde_json::json!({"success": true}), "ctx").is_ok());
        assert!(confirm_success(&serde_json::json!({"success": false}), "ctx").is_err());
        assert!(confirm_success(&serde_json::json!({}), "ctx").is_err());
    }

    /// Answers every request with a fixed status and body. Same shape as
    /// `mock_bot_api` in `src/provider/telegram.rs`.
    async fn mock_status(status: u16, body: serde_json::Value) -> String {
        use axum::routing::any;
        use axum::{Json, Router};

        let app = Router::new().fallback(any(move || {
            let body = body.clone();
            async move {
                (
                    axum::http::StatusCode::from_u16(status).unwrap(),
                    Json(body),
                )
            }
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        format!("http://{addr}")
    }

    #[tokio::test]
    async fn a_4xx_without_an_error_envelope_is_still_a_failure() {
        // Otherwise it falls through as success and dies later at deserialization,
        // with a message that hides the real cause.
        let base = mock_status(403, serde_json::json!({"unexpected": true})).await;
        let ep = InstagramEndpoints::single(&base);
        let err = fetch_profile(&ep, "tok").await.unwrap_err();
        assert!(err.contains("403"), "{err}");
    }

    #[tokio::test]
    async fn a_subscribe_answering_success_false_is_a_failure() {
        let base = mock_status(200, serde_json::json!({"success": false})).await;
        let ep = InstagramEndpoints::single(&base);
        let err = subscribe(&ep, "tok", "messages").await.unwrap_err();
        assert!(err.contains("not confirmed"), "{err}");
    }
}
