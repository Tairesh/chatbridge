//! Provider-generic OAuth: one pair of routes, one state format, one popup
//! contract for every provider that has a login.
//!
//! Dispatch is a `match` on `ProviderKind`, not a `dyn` trait: `async fn` in a
//! trait is not object-safe without `async_trait`, and the existing
//! `WebhookProvider` already establishes the pattern of a trait used through a
//! concrete type. Adding WhatsApp is a new file plus one arm per function.

pub mod instagram;

use serde::Serialize;

use crate::config::AppConfig;
use crate::model::{InstagramConfig, ProviderKind};

/// Instagram Business Login. `instagram_business_basic` is required for `/me`;
/// `instagram_business_manage_messages` is what makes DMs flow at all.
pub const INSTAGRAM_SCOPES: &str = "instagram_business_basic,instagram_business_manage_messages";

/// Every field `provider::instagram::classify_event` can handle. Meta accepts all
/// four on `POST /me/subscribed_apps` and echoes them back from the GET, which is
/// what "the field exists" means here.
///
/// `messages` and `message_edit` are additionally confirmed in production by the
/// sibling PHP integration. `message_reads` is a Facebook Page field and does not
/// exist on the `instagram` object — do not "correct" `messaging_seen` to it.
pub const INSTAGRAM_FIELDS: [&str; 4] = [
    "messages",
    "message_edit",
    "message_reactions",
    "messaging_seen",
];

/// What a provider learned about the account, expressed in `channels` terms.
pub struct Connected {
    /// Provider identity → `channels.external_key`. Instagram: the IG-User-ID.
    pub external_key: String,
    /// Proposed `channels.name` for a channel that does not exist yet.
    pub display_name: String,
    /// Provider config blob → `channels.config`.
    pub config: serde_json::Value,
}

/// One entry of `GET /api/oauth/providers`. The panel hardcodes nothing about a
/// provider, so everything it needs to render and open the popup is here.
#[derive(Debug, Serialize)]
pub struct ProviderDescriptor {
    /// Lowercase, matching the `provider` field of `GET /api/channels`.
    pub provider: String,
    pub label: &'static str,
    pub start_path: String,
    /// Must be registered by hand in the provider's dashboard, byte for byte.
    pub redirect_uri: String,
}

pub fn supports(kind: ProviderKind) -> bool {
    matches!(kind, ProviderKind::Instagram)
}

fn label(kind: ProviderKind) -> &'static str {
    match kind {
        ProviderKind::Instagram => "Instagram",
        ProviderKind::Telegram => "Telegram",
        ProviderKind::Widget => "Widget",
    }
}

pub fn start_path(kind: ProviderKind) -> String {
    format!("/api/oauth/{kind}/start")
}

pub fn redirect_uri(cfg: &AppConfig, kind: ProviderKind) -> String {
    format!("{}/api/oauth/{kind}/callback", cfg.public_base_url)
}

pub fn descriptors(cfg: &AppConfig) -> Vec<ProviderDescriptor> {
    [
        ProviderKind::Instagram,
        ProviderKind::Telegram,
        ProviderKind::Widget,
    ]
    .into_iter()
    .filter(|k| supports(*k))
    .map(|kind| ProviderDescriptor {
        provider: kind.to_string(),
        label: label(kind),
        start_path: start_path(kind),
        redirect_uri: redirect_uri(cfg, kind),
    })
    .collect()
}

pub fn authorize_url(kind: ProviderKind, cfg: &AppConfig, state: &str) -> Result<String, String> {
    if !supports(kind) {
        return Err(format!("{kind} has no OAuth login"));
    }
    // Built through `Url` rather than `format!` so the redirect URI and the scope
    // list are percent-encoded correctly; Meta rejects a redirect_uri that does not
    // match the registered one byte for byte.
    let mut url = reqwest::Url::parse(&format!("{}/oauth/authorize", cfg.instagram.authorize))
        .map_err(|e| format!("bad instagram authorize base: {e}"))?;
    url.query_pairs_mut()
        .append_pair("client_id", &cfg.instagram_app_id)
        .append_pair("redirect_uri", &redirect_uri(cfg, kind))
        .append_pair("response_type", "code")
        .append_pair("scope", INSTAGRAM_SCOPES)
        .append_pair("state", state);
    Ok(url.to_string())
}

/// Read the bearer credential out of a stored `channels.config` blob.
fn access_token(config: &serde_json::Value) -> Result<String, String> {
    config["access_token"]
        .as_str()
        .map(str::to_owned)
        .ok_or_else(|| "channel config has no access_token".to_owned())
}

pub async fn exchange(
    kind: ProviderKind,
    cfg: &AppConfig,
    code: &str,
) -> Result<Connected, String> {
    if !supports(kind) {
        return Err(format!("{kind} has no OAuth login"));
    }
    let short = instagram::exchange_code(
        &cfg.instagram,
        &cfg.instagram_app_id,
        &cfg.instagram_app_secret,
        &redirect_uri(cfg, kind),
        code,
    )
    .await?;
    // The short-lived token lives about an hour and is never stored.
    let long = instagram::exchange_long_lived(
        &cfg.instagram,
        &cfg.instagram_app_secret,
        &short.access_token,
    )
    .await?;
    let profile = instagram::fetch_profile(&cfg.instagram, &long.access_token).await?;

    let display_name = match profile.username {
        Some(ref username) => format!("@{username}"),
        None => format!("instagram:{}", profile.user_id),
    };
    let config = serde_json::to_value(InstagramConfig {
        access_token: long.access_token,
        token_expires_at: Some(chrono::Utc::now() + chrono::TimeDelta::seconds(long.expires_in)),
        username: profile.username,
    })
    .map_err(|e| format!("failed to serialize instagram config: {e}"))?;

    Ok(Connected {
        external_key: profile.user_id,
        display_name,
        config,
    })
}

/// Subscribe the account to the events we can handle, returning what actually took.
///
/// A single unrecognised field name makes Meta reject the whole call, and that call
/// is on the create path — a wrong guess would make channel creation impossible. So
/// the failure path probes each field alone to find the bad ones, then issues one
/// final call with the survivors.
///
/// That final call is not optional. Whether Meta replaces the field set or adds to
/// it is undocumented, and the probe loop's last request carries a single field: if
/// the semantics are "replace", stopping there would leave exactly one field
/// subscribed. Re-sending the accepted list is correct under either reading.
pub async fn subscribe(
    kind: ProviderKind,
    cfg: &AppConfig,
    config: &serde_json::Value,
) -> Result<Vec<String>, String> {
    if !supports(kind) {
        return Ok(Vec::new());
    }
    let token = access_token(config)?;
    let all: Vec<String> = INSTAGRAM_FIELDS.iter().map(|f| (*f).to_owned()).collect();

    let full_error = match instagram::subscribe(&cfg.instagram, &token, &all.join(",")).await {
        Ok(()) => {
            // "Are we actually subscribing?" has to be answerable from the log, not
            // only from a later GET that cannot say *which* app the subscription
            // belongs to.
            tracing::info!(fields = %all.join(","), "subscribed the account to instagram events");
            return Ok(all);
        }
        Err(e) => e,
    };

    let mut accepted = Vec::new();
    let mut rejected = Vec::new();
    for field in INSTAGRAM_FIELDS {
        match instagram::subscribe(&cfg.instagram, &token, field).await {
            Ok(()) => accepted.push(field.to_owned()),
            Err(e) => rejected.push(format!("{field} ({e})")),
        }
    }
    if accepted.is_empty() {
        return Err(format!(
            "no webhook field could be subscribed: {full_error}; per-field: {}",
            rejected.join("; ")
        ));
    }
    tracing::warn!(
        "instagram rejected webhook fields: {}; keeping {}",
        rejected.join("; "),
        accepted.join(",")
    );
    instagram::subscribe(&cfg.instagram, &token, &accepted.join(",")).await?;
    Ok(accepted)
}

pub async fn unsubscribe(
    kind: ProviderKind,
    cfg: &AppConfig,
    config: &serde_json::Value,
) -> Result<(), String> {
    if !supports(kind) {
        return Ok(());
    }
    let result = instagram::unsubscribe(&cfg.instagram, &access_token(config)?).await;
    if result.is_ok() {
        tracing::info!("unsubscribed the account from instagram events");
    }
    result
}

/// How long a login may stay in flight. The user is mid-flow, so ten minutes is
/// generous.
const STATE_TTL_SECONDS: i64 = 600;

/// The `state` parameter, round-tripped untouched by the provider.
///
/// Its own type rather than `jwt::Claims`, which is `{sub, iat}` and states "this
/// is client/operator X" — a different claim about a different subject.
///
/// It is not bound to a session, because the application has no sessions: the
/// settings panel is unauthenticated. It still carries the pinned channel and still
/// rejects a callback that no `start` on this deployment issued.
#[derive(Debug, Serialize, serde::Deserialize)]
pub struct OAuthState {
    /// Provider, which must match the one in the callback path.
    pub p: String,
    /// The channel whose "Reconnect" button opened the popup, if any.
    pub ch: Option<uuid::Uuid>,
    /// Makes two states issued in the same second differ.
    pub jti: uuid::Uuid,
    pub iat: i64,
    pub exp: i64,
}

pub fn sign_state(secret: &[u8], kind: ProviderKind, channel_id: Option<uuid::Uuid>) -> String {
    let now = chrono::Utc::now().timestamp();
    let claims = OAuthState {
        p: kind.to_string(),
        ch: channel_id,
        jti: uuid::Uuid::new_v4(),
        iat: now,
        exp: now + STATE_TTL_SECONDS,
    };
    jsonwebtoken::encode(
        &jsonwebtoken::Header::new(jsonwebtoken::Algorithm::HS256),
        &claims,
        &jsonwebtoken::EncodingKey::from_secret(secret),
    )
    .expect("JWT encoding cannot fail")
}

pub fn verify_state(secret: &[u8], token: &str, kind: ProviderKind) -> Result<OAuthState, String> {
    // The default validation checks `exp`, which is exactly the expiry we want.
    let validation = jsonwebtoken::Validation::new(jsonwebtoken::Algorithm::HS256);
    let data = jsonwebtoken::decode::<OAuthState>(
        token,
        &jsonwebtoken::DecodingKey::from_secret(secret),
        &validation,
    )
    .map_err(|e| format!("invalid or expired login attempt: {e}"))?;

    if data.claims.p != kind.to_string() {
        return Err(format!(
            "login attempt was started for provider '{}', not '{kind}'",
            data.claims.p
        ));
    }
    Ok(data.claims)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::InstagramEndpoints;

    fn test_config() -> AppConfig {
        AppConfig {
            instagram_verify_token: "vt".into(),
            instagram_app_secret: "secret".into(),
            instagram_app_id: "1234567890".into(),
            instagram: InstagramEndpoints::single("http://mock.test"),
            redis_url: "redis://localhost:6379".into(),
            app_jwt_secret: "test-secret-at-least-32-bytes-long!!".into(),
            public_base_url: "https://example.com".into(),
            telegram_api_base: "http://127.0.0.1:1".into(),
        }
    }

    #[test]
    fn only_instagram_supports_oauth_today() {
        assert!(supports(ProviderKind::Instagram));
        assert!(!supports(ProviderKind::Telegram));
        assert!(!supports(ProviderKind::Widget));
    }

    #[test]
    fn the_redirect_uri_is_derived_from_the_public_base_url() {
        // This exact string has to be registered by hand in the Meta dashboard and
        // Meta compares it byte for byte, so it is worth pinning in a test.
        assert_eq!(
            redirect_uri(&test_config(), ProviderKind::Instagram),
            "https://example.com/api/oauth/instagram/callback"
        );
    }

    #[test]
    fn the_descriptor_names_the_provider_in_lowercase() {
        // `GET /api/channels` reports `provider` as the lowercase database value;
        // the two lists are compared in the panel, so they must agree.
        let ds = descriptors(&test_config());
        assert_eq!(ds.len(), 1);
        assert_eq!(ds[0].provider, "instagram");
        assert_eq!(ds[0].start_path, "/api/oauth/instagram/start");
    }

    #[test]
    fn the_authorize_url_carries_every_parameter_meta_needs() {
        let url = authorize_url(ProviderKind::Instagram, &test_config(), "STATE").unwrap();
        assert!(
            url.starts_with("http://mock.test/oauth/authorize?"),
            "{url}"
        );
        assert!(url.contains("client_id=1234567890"), "{url}");
        assert!(url.contains("response_type=code"), "{url}");
        assert!(url.contains("state=STATE"), "{url}");
        // Both values are URL-encoded, so assert on the encoded forms.
        assert!(
            url.contains(
                "redirect_uri=https%3A%2F%2Fexample.com%2Fapi%2Foauth%2Finstagram%2Fcallback"
            ),
            "{url}"
        );
        assert!(
            url.contains("scope=instagram_business_basic%2Cinstagram_business_manage_messages"),
            "{url}"
        );
    }

    #[test]
    fn a_provider_without_oauth_cannot_produce_an_authorize_url() {
        assert!(authorize_url(ProviderKind::Widget, &test_config(), "S").is_err());
    }

    #[test]
    fn the_subscribed_field_set_matches_what_the_parser_handles() {
        // classify_event handles message, edit, reaction and read. Subscribing to
        // fewer means those events silently never arrive.
        assert_eq!(
            INSTAGRAM_FIELDS.join(","),
            "messages,message_edit,message_reactions,messaging_seen"
        );
    }

    #[test]
    fn the_access_token_is_read_out_of_the_stored_blob() {
        let cfg = serde_json::json!({"access_token": "tok", "username": "biz"});
        assert_eq!(access_token(&cfg).unwrap(), "tok");
        assert!(access_token(&serde_json::json!({})).is_err());
    }

    /// A Meta stand-in that rejects any subscribe whose field list contains
    /// `bad_field`, and accepts everything else.
    async fn mock_picky_meta(bad_field: &'static str) -> String {
        use axum::extract::Request;
        use axum::routing::any;
        use axum::{Json, Router};

        let app = Router::new().fallback(any(move |req: Request| async move {
            let fields = req
                .uri()
                .query()
                .unwrap_or_default()
                .split('&')
                .find_map(|p| p.strip_prefix("subscribed_fields="))
                .unwrap_or_default()
                .to_owned();
            if fields.split("%2C").any(|f| f == bad_field) {
                return Json(serde_json::json!({
                    "error": {"message": format!("(#100) Invalid field: {bad_field}"), "code": 100}
                }));
            }
            Json(serde_json::json!({"success": true}))
        }));

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        format!("http://{addr}")
    }

    /// Answers every request with the same error envelope.
    async fn mock_rejecting_meta() -> String {
        use axum::routing::any;
        use axum::{Json, Router};

        let app = Router::new().fallback(any(|| async {
            Json(serde_json::json!({
                "error": {"message": "(#100) Invalid field", "code": 100}
            }))
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        format!("http://{addr}")
    }

    #[tokio::test]
    async fn one_bad_field_name_costs_that_field_and_nothing_else() {
        // A rejected name must not make channel creation impossible, which is what a
        // plain "subscribe the whole list or fail" would do.
        let base = mock_picky_meta("message_edit").await;
        let mut cfg = test_config();
        cfg.instagram = InstagramEndpoints::single(&base);

        let accepted = subscribe(
            ProviderKind::Instagram,
            &cfg,
            &serde_json::json!({"access_token": "tok"}),
        )
        .await
        .unwrap();

        assert!(accepted.contains(&"messages".to_owned()));
        assert!(!accepted.contains(&"message_edit".to_owned()));
        assert_eq!(accepted.len(), INSTAGRAM_FIELDS.len() - 1);
    }

    #[tokio::test]
    async fn a_provider_that_rejects_everything_is_a_hard_failure() {
        // Nothing subscribed means no events at all, which is not a degraded channel
        // — it is a broken one, and the caller rolls the row back.
        //
        // `mock_picky_meta` is the wrong tool here: the probe loop sends one field per
        // request, so rejecting the single name "messages" still lets the other three
        // probes through and `subscribe` returns Ok. Reject everything instead.
        let base = mock_rejecting_meta().await;
        let mut cfg = test_config();
        cfg.instagram = InstagramEndpoints::single(&base);

        let err = subscribe(
            ProviderKind::Instagram,
            &cfg,
            &serde_json::json!({"access_token": "tok"}),
        )
        .await
        .unwrap_err();
        assert!(err.contains("no webhook field"), "{err}");
    }

    const SECRET: &[u8] = b"test-secret-at-least-32-bytes-long!!";

    #[test]
    fn a_state_round_trips_with_its_pinned_channel() {
        let channel_id = uuid::Uuid::new_v4();
        let token = sign_state(SECRET, ProviderKind::Instagram, Some(channel_id));
        let claims = verify_state(SECRET, &token, ProviderKind::Instagram).unwrap();
        assert_eq!(claims.p, "instagram");
        assert_eq!(claims.ch, Some(channel_id));
    }

    #[test]
    fn a_state_round_trips_without_a_channel() {
        let token = sign_state(SECRET, ProviderKind::Instagram, None);
        assert_eq!(
            verify_state(SECRET, &token, ProviderKind::Instagram)
                .unwrap()
                .ch,
            None
        );
    }

    #[test]
    fn two_states_issued_together_differ() {
        // Without `jti` two states signed in the same second would be byte-identical,
        // which makes them replayable across two concurrent login attempts.
        let a = sign_state(SECRET, ProviderKind::Instagram, None);
        let b = sign_state(SECRET, ProviderKind::Instagram, None);
        assert_ne!(a, b);
    }

    #[test]
    fn a_state_signed_with_another_secret_is_rejected() {
        let token = sign_state(
            b"another-secret-that-is-32-bytes!!!!!",
            ProviderKind::Instagram,
            None,
        );
        assert!(verify_state(SECRET, &token, ProviderKind::Instagram).is_err());
    }

    #[test]
    fn a_state_for_another_provider_is_rejected() {
        // The provider is in the path *and* in the state; a mismatch means the
        // callback was reached by a route it was not issued for.
        let token = sign_state(SECRET, ProviderKind::Instagram, None);
        let err = verify_state(SECRET, &token, ProviderKind::Telegram).unwrap_err();
        assert!(err.contains("provider"), "{err}");
    }

    #[test]
    fn an_expired_state_is_rejected() {
        let claims = OAuthState {
            p: "instagram".into(),
            ch: None,
            jti: uuid::Uuid::new_v4(),
            iat: 1_700_000_000,
            exp: 1_700_000_600,
        };
        let token = jsonwebtoken::encode(
            &jsonwebtoken::Header::new(jsonwebtoken::Algorithm::HS256),
            &claims,
            &jsonwebtoken::EncodingKey::from_secret(SECRET),
        )
        .unwrap();
        assert!(verify_state(SECRET, &token, ProviderKind::Instagram).is_err());
    }

    #[test]
    fn a_garbage_state_is_rejected() {
        assert!(verify_state(SECRET, "not.a.jwt", ProviderKind::Instagram).is_err());
    }
}
