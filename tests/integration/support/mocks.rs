//! Stand-ins for the Graph API and the Bot API.

use std::sync::Arc;

use axum::http::StatusCode;
use sqlx::PgPool;
use uuid::Uuid;

use super::*;
use chatbridge::config::AppState;

/// One request the mock saw. A named struct rather than a tuple because two
/// different features POST to `me/messages` — a send and a seen marker — so method
/// and path stopped being enough to tell them apart.
#[derive(Debug, Clone)]
pub struct SeenRequest {
    pub method: String,
    pub path: String,
    pub query: String,
    pub body: String,
}

/// Every request the mock saw, in order.
pub type Requests = Arc<std::sync::Mutex<Vec<SeenRequest>>>;

/// One server for all three Meta hosts. `responses` is keyed by the request path
/// with its leading slash stripped, e.g. "me" or "me/subscribed_apps"; anything
/// unlisted answers `{"success": true}`.
///
/// It **records** every request, because the permissive default is a trap: a test
/// that only asserts on the database passes just as happily when the production
/// code never made the call at all. Any test whose name claims a provider call
/// happened has to assert against this log.
pub async fn spawn_mock_instagram_recording(responses: serde_json::Value) -> (String, Requests) {
    use axum::Router;
    use axum::extract::Request as AxumRequest;
    use axum::routing::any;

    let responses = Arc::new(responses);
    let seen: Requests = Arc::new(std::sync::Mutex::new(Vec::new()));
    let recorder = seen.clone();

    let app = Router::new().fallback(any(move |req: AxumRequest| {
        let responses = responses.clone();
        let recorder = recorder.clone();
        async move {
            let method = req.method().to_string();
            let path = req.uri().path().trim_start_matches('/').to_owned();
            let query = req.uri().query().unwrap_or_default().to_owned();
            let bytes = axum::body::to_bytes(req.into_body(), 64 * 1024)
                .await
                .unwrap_or_default();
            let body = String::from_utf8_lossy(&bytes).into_owned();
            recorder.lock().unwrap().push(SeenRequest {
                method,
                path: path.clone(),
                query,
                body,
            });
            let response = responses
                .get(path.as_str())
                .cloned()
                .unwrap_or_else(|| serde_json::json!({"success": true}));
            axum::Json(response)
        }
    }));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (format!("http://{addr}"), seen)
}

/// For tests that do not need the request log.
pub async fn spawn_mock_instagram(responses: serde_json::Value) -> String {
    spawn_mock_instagram_recording(responses).await.0
}

/// Did the mock see a subscribe carrying every field we mean to subscribe to?
///
/// reqwest percent-encodes the comma in a query value, so the expected list is
/// matched on `%2C`.
pub fn subscribed_all_fields(requests: &Requests) -> bool {
    let wanted = "subscribed_fields=messages%2Cmessage_edit%2Cmessage_reactions%2Cmessaging_seen";
    requests
        .lock()
        .unwrap()
        .iter()
        .any(|r| r.method == "POST" && r.path == "me/subscribed_apps" && r.query.contains(wanted))
}

pub fn saw(requests: &Requests, method: &str, path: &str) -> bool {
    requests
        .lock()
        .unwrap()
        .iter()
        .any(|r| r.method == method && r.path == path)
}

/// How many times the mock saw this method and path.
pub fn count(requests: &Requests, method: &str, path: &str) -> usize {
    requests
        .lock()
        .unwrap()
        .iter()
        .filter(|r| r.method == method && r.path == path)
        .count()
}

/// Did the mock see a request to `path` whose body contains `needle`?
pub fn saw_body(requests: &Requests, path: &str, needle: &str) -> bool {
    requests
        .lock()
        .unwrap()
        .iter()
        .any(|r| r.path == path && r.body.contains(needle))
}

/// A mock that walks the whole happy path: code → short → long → profile → subscribe.
pub fn instagram_login_ok(user_id: &str, username: &str) -> serde_json::Value {
    serde_json::json!({
        "oauth/access_token": {"access_token": "short_lived", "user_id": user_id},
        "access_token": {"access_token": "long_lived", "token_type": "bearer", "expires_in": 5_183_944},
        "me": {"user_id": user_id, "username": username, "id": user_id},
        "me/subscribed_apps": {"success": true},
    })
}

/// Drive the callback the way the browser would, with a state this deployment signed.
pub async fn oauth_callback(
    state: Arc<AppState>,
    channel_id: Option<Uuid>,
) -> (StatusCode, String) {
    let token = chatbridge::oauth::sign_state(
        TEST_JWT_SECRET.as_bytes(),
        chatbridge::model::ProviderKind::Instagram,
        channel_id,
    );
    let (status, _, body) = request_raw(
        state,
        "GET",
        &format!("/api/oauth/instagram/callback?code=AQB123&state={token}"),
    )
    .await;
    (status, body)
}

/// Load a channel and its parsed config so a test can drive one refresh directly.
pub async fn channel_for_refresh(
    pool: &PgPool,
    id: Uuid,
) -> (chatbridge::db::Channel, chatbridge::model::InstagramConfig) {
    let channel = chatbridge::db::find_live_channel_by_id(pool, id)
        .await
        .unwrap()
        .unwrap();
    let config = serde_json::from_value(channel.config.clone()).unwrap();
    (channel, config)
}

/// Spawn a fake Bot API that answers `/bot<token>/<method>` from `responses`,
/// defaulting to `{"ok":true,"result":{}}`.
pub async fn spawn_mock_telegram(responses: serde_json::Value) -> String {
    use axum::Router;
    use axum::extract::Path;
    use axum::routing::any;

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
                axum::Json(body)
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

pub fn get_me_ok(bot_id: i64) -> serde_json::Value {
    serde_json::json!({"ok": true, "result": {
        "id": bot_id, "is_bot": true, "first_name": "Acme", "username": "acme_bot"
    }})
}
