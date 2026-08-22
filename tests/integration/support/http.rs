//! Driving the HTTP surface of the app under test.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use tower::ServiceExt;

use chatbridge::config::AppState;
use chatbridge::routes;

pub async fn post_json(
    state: Arc<AppState>,
    uri: &str,
    body: serde_json::Value,
) -> (StatusCode, serde_json::Value) {
    request_json(state, "POST", uri, Some(body)).await
}

pub async fn request_json(
    state: Arc<AppState>,
    method: &str,
    uri: &str,
    body: Option<serde_json::Value>,
) -> (StatusCode, serde_json::Value) {
    let app = routes::build(state);
    let builder = Request::builder().method(method).uri(uri);
    let request = match body {
        Some(b) => builder
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&b).unwrap()))
            .unwrap(),
        None => builder.body(Body::empty()).unwrap(),
    };
    let resp = app.oneshot(request).await.unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let json = serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null);
    (status, json)
}

pub async fn request_raw(
    state: Arc<AppState>,
    method: &str,
    uri: &str,
) -> (StatusCode, axum::http::HeaderMap, String) {
    let app = routes::build(state);
    let request = Request::builder()
        .method(method)
        .uri(uri)
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(request).await.unwrap();
    let status = resp.status();
    let headers = resp.headers().clone();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    (
        status,
        headers,
        String::from_utf8_lossy(&bytes).into_owned(),
    )
}

/// `request_json` parses the body, and an `AppError` renders as plain text — so a 4xx
/// comes back as `Null` and its message is lost. This keeps the text.
pub async fn request_text(
    state: Arc<AppState>,
    method: &str,
    uri: &str,
    body: Option<serde_json::Value>,
) -> (StatusCode, String) {
    let app = routes::build(state);
    let builder = Request::builder().method(method).uri(uri);
    let request = match body {
        Some(b) => builder
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&b).unwrap()))
            .unwrap(),
        None => builder.body(Body::empty()).unwrap(),
    };
    let resp = app.oneshot(request).await.unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    (status, String::from_utf8_lossy(&bytes).into_owned())
}
