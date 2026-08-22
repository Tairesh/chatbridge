//! Integration tests. Requires DATABASE_URL and REDIS_URL.
//!
//! `tests/<name>/main.rs` is a single test target named `<name>`, so this is still
//! one binary — the split is for readability, not isolation.

// `common` is shared with tests/client_identity.rs and stays where it is.
#[path = "../common/mod.rs"]
mod common;

mod support;

mod cache;
mod channels_api;
mod channels_db;
mod connection;
mod instagram_flow;
mod messages;
mod oauth;
mod operator_ws;
mod read_receipts;
mod refresh;
mod webhooks;
mod widget_ws;
