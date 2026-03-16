# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Run

- **Build:** `cargo build`
- **Run:** `cargo run` (requires env vars below)
- **Lint:** `cargo clippy`
- **Format:** `cargo fmt`
- **Unit tests (no DB):** `cargo test --lib`
- **All tests (needs Postgres + Redis):** `DATABASE_URL=postgres://chatbridge:chatbridge@localhost:5432/chatbridge REDIS_URL=redis://localhost:6379 cargo test`
- **Run single test:** `cargo test <test_name>` or `cargo test --test <file_name>` for a specific test file
- **Docker (full stack):** `docker compose up --build`

## Gotchas

- reqwest 0.13+ TLS feature is `rustls` (not `rustls-tls`)
- DB client functions (`upsert_client`, `find_client_by_external_id`) accept `ProviderKind` enum, not `&str`
- `reqwest::Client` is a `LazyLock` static in `provider/instagram.rs` — don't create new clients per-request
- Integration tests use RAII drop guards (`TestChannel`, `TestClient`) in `tests/common/mod.rs` for DB cleanup — always use these instead of manual DELETE queries

## Environment Variables

Required at runtime:
- `META_VERIFY_TOKEN` — token for Meta/Instagram webhook subscription handshake
- `INSTAGRAM_APP_SECRET` — HMAC-SHA256 secret for Instagram payload signature validation
- `DATABASE_URL` — Postgres connection string (e.g. `postgres://chatbridge:chatbridge@localhost:5432/chatbridge`)
- `REDIS_URL` — Redis connection string (e.g. `redis://localhost:6379`)
- `WIDGET_JWT_SECRET` — HMAC-SHA256 secret for signing/verifying WebSocket widget JWTs


## Project Overview

Multi-provider chat bridge for Instagram, Telegram, and WebSocket chat widgets, built with Axum, Tokio, sqlx, and Redis. Lib crate (`src/lib.rs`) + binary entrypoint (`src/main.rs`). Postgres stores channel configuration; Redis handles cross-replica pub/sub for message routing. Migrations run automatically on startup.

### Module Structure

- `cache.rs` — `ChannelCache` (in-memory read-through for channel lookups) + `ClientCache` (in-memory read-through for client lookups by `(ProviderKind, external_id)`), both invalidated via Redis Pub/Sub. Also: `publish_invalidation` helper, `spawn_invalidation_listener`
- `jwt.rs` — HS256 JWT sign/verify for WebSocket widget client identity (`Claims { sub, iat }`)
- `registry.rs` — `ClientRegistry` (tracks active WS connections per client UUID via `RwLock<HashMap<Uuid, HashSet<u64>>>`)
- `config.rs` — `AppConfig` (from env vars) and `AppState` (config + PgPool + Redis + ChannelCache + ClientCache + ClientRegistry + shutdown token). `AppState` does NOT derive `Clone` — it's always behind `Arc<AppState>`
- `db.rs` — Pool init, migrations, channel lookup queries, `Client` struct, `upsert_client`, `find_client_by_external_id`. `InstagramChannel` includes `access_token`
- `error.rs` — `WebhookError` enum with `IntoResponse` (database errors are logged but not leaked to clients)
- `model.rs` — `InternalMessage` (with optional `client_id`), `ProviderKind`, `EventKind`, `WsInbound`, `WsActionKind` (`send`/`edit`/`read`), `WsOutbound` (`Auth`/`Ack`/`Error`)
- `provider/mod.rs` — `WebhookProvider` trait (verify + parse). `parse` takes `redis: ConnectionManager` for cache invalidation publishing
- `provider/instagram.rs` — Constant-time HMAC-SHA256 verification, Meta webhook payload parsing, client resolution via Instagram Graph API (background `tokio::spawn` with 24h staleness check)
- `provider/telegram.rs` — Secret token verification, Telegram Update parsing, `TelegramUser` struct, `resolve_telegram_client` (cache lookup → 24h staleness → background upsert, same pattern as Instagram)
- `handler.rs` — Axum handlers (`meta_verify`, `instagram_ingest`, `telegram_ingest`, `widget_ws`)
- `routes.rs` — Router assembly

### Routes

| Method | Path | Handler | Notes |
|--------|------|---------|-------|
| GET | `/webhook/instagram` | `meta_verify` | hub.challenge handshake |
| POST | `/webhook/instagram` | `instagram_ingest` | HMAC via `INSTAGRAM_APP_SECRET`, channel lookup by sender/recipient ID |
| POST | `/webhook/telegram/{channel_id}` | `telegram_ingest` | Secret token from DB by channel UUID |
| GET | `/ws/{widget_id}` | `widget_ws` | WebSocket upgrade, validates widget_id against DB, publishes to Redis |

### Handler Flow (HTTP webhooks)

1. Extract headers + raw body
2. `provider.verify(headers, body)` → 403 if invalid (Instagram uses constant-time HMAC via `verify_slice`)
3. Return 200 OK immediately
4. `tokio::spawn` (instrumented with tracing spans) → parse payload, lookup channel via in-memory cache (read-through to DB on miss), log `InternalMessage` to stdout, publish to Redis (`instagram:{channel_id}` / `telegram:{channel_id}`)

### Handler Flow (WebSocket widget)

1. Validate `widget_id` via in-memory cache (read-through to `widget_channels` table) → 404 if unknown
2. Upgrade to WebSocket connection
3. JWT issued on connect (`WsOutbound::Auth`), client registered in `ClientRegistry` (RAII drop guard for deregister)
4. Message loop via `tokio::select!`: idle timeout (5 min) triggers ping/pong keepalive, shutdown cancellation sends close frame. All sends wrapped in 5s timeout for backpressure
5. Receive JSON `{"action": "send"|"edit", "mid": "uuid", "text": "...", "attachments": ["uuid", ...]}` → validate → log → publish to Redis (`widget:{channel_id}`) → send ACK
6. Unknown actions, malformed messages, and invalid `mid` values get error response; connection stays alive
7. On shutdown signal: `CancellationToken` triggers close frame to all clients, main.rs drain loop waits up to 10s

### Channel Cache

Channel and client data is cached in-memory (`ChannelCache` and `ClientCache` in `cache.rs`) to avoid hitting Postgres on every incoming webhook/message. Both caches use read-through: on miss, queries DB and stores the result. Invalidation is via Redis Pub/Sub on a universal `cache_invalidation` topic:

```
PUBLISH cache_invalidation "channel:<uuid>"
PUBLISH cache_invalidation "client:<uuid>"
```

A background task (`spawn_invalidation_listener`) subscribes to the `cache_invalidation` topic and dispatches by entity type (`channel` → `ChannelCache`, `client` → `ClientCache`). All replicas receive the event and update their local cache. Providers call `publish_invalidation` after every `upsert_client`.

### Database

Tables: `instagram_channels`, `telegram_channels`, `widget_channels`, `clients`. Migrations in `migrations/`. Schema managed by sqlx with auto-run on startup.

### Docker

`compose.yaml` runs `nginx` + `chatbridge` + `postgres:16-alpine` + `redis:7-alpine`. Dockerfile and nginx.conf live in `docker/`. Config via `.env` file (see `.env.example`).

### Specs & Plans

- Design specs: `docs/superpowers/specs/`
- Implementation plans: `docs/superpowers/plans/`
