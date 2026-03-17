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
- Integration tests use RAII drop guards (`TestChannel`, `TestClient`, `TestChat`, `TestMessage`) in `tests/common/mod.rs` for DB cleanup — always use these instead of manual DELETE queries
- Channel insert helpers in integration tests must INSERT into `channels` table first, then the provider-specific table (FK constraint)
- Instagram/Telegram client resolution is async — `sender_id` may not exist in `clients` yet when the message is persisted. `persist_and_publish` verifies client existence before using `sender_id` for FK-constrained inserts

## Environment Variables

Required at runtime:
- `META_VERIFY_TOKEN` — token for Meta/Instagram webhook subscription handshake
- `INSTAGRAM_APP_SECRET` — HMAC-SHA256 secret for Instagram payload signature validation
- `DATABASE_URL` — Postgres connection string (e.g. `postgres://chatbridge:chatbridge@localhost:5432/chatbridge`)
- `REDIS_URL` — Redis connection string (e.g. `redis://localhost:6379`)
- `WIDGET_JWT_SECRET` — HMAC-SHA256 secret for signing/verifying WebSocket JWTs (used for both widget clients and operators)


## Project Overview

Multi-provider chat bridge for Instagram, Telegram, and WebSocket chat widgets, built with Axum, Tokio, sqlx, and Redis. Lib crate (`src/lib.rs`) + binary entrypoint (`src/main.rs`). Postgres stores channel configuration; Redis handles cross-replica pub/sub for message routing. Migrations run automatically on startup.

### Module Structure

- `cache.rs` — `ChannelCache`, `ClientCache`, `OperatorCache`, `ChatCache` — all in-memory read-through, invalidated via Redis Pub/Sub. Also: `publish_invalidation` helper, `spawn_invalidation_listener`
- `jwt.rs` — HS256 JWT sign/verify for WebSocket widget client identity (`Claims { sub, iat }`)
- `registry.rs` — `ClientRegistry` (tracks active WS connections per client/operator UUID via `RwLock<HashMap<Uuid, HashMap<u64, mpsc::Sender<String>>>>` + `operator_ids: HashSet<Uuid>`)
- `config.rs` — `AppConfig` (from env vars) and `AppState` (config + PgPool + Redis + ChannelCache + ClientCache + OperatorCache + ChatCache + ClientRegistry + shutdown token). `AppState` does NOT derive `Clone` — it's always behind `Arc<AppState>`
- `db.rs` — Pool init, migrations, channel lookup queries, `Client`/`Operator`/`ChatInfo`/`ChatSummary`/`ChatMessage` structs, `upsert_client`, `find_client_by_external_id`, `find_or_create_chat`, `insert_message` (with dedup), `edit_message`, `mark_messages_read` (watermark), `mark_messages_read_by_id` (by UUID). `InstagramChannel` includes `access_token`
- `error.rs` — `WebhookError` enum with `IntoResponse` (database errors are logged but not leaked to clients)
- `model.rs` — `Sender`, `NewMessage` (pre-insert), `IncomingMessage`/`IncomingEdit`/`IncomingRead` (post-insert), `IncomingEvent` (tagged enum for Redis: `message`/`edit`/`read`), `ProviderKind`, `EventKind`, `WsInbound`, `OperatorInbound`, `WsActionKind` (`send`/`edit`/`read`), `WsOutbound` (`Auth`/`Ack`/`Error`)
- `provider/mod.rs` — `WebhookProvider` trait (verify + parse). `parse` takes `redis: ConnectionManager` for cache invalidation publishing
- `provider/instagram.rs` — Constant-time HMAC-SHA256 verification, Meta webhook payload parsing, client resolution via Instagram Graph API (background `tokio::spawn` with 24h staleness check)
- `provider/telegram.rs` — Secret token verification, Telegram Update parsing, `TelegramUser` struct, `resolve_telegram_client` (cache lookup → 24h staleness → background upsert, same pattern as Instagram)
- `handler.rs` — Axum handlers (`meta_verify`, `instagram_ingest`, `telegram_ingest`, `widget_ws`, `operator_ws`, `get_chats`, `get_chat_messages`), `spawn_message_listener` (shared Redis listener for dispatching events to WS clients). In read events (`IncomingRead`), `sender` = original message author (not the reader). The shared listener uses this for routing: if sender matches chat's client → deliver to client; otherwise → deliver to operators
- `routes.rs` — Router assembly

### Routes

| Method | Path | Handler | Notes |
|--------|------|---------|-------|
| GET | `/webhook/instagram` | `meta_verify` | hub.challenge handshake |
| POST | `/webhook/instagram` | `instagram_ingest` | HMAC via `INSTAGRAM_APP_SECRET`, channel lookup by sender/recipient ID |
| POST | `/webhook/telegram/{channel_id}` | `telegram_ingest` | Secret token from DB by channel UUID |
| GET | `/ws/{widget_id}` | `widget_ws` | WebSocket upgrade, validates widget_id against DB, publishes to Redis |
| GET | `/api/chats` | `get_chats` | List active chats with last message summary |
| GET | `/api/chats/{chat_id}` | `get_chat_messages` | Chat message history |
| GET | `/ws/operator` | `operator_ws` | Operator WebSocket, JWT auth via `?token=` query param |

### Handler Flow (HTTP webhooks)

1. Extract headers + raw body
2. `provider.verify(headers, body)` → 403 if invalid (Instagram uses constant-time HMAC via `verify_slice`)
3. Return 200 OK immediately
4. `tokio::spawn` (instrumented with tracing spans) → parse payload → `NewMessage`, branch on `EventKind`: Message → verify sender, resolve chat, insert (dedup), publish `IncomingEvent::Message`; Edit → update text, publish `IncomingEvent::Edit`; Read → watermark mark-read, publish `IncomingEvent::Read`; Reaction/Unknown → log only

### Handler Flow (WebSocket widget)

1. Validate `widget_id` via in-memory cache (read-through to `widget_channels` table) → 404 if unknown
2. Upgrade to WebSocket connection
3. JWT issued on connect (`WsOutbound::Auth`), client registered in `ClientRegistry` (RAII drop guard for deregister)
4. Message loop via `tokio::select!`: idle timeout (5 min) triggers ping/pong keepalive, shutdown cancellation sends close frame. All sends wrapped in 5s timeout for backpressure
5. Receive JSON `{"action": "send"|"edit"|"read", "mid": "uuid", "text": "...", "attachments": ["uuid", ...]}` → validate → persist/update DB → publish `IncomingEvent` to Redis `incoming_messages` → send ACK
6. Unknown actions, malformed messages, and invalid `mid` values get error response; connection stays alive
7. On shutdown signal: `CancellationToken` triggers close frame to all clients, main.rs drain loop waits up to 10s

### Channel Cache

Channel and client data is cached in-memory (`ChannelCache` and `ClientCache` in `cache.rs`) to avoid hitting Postgres on every incoming webhook/message. Both caches use read-through: on miss, queries DB and stores the result. Invalidation is via Redis Pub/Sub on a universal `cache_invalidation` topic:

```
PUBLISH cache_invalidation "channel:<uuid>"
PUBLISH cache_invalidation "client:<uuid>"
PUBLISH cache_invalidation "operator:<uuid>"
PUBLISH cache_invalidation "chat:<uuid>"
```

A background task (`spawn_invalidation_listener`) subscribes to the `cache_invalidation` topic and dispatches by entity type (`channel` → `ChannelCache`, `client` → `ClientCache`). All replicas receive the event and update their local cache. Providers call `publish_invalidation` after every `upsert_client`.

### Database

Tables: `channels`, `instagram_channels`, `telegram_channels`, `widget_channels`, `clients`, `operators`, `chats`, `messages`. The `channels` table is the unified parent — provider-specific tables have FK to `channels(id)`. `chats` tracks active conversations per `(client_id, channel_id)` with partial unique index. `messages` stores all persisted incoming messages. Migrations in `migrations/`. Schema managed by sqlx with auto-run on startup.

### Docker

`compose.yaml` runs `nginx` + `chatbridge` + `postgres:16-alpine` + `redis:7-alpine`. Dockerfile and nginx.conf live in `docker/`. Config via `.env` file (see `.env.example`).

### Specs & Plans

- Design specs: `docs/superpowers/specs/`
- Implementation plans: `docs/superpowers/plans/`
