# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Run

- **Build:** `cargo build`
- **Run:** `cargo run` (requires env vars below)
- **Lint:** `cargo clippy`
- **Format:** `cargo fmt`
- **Unit tests (no DB):** `cargo test --lib`
- **All tests (needs Postgres):** `DATABASE_URL=postgres://webhook:webhook@localhost:5432/webhook cargo test`
- **Run single test:** `cargo test <test_name>`
- **Docker (full stack):** `docker compose up --build`

## Environment Variables

Required at runtime:
- `META_VERIFY_TOKEN` — token for Meta/Instagram webhook subscription handshake
- `INSTAGRAM_APP_SECRET` — HMAC-SHA256 secret for Instagram payload signature validation
- `DATABASE_URL` — Postgres connection string (e.g. `postgres://webhook:webhook@localhost:5432/webhook`)
- `REDIS_URL` — Redis connection string (e.g. `redis://localhost:6379`)

Optional:
- `PORT` — server listen port (default: 3000)

## Project Overview

Multi-provider webhook microservice for Instagram and Telegram, built with Axum, Tokio, and sqlx. Lib crate (`src/lib.rs`) + binary entrypoint (`src/main.rs`). Postgres stores channel configuration; migrations run automatically on startup.

### Module Structure

- `config.rs` — `AppConfig` (from env vars) and `AppState` (config + PgPool + Redis)
- `db.rs` — Pool init, migrations, channel lookup queries
- `error.rs` — `WebhookError` enum with `IntoResponse`
- `model.rs` — `InternalMessage`, `ProviderKind`, `EventKind`
- `provider/mod.rs` — `WebhookProvider` trait (verify + parse)
- `provider/instagram.rs` — HMAC-SHA256 verification, Meta webhook payload parsing
- `provider/telegram.rs` — Secret token verification, Telegram Update parsing
- `handler.rs` — Axum handlers (`meta_verify`, `instagram_ingest`, `telegram_ingest`, `widget_ws`)
- `routes.rs` — Router assembly

### Routes

| Method | Path | Handler | Notes |
|--------|------|---------|-------|
| GET | `/webhook/instagram` | `meta_verify` | hub.challenge handshake |
| POST | `/webhook/instagram` | `instagram_ingest` | HMAC via `INSTAGRAM_APP_SECRET`, channel lookup by sender/recipient ID |
| POST | `/webhook/telegram/{channel_id}` | `telegram_ingest` | Secret token from DB by channel UUID |
| GET | `/ws/{widget_id}` | `widget_ws` | WebSocket upgrade, validates widget_id against DB, publishes to Redis |

### Handler Flow

1. Extract headers + raw body
2. `provider.verify(headers, body)` → 403 if invalid
3. Return 200 OK immediately
4. `tokio::spawn` → parse payload, lookup channel in DB, log `InternalMessage` to stdout

### Database

Tables: `instagram_channels`, `telegram_channels`, `widget_channels`. Migrations in `migrations/`. Schema managed by sqlx with auto-run on startup.

### Docker

`compose.yaml` runs `nginx` + `webhook` + `postgres:16-alpine` + `redis:7-alpine`. Dockerfile and nginx.conf live in `docker/`. Config via `.env` file (see `.env.example`).
