# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Run

Use the `justfile` — `just test` starts the Postgres and Redis containers and injects
`DATABASE_URL` / `REDIS_URL` itself, so never spell those out by hand.

- **Build:** `just build`
- **Run:** `just run` (requires env vars below)
- **Lint:** `just lint`
- **Format:** `just fmt` (`just fmt-check` to verify without writing)
- **Unit tests (no DB):** `just test-unit`
- **All tests (starts Postgres + Redis, sets env):** `just test`
- **Run single test:** `just test-one <test_name>`
- **Full gate (fmt + lint + test):** `just check`
- **Docker (full stack):** `just up` (`just upd` detached, `just down` to stop)
- `just --list` shows every recipe.

## Gotchas

- `reqwest::Client` is a `LazyLock` static in `provider/instagram.rs` and `provider/telegram.rs` — don't create new clients per-request
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

Multi-provider chat bridge for Instagram, Telegram, and WebSocket chat widgets, built with Axum, Tokio, sqlx, and Redis. Postgres stores channel configuration; Redis handles cross-replica pub/sub for message routing. Migrations run automatically on startup.

### Architecture

- **Lib crate** (`src/lib.rs`) + binary entrypoint (`src/main.rs`)
- **Providers** (`provider/`): `WebhookProvider` trait (verify + parse) — Instagram (HMAC-SHA256) and Telegram (secret token)
- **Pipeline** (`pipeline.rs`): shared message processing — verify sender → resolve chat → persist → publish to Redis. Leaf module: never depends on `handler` or `listener`
- **Handlers** (`handler/`): webhook HTTP handlers, widget/operator WebSocket handlers, REST API
- **Listener** (`listener.rs`): Redis subscriber that dispatches events to WebSocket connections via `ClientRegistry`
- **Cache** (`cache.rs`): in-memory read-through caches (`ChannelCache`, `ClientCache`, `OperatorCache`, `ChatCache`), invalidated via Redis Pub/Sub on `cache_invalidation` topic
- **Config** (`config.rs`): `AppState` does NOT derive `Clone` — always behind `Arc<AppState>`

Key patterns:
- Webhooks return 200 immediately, then process asynchronously via `tokio::spawn`
- Operator→Telegram delivery is also async: Ack fires immediately, then `tokio::spawn` resolves the recipient (cache lookups) and calls `telegram::send`. Failures notify the operator via `ClientRegistry`
- Client resolution (Instagram/Telegram) is async with 24h staleness check — `sender_id` may not exist in `clients` yet
- `ClientCache` is dual-keyed: by UUID and by `(ProviderKind, external_id)`
- `listener.rs`: in read events, `sender` = original message author (not the reader)

### Database

Tables: `channels`, `instagram_channels`, `telegram_channels`, `widget_channels`, `clients`, `operators`, `chats`, `messages`. The `channels` table is the unified parent — provider-specific tables have FK to `channels(id)`. `chats` tracks active conversations per `(client_id, channel_id)` with partial unique index. `messages` stores all persisted incoming messages. Migrations in `migrations/`. Schema managed by sqlx with auto-run on startup.

### Docker

`compose.yaml` runs `nginx` + `chatbridge` + `postgres:16-alpine` + `redis:7-alpine`. Dockerfile and nginx.conf live in `docker/`. Config via `.env` file (see `.env.example`).

### Specs & Plans

- Design specs: `docs/superpowers/specs/`
- Implementation plans: `docs/superpowers/plans/`
- Known gaps & shortcuts: `docs/tech_debt.md` — record debt when you find it, do not defer
