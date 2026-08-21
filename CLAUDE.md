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
- Channels live in ONE table. `channels.external_key` is the provider's non-secret identity
  (widget → `widget_id`, telegram → numeric bot id, instagram → `user_id`) and `channels.config`
  is a JSONB blob of provider settings including secrets. The blob has no provider tag — read it
  by matching on the `provider` column into `TelegramConfig` / `InstagramConfig` / `WidgetConfig`
- `UNIQUE (provider, external_key)` has no `deleted_at` filter on purpose: one channel per
  identity forever, so a delete-then-recreate cannot split a customer's history across two ids.
  Creating a channel on a deleted identity returns `409 channel_deleted`; the panel restores it
- Channel deletion is soft (`channels.deleted_at`). `db::hard_delete_channel` has exactly one
  legitimate caller: rolling back a create whose `setWebhook` failed
- In `POST /api/channels` for telegram, INSERT comes BEFORE `setWebhook`. Telegram keeps one
  webhook per bot and `setWebhook` overwrites it silently, so a check-then-register order would
  let a duplicate create repoint a live channel's webhook
- **Adding a migration needs a forced rebuild.** `sqlx::migrate!()` embeds migrations at compile
  time via `include_str!`, so a *newly created* file is not in the previous build's dependency
  graph and cargo will not recompile — `just test` then passes while the migration silently never
  runs. `touch src/db.rs` after creating a migration file
- Instagram/Telegram client resolution is async — `sender_id` may not exist in `clients` yet when the message is persisted. `persist_and_publish` verifies client existence before using `sender_id` for FK-constrained inserts

## Environment Variables

Required at runtime:
- `INSTAGRAM_VERIFY_TOKEN` — token for Meta/Instagram webhook subscription handshake
- `INSTAGRAM_APP_SECRET` — HMAC-SHA256 secret for Instagram payload signature validation
- `DATABASE_URL` — Postgres connection string (e.g. `postgres://chatbridge:chatbridge@localhost:5432/chatbridge`)
- `REDIS_URL` — Redis connection string (e.g. `redis://localhost:6379`)
- `APP_JWT_SECRET` — HMAC-SHA256 secret for signing/verifying WebSocket JWTs (used for both widget clients and operators)
- `PUBLIC_BASE_URL` — public origin of this deployment, no trailing slash. Used to build Telegram
  webhook URLs for `setWebhook` and the `endpoint` field returned by `/api/channels`
- `TELEGRAM_API_BASE` — optional, defaults to `https://api.telegram.org`. Set it to point a local
  run at a fake Bot API. Integration tests do not read it: they construct `AppConfig` directly and
  pass a mock's URL, and `build_state` defaults to an unreachable address so that a test which
  forgets the mock fails locally instead of calling the real Bot API


## Project Overview

Multi-provider chat bridge for Instagram, Telegram, and WebSocket chat widgets, built with Axum, Tokio, sqlx, and Redis. Postgres stores channel configuration; Redis handles cross-replica pub/sub for message routing. Migrations run automatically on startup.

### Architecture

- **Lib crate** (`src/lib.rs`) + binary entrypoint (`src/main.rs`)
- **Providers** (`provider/`): `WebhookProvider` trait (verify + parse) — Instagram (HMAC-SHA256) and Telegram (secret token)
- **Pipeline** (`pipeline.rs`): shared message processing — verify sender → resolve chat → persist → publish to Redis. Leaf module: never depends on `handler` or `listener`
- **Handlers** (`handler/`): webhook HTTP handlers, widget/operator WebSocket handlers, REST API, channel CRUD (`handler/channels.rs`)
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

Tables: `channels`, `clients`, `operators`, `chats`, `messages`. `channels` is the single channel table — `provider`, `name`, `external_key`, `config` (JSONB), `deleted_at` — with `UNIQUE (provider, external_key)`. `chats` tracks active conversations per `(client_id, channel_id)` with partial unique index. `messages` stores all persisted incoming messages. Migrations in `migrations/`. Schema managed by sqlx with auto-run on startup.

### Docker

`compose.yaml` runs `nginx` + `chatbridge` + `postgres:16-alpine` + `redis:7-alpine`. Dockerfile and nginx.conf live in `docker/`. Config via `.env` file (see `.env.example`).

### Specs & Plans

- Design specs: `docs/superpowers/specs/`
- Implementation plans: `docs/superpowers/plans/`
- Known gaps & shortcuts: `docs/tech_debt.md` — record debt when you find it, do not defer
