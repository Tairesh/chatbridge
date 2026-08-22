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
- **Instagram delivers messaging events in two different shapes, and the version decides
  which.** Up to Graph API v25.0 they arrive as `entry[].messaging[]`; from v26.0 the same
  event object arrives as `entry[].changes[].value` with `field` naming the type, and its
  `timestamp` is a **string** instead of a number. `provider/instagram.rs` accepts both.
  Anything that parses only `messaging` sees a silent nothing on v26 — the payload
  deserializes, the handler answers 200, and the event evaporates. The blog post at
  `../www/public/blog/instagram-integration.html` documents the v25 shape only
- The App Dashboard's per-field **Test** button sends a fixed sample payload whose
  `recipient.id` is the dummy `23245`, so it can never match a real channel and never
  reaches the inbox — `no channel found for instagram event` on that id is the correct
  outcome, not a bug. It is also not evidence that live delivery works: the button fires
  whether or not the app is subscribed to the field
- A `changes[]` entry is accepted on the **shape** of its `value` (does it carry `message`,
  `message_edit`, `read` or `reaction`?), not on the `field` name. Comments and the App
  Dashboard's Test button also arrive as changes, and name-gating would either drop real
  events on an unlisted field or invent an inbox entry from a comment
- Instagram Graph responses need three separate checks: an `error` envelope in the body, an
  HTTP status of 400 or more (which can arrive with no envelope), and — on
  `/me/subscribed_apps` — a body that is not `{"success": true}`. A `200 {"success": false}`
  taken as success creates a channel that silently receives nothing
- `subscribed_fields` on `/me/subscribed_apps` **replaces** the account's set rather than
  adding to it — verified on the live API by sending four fields, then two, and reading back
  exactly two. `oauth::subscribe` relies on that: when the full list is rejected it probes
  each field alone to find the bad names, then issues **one final call** with the survivors,
  because stopping after the probe loop would leave only the last field subscribed
- All four fields in `oauth::INSTAGRAM_FIELDS` are accepted by `POST /me/subscribed_apps`,
  echoed back by the GET, and observed firing dashboard test sends. `message_reads` is a
  Facebook Page field and does not exist on the `instagram` object — do not "correct"
  `messaging_seen` to it
- OAuth `state` is a JWT signed with `APP_JWT_SECRET`, separate from `jwt::Claims`. It carries
  the pinned `channel_id` for "Reconnect" and expires in 10 minutes. It is a CSRF guard, not
  an authorization check — the panel has no authorization at all
- The OAuth callback keys its rollback off the upsert's own `(xmax = 0)`, never off a
  `SELECT` taken before it: a concurrent login could otherwise have it hard-delete a live
  channel and its history
- The OAuth callback always answers `200`, even on failure: it is a page for a browser, and
  the popup reads the outcome from its `postMessage` payload
- `refresh::spawn_token_refresher` is called from `main.rs` only, never from `build_state`:
  spawning it in tests would call the real Graph API every hour
- `/api/channels/{id}/connection` answers for every provider, including widget (`ok: true`,
  "registers nothing"). It never returns 400 for "wrong provider" — the panel decides which
  channels get the button
- nginx serves the frontend under `/frontend/`, so the panel is at
  `/frontend/settings.html`; a bare `/settings.html` is proxied to the app and 404s
- Client resolution writes the `clients` row **synchronously** for a sender it has never
  seen, and only the Instagram profile *lookup* (a network call to Meta) is spawned. This is
  load-bearing: `persist_and_publish` refuses to set `sender_id` — and therefore cannot
  create the chat — unless the client row already exists, so deferring that write loses the
  chat for the first message of every new conversation, which is the one message that
  decides whether the conversation shows up at all. `persist_and_publish` still verifies
  existence, because the operator path and the stale-refresh path can both race

## Environment Variables

Required at runtime:
- `INSTAGRAM_APP_ID` — public id of the *Instagram* app (not the Meta app's id from
  Settings → Basic; pasting the Meta value fails with non-obvious authentication errors)
- `INSTAGRAM_VERIFY_TOKEN` — token for Meta/Instagram webhook subscription handshake
- `INSTAGRAM_APP_SECRET` — HMAC-SHA256 secret for Instagram payload signature validation
- `DATABASE_URL` — Postgres connection string (e.g. `postgres://chatbridge:chatbridge@localhost:5432/chatbridge`)
- `REDIS_URL` — Redis connection string (e.g. `redis://localhost:6379`)
- `APP_JWT_SECRET` — HMAC-SHA256 secret for signing/verifying WebSocket JWTs (used for both widget clients and operators)
- `PUBLIC_BASE_URL` — public origin of this deployment, no trailing slash. Used to build Telegram
  webhook URLs for `setWebhook` and the `endpoint` field returned by `/api/channels`
- `INSTAGRAM_API_BASE` — optional. Overrides all three Meta hosts at once
  (`www.instagram.com` for authorize, `api.instagram.com` for the code exchange,
  `graph.instagram.com/v26.0` for everything else). Integration tests do not read it:
  they build `AppConfig` directly and pass a mock's URL, and `build_state` defaults to an
  unreachable address so a test that forgets its mock fails locally
- `TELEGRAM_API_BASE` — optional, defaults to `https://api.telegram.org`. Set it to point a local
  run at a fake Bot API. Integration tests do not read it: they construct `AppConfig` directly and
  pass a mock's URL, and `build_state` defaults to an unreachable address so that a test which
  forgets the mock fails locally instead of calling the real Bot API


## Project Overview

Multi-provider chat bridge for Instagram, Telegram, and WebSocket chat widgets, built with Axum, Tokio, sqlx, and Redis. Postgres stores channel configuration; Redis handles cross-replica pub/sub for message routing. Migrations run automatically on startup.

### Architecture

- **Lib crate** (`src/lib.rs`) + binary entrypoint (`src/main.rs`)
- **Providers** (`provider/`): `WebhookProvider` trait (verify + parse) — Instagram (HMAC-SHA256) and Telegram (secret token)
- **OAuth** (`oauth/`): provider-generic login — `mod.rs` dispatches on `ProviderKind` with a
  `match` (no `dyn` trait), `instagram.rs` holds the Graph API calls. Routes live in
  `handler/oauth.rs`
- **Connection** (`handler/connection.rs`): `GET|POST /api/channels/{id}/connection`, one
  provider-neutral shape for "is this channel wired up, and can I fix it"
- **Refresh** (`refresh.rs`): hourly pass that renews Instagram long-lived tokens three days
  before they expire. `refresh_channel` is split out from the loop so it is testable
- **Pipeline** (`pipeline.rs`): shared message processing — verify sender → resolve chat → persist → publish to Redis. Leaf module: never depends on `handler` or `listener`
- **Handlers** (`handler/`): webhook HTTP handlers, widget/operator WebSocket handlers, REST API, channel CRUD (`handler/channels.rs`)
- **Listener** (`listener.rs`): Redis subscriber that dispatches events to WebSocket connections via `ClientRegistry`
- **Cache** (`cache.rs`): in-memory read-through caches (`ChannelCache`, `ClientCache`, `OperatorCache`, `ChatCache`), invalidated via Redis Pub/Sub on `cache_invalidation` topic
- **Config** (`config.rs`): `AppState` does NOT derive `Clone` — always behind `Arc<AppState>`

Key patterns:
- Webhooks return 200 immediately, then process asynchronously via `tokio::spawn`
- Operator→Telegram and Operator→Instagram delivery are both async: Ack fires immediately,
  then `tokio::spawn` resolves the recipient (cache lookups) and calls the provider. Failures
  notify the operator via `ClientRegistry`. For Instagram the recipient is the client's
  `external_id`, which is the IGSID — the same value that arrived as `sender.id` inbound;
  there is no PSID lookup on this flow
- Instagram send errors are classified by **subcode**, not code, in
  `operator_ws::describe_instagram_failure`: `10` alone is just "permission denied" and `190`
  alone is "bad token", neither of which tells an operator whether to wait, reconnect or give
  up. `2534022` is the closed 24-hour window; `2534014` is a recipient this app has never
  seen. Anything unrecognised is passed through verbatim rather than swallowed
- Client resolution (Instagram/Telegram) is async with 24h staleness check — `sender_id` may not exist in `clients` yet
- `ClientCache` is dual-keyed: by UUID and by `(ProviderKind, external_id)`
- `listener.rs`: in read events, `sender` = original message author (not the reader)
- An outbound message is stored under a local id (`operator:<mid>`) and then **renamed** to
  the provider's id once the send returns (`db::rename_external_message_id`). Read receipts
  and edits arrive keyed by the provider's id and `mark_messages_read` anchors on
  `(channel_id, external_message_id)`, so without the rename every receipt for an operator's
  reply resolves to nothing. Instagram does this; Telegram does not yet — its `send` discards
  the returned id

### Database

Tables: `channels`, `clients`, `operators`, `chats`, `messages`. `channels` is the single channel table — `provider`, `name`, `external_key`, `config` (JSONB), `deleted_at` — with `UNIQUE (provider, external_key)`. `chats` tracks active conversations per `(client_id, channel_id)` with partial unique index. `messages` stores all persisted incoming messages. Migrations in `migrations/`. Schema managed by sqlx with auto-run on startup.

### Docker

`compose.yaml` runs `nginx` + `chatbridge` + `postgres:16-alpine` + `redis:7-alpine`. Dockerfile and nginx.conf live in `docker/`. Config via `.env` file (see `.env.example`).

### Specs & Plans

- Design specs: `docs/superpowers/specs/`
- Implementation plans: `docs/superpowers/plans/`
- Known gaps & shortcuts: `docs/tech_debt.md` — record debt when you find it, do not defer
