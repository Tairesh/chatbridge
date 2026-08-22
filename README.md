# Chatbridge

[![CI](https://github.com/Tairesh/chatbridge/actions/workflows/ci.yml/badge.svg)](https://github.com/Tairesh/chatbridge/actions/workflows/ci.yml)

Multi-provider chat bridge for **Instagram**, **Telegram**, and **WebSocket chat widgets** with **operator dashboard**, built with Rust, Axum, PostgreSQL, and Redis. Supports bidirectional messaging between operators and clients across all providers.

## Event Flow

```
                         ┌──────────────────────────────────────────┐
                         │           Chatbridge Server               │
                         │               :3800                      │
                         │                                          │
  Instagram/Meta ──POST──▶ /webhook/instagram                       │
                         │   │                                      │
                         │   ├─ Verify HMAC-SHA256 signature        │
                         │   │  (X-Hub-Signature-256 header)        │
                         │   │                                      │
                         │   ├─ Return 200 OK ◀── immediate         │
                         │   │                                      │
                         │   └─ Background: parse payload           │
                         │      ├─ Match sender/recipient ID        │
                         │      │  (in-memory cache → DB)           │
                         │      ├─ Resolve client identity          │
                         │      │  (DB lookup + Graph API bg)       │
                         │      ├─ Persist → Postgres messages      │
                         │      └─ Publish ──▶ Redis incoming_messages
                         │                                          │
    Telegram ────POST────▶ /webhook/telegram/{channel_id}           │
                         │   │                                      │
                         │   ├─ Lookup bot_secret by UUID           │
                         │   │  (in-memory cache → DB fallback)     │
                         │   │                                      │
                         │   ├─ Verify secret token header          │
                         │   │  (X-Telegram-Bot-Api-Secret-Token)   │
                         │   │                                      │
                         │   ├─ Return 200 OK ◀── immediate         │
                         │   │                                      │
                         │   └─ Background: parse Update            │
                         │      ├─ Resolve client from `from`       │
                         │      │  (cache → DB, 24h staleness)      │
                         │      ├─ Persist → Postgres messages      │
                         │      └─ Publish ──▶ Redis incoming_messages
                         │                                          │
  Widget Client ───WS────▶ /ws/{widget_id}                          │
                         │   │                                      │
                         │   ├─ Lookup widget_id                    │
                         │   │  (in-memory cache → DB)              │
                         │   ├─ Upgrade to WebSocket                │
                         │   ├─ JWT auth (issue or verify)          │
                         │   ├─ Send chat event if a chat exists    │
                         │   │  {action, chat_id, status}           │
                         │   ├─ Register in ClientRegistry (mpsc)   │
                         │   │                                      │
                         │   └─ Message loop (select!):             │
                         │      ├─ Receive from registry → forward  │
                         │      ├─ 5min idle → ping/pong            │
                         │      ├─ Shutdown → close frame           │
                         │      ├─ Parse JSON action                │
                         │      │  (send / edit / read)             │
                         │      ├─ Persist → Postgres messages      │
                         │      ├─ Publish ──▶ Redis incoming_messages
                         │      └─ Send ACK ──▶ client (5s timeout) │
                         │                                          │
    Operator ──────WS────▶ /ws/operator                             │
                         │   │                                      │
                         │   ├─ JWT auth (issue or verify)          │
                         │   ├─ Register in ClientRegistry (mpsc)   │
                         │   │                                      │
                         │   └─ Message loop (select!):             │
                         │      ├─ Receive from registry → forward  │
                         │      ├─ 5min idle → ping/pong            │
                         │      ├─ Shutdown → close frame           │
                         │      ├─ Parse JSON action                │
                         │      │  {action, chat_id, mid, text}     │
                         │      ├─ Persist → Postgres messages      │
                         │      ├─ Publish ──▶ Redis incoming_messages
                         │      ├─ Send ACK ──▶ operator            │
                         │      └─ Telegram? → spawn delivery       │
                         │         └─ POST Bot API /sendMessage     │
                         │                                          │
    Operator ──────GET───▶ /api/chats                               │
                         │   └─ List active chats with summaries    │
                         │                                          │
    Operator ──────GET───▶ /api/chats/{chat_id}                     │
                         │   └─ Chat message history (also used by  │
                         │      the widget to load its own history) │
                         │                                          │
  Instagram/Meta ──GET───▶ /webhook/instagram                       │
                         │   └─ Subscription verification           │
                         │      (hub.challenge handshake)           │
                         │                                          │
                         │  ┌─ Shared Redis Listener ─────────────┐ │
                         │  │ Subscribe: incoming_messages        │ │
                         │  │ On event:                           │ │
                         │  │  ├─ Resolve chat → client_id        │ │
                         │  │  ├─ Send to client (if not sender)  │ │
                         │  │  └─ Send to operators (skip sender) │ │
                         │  └─────────────────────────────────────┘ │
                         └───────┬─────────────────┬────────────────┘
                                 │                 │
                            ┌────▼────┐      ┌─────▼─────┐
                            │ Postgres │      │   Redis   │
                            │  :5432   │      │   :6379   │
                            └─────────┘      └───────────┘
                         channels             pub/sub channels:
                         clients              incoming_messages
                         operators            cache_invalidation
                         chats
                         clients
                         operators
                         chats
                         messages
```

### Message Lifecycle

Handlers build a `NewMessage`, then branch on `EventKind`:

```
EventKind::Message  → INSERT (dedup) → publish IncomingEvent::Message
EventKind::Edit     → UPDATE text    → publish IncomingEvent::Edit
EventKind::Read     → UPDATE status  → publish IncomingEvent::Read (watermark)
EventKind::Reaction → log only
EventKind::Unknown  → log only
```

Only `Message` events create rows. `Edit` and `Read` mutate existing rows found by
`(channel_id, external_message_id)`.

**Read receipts** are a watermark: the message the receipt names **and every older one**
in that chat are marked read, excluding the reader's own. When the named message is not
found — the receipt overtook the id adoption below, or the message was sent from the
provider's own app — `db::mark_chat_read` falls back to everything the other side has
unread in that customer's active chat.

**External message ids** are built in `src/external_id.rs` and are unique *within a
channel*, because `UNIQUE (channel_id, external_message_id)` spans the channel and a
collision silently drops a message rather than raising. A Telegram `message_id` is unique
only inside its chat, so the id carries the chat; a widget `mid` comes from the browser,
so it carries the client.

**Outbound messages** are stored under a local id (`operator:<mid>`) and adopt the
provider's id once the send returns, keyed on the row's UUID
(`db::adopt_external_message_id`). Without that, every receipt for an operator's reply
would resolve to nothing.

**Status** is a ladder: `new` → `delivered` → `read`, plus the dead end `failed`, held in
a `MessageStatus` enum and enforced by `messages_status_check`. One tick in the panel
means the provider took the message, two mean the customer read it, `✗` means it was
refused. A widget message passes no provider and goes straight from `new` to `read`.

Published to Redis as `IncomingEvent` with a `"type"` discriminator:
```json
{"type": "message", "id": "...", "text": "...", "status": "new", ...}
{"type": "edit", "id": "...", "text": "...", "edited_at": "...", ...}
{"type": "read", "id": "...", ...}
```

Chat resolution: if the sender exists in the `clients` table, `find_or_create_chat` finds or creates an active chat for `(client_id, channel_id)`. At most one active chat per pair (enforced by partial unique index).

### Caching

Channel, client, operator, and chat data are cached in-memory (`ChannelCache`, `ClientCache`, `OperatorCache`, `ChatCache`) to avoid a Postgres round-trip on every incoming webhook/message. All caches use a read-through strategy: on a miss, query the database and store the result locally.

When data is updated, publish an invalidation event to Redis so all replicas evict the stale entry:

```bash
# Invalidate a specific entity
redis-cli PUBLISH cache_invalidation "channel:<uuid>"
redis-cli PUBLISH cache_invalidation "client:<uuid>"
redis-cli PUBLISH cache_invalidation "operator:<uuid>"
redis-cli PUBLISH cache_invalidation "chat:<uuid>"
```

Each replica runs a background listener on the `cache_invalidation` topic that dispatches by entity type. Providers automatically publish client invalidation after every `upsert_client`.

## Project Structure

```
src/
├── main.rs              # Entrypoint: load config, connect DB, start server, spawn shared listener, graceful shutdown
├── lib.rs               # Public module re-exports
├── cache.rs             # In-memory caches (channel, client, operator, chat) with Redis Pub/Sub invalidation; ClientCache dual-keyed by UUID + (provider, external_id)
├── config.rs            # AppConfig (env vars) + AppState (config + DB pool + Redis + caches + registry + shutdown token)
├── db.rs                # Postgres pool, migrations, single-table channel queries + CRUD, client/operator/chat queries, message persistence
├── error.rs             # AppError → HTTP status mapping
├── jwt.rs               # HS256 JWT sign/verify for WebSocket identity (widget clients + operators)
├── registry.rs          # ClientRegistry (tracks active WS connections per client/operator UUID via mpsc channels)
├── model.rs             # Sender, NewMessage, Conversation, MessageStatus, IncomingMessage/Edit/Read, ProviderKind, EventKind, WsInbound, OperatorInbound, WsOutbound
├── external_id.rs       # The one place that builds messages.external_message_id, per provider
├── refresh.rs           # Hourly pass renewing Instagram long-lived tokens before they expire
├── pipeline.rs          # Shared message processing pipeline (persist_and_publish, resolve_sender, publish_event)
├── listener.rs          # Shared Redis listener (spawn_message_listener) — dispatches events to WS connections
├── handler/
│   ├── mod.rs           # Shared handler utilities (ConnectionGuard, send_outbound, resolve_client/operator, WS constants)
│   ├── webhook.rs       # HTTP webhook handlers (meta_verify, instagram_ingest, telegram_ingest)
│   ├── widget_ws.rs     # Widget WebSocket handler
│   ├── operator_ws.rs   # Operator WebSocket handler (async Telegram delivery via Bot API)
│   ├── api.rs           # REST API handlers (get_chats, get_chat_messages)
│   ├── channels.rs      # Channel CRUD for the settings panel (list/create/update/delete)
│   ├── connection.rs    # GET|POST /api/channels/{id}/connection — provider-neutral "is this wired up"
│   └── oauth.rs         # OAuth routes: provider list, start (307), popup callback
├── routes.rs            # Router assembly
├── oauth/
│   ├── mod.rs           # Provider-generic login core: descriptors, redirect URIs, state JWT
│   └── instagram.rs     # Graph API calls: code exchange, long-lived token, profile, subscribe, send, mark_seen
└── provider/
    ├── mod.rs           # WebhookProvider trait (verify + parse)
    ├── instagram.rs     # HMAC-SHA256 verification, both webhook shapes, echoes, client identity via Graph API
    └── telegram.rs      # Secret token verification, Telegram Update parsing, client resolution, Bot API calls (sendMessage, getMe, setWebhook, deleteWebhook, getWebhookInfo)

docker/
├── Dockerfile           # Multi-stage build
└── nginx.conf           # Nginx reverse proxy config

frontend/
├── widget.html          # Chat widget test page (WebSocket client)
├── operator.html        # Operator dashboard (WebSocket + REST API)
├── settings.html        # Channel settings panel (CRUD, Instagram login popup, connection status)
└── style.css            # Shared stylesheet for the panel pages

migrations/              # SQL migrations (auto-run on startup)
tests/
├── common/mod.rs        # Shared with both test binaries (RAII cleanup guards, pool setup)
├── client_identity.rs   # Client identity upsert tests
└── integration/         # One test binary, split by subject
    ├── main.rs          # Module declarations
    ├── support/         # State builders, fixtures, HTTP/WS helpers, Graph and Bot API mocks
    ├── webhooks.rs      widget_ws.rs      operator_ws.rs    read_receipts.rs
    ├── messages.rs      cache.rs          channels_db.rs    channels_api.rs
    └── oauth.rs         instagram_flow.rs refresh.rs        connection.rs
compose.yaml             # nginx + chatbridge + postgres + redis services
```

## Quick Start

### With Docker (recommended)

```bash
cp .env.example .env
# Edit .env with your tokens

docker compose up --build
```

Nginx listens on `http://localhost:80` and proxies to the chatbridge server (including WebSocket upgrade for `/ws/`), with Postgres and Redis provisioned automatically.

### Without Docker

Requires Rust 1.88+, a running PostgreSQL instance, and Redis.

```bash
# Set environment variables
export INSTAGRAM_APP_ID=your_instagram_app_id
export INSTAGRAM_VERIFY_TOKEN=your_token
export INSTAGRAM_APP_SECRET=your_secret
export DATABASE_URL=postgres://chatbridge:chatbridge@localhost:5432/chatbridge
export REDIS_URL=redis://localhost:6379
export APP_JWT_SECRET=your-jwt-secret-here-at-least-32-bytes
export PUBLIC_BASE_URL=https://your-public-host

# Build and run
cargo build
cargo run
```

Migrations run automatically on startup.

## Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `INSTAGRAM_APP_ID` | yes | — | Public id of the **Instagram** app (App dashboard → Use cases → Manage messaging & content on Instagram). Not the Meta app id from Settings → Basic |
| `INSTAGRAM_VERIFY_TOKEN` | yes | — | Token for Instagram webhook subscription handshake |
| `INSTAGRAM_APP_SECRET` | yes | — | HMAC-SHA256 secret for Instagram signature validation, and for the OAuth token exchanges |
| `DATABASE_URL` | yes | — | Postgres connection string |
| `REDIS_URL` | yes | — | Redis connection string |
| `APP_JWT_SECRET` | yes | — | HMAC-SHA256 secret for WebSocket JWTs (widget clients + operators) |
| `PUBLIC_BASE_URL` | yes | — | Public origin of this deployment, no trailing slash. Builds Telegram webhook URLs and the `endpoint` field of a channel |
| `TELEGRAM_API_BASE` | no | `https://api.telegram.org` | Telegram Bot API origin. Point it at a fake Bot API for local work |
| `RUST_LOG` | no | `info` | Log level. `chatbridge=debug` logs raw webhook payloads and every provider call |
| `INSTAGRAM_API_BASE` | no | — | Overrides all three Meta hosts at once (`www.instagram.com`, `api.instagram.com`, `graph.instagram.com/v26.0`). For pointing a local run at a fake API |

## Connecting an Instagram account

Register `<PUBLIC_BASE_URL>/api/oauth/instagram/callback` as a redirect URI in the Meta
dashboard (*Use cases → Manage messaging & content on Instagram → Set up Instagram business
login → Business Login Settings*). Meta compares it byte for byte; the settings panel shows
the exact string with a copy button.

`PUBLIC_BASE_URL` must be **https** — Meta refuses a plain-http redirect URI — and the panel
has to be opened at that same origin, because the popup lands there.

Then press **Log in with Instagram** in the panel. The popup returns with the account
connected, subscribed, and its 60-day token recorded. A background pass refreshes tokens
hourly, starting three days before they expire.

Until App Review grants advanced access to `instagram_business_basic` and
`instagram_business_manage_messages`, the login works only for Instagram accounts that hold a
role on the app.

Outbound replies are text-only: no attachments, and no enforcement of Instagram's 24-hour
customer-service window (Meta rejects a late reply and the operator is told why). See
`docs/tech_debt.md`.

## Testing

### Unit tests (no database needed)

```bash
just test-unit
```

Covers HMAC verification, secret token validation, event classification, payload deserialization, WebSocket message types, UUID mid validation, TelegramUser/display name building, and the chat-history WebSocket event.

### All tests (unit + integration)

Requires Docker; `just test` starts Postgres and Redis and injects `DATABASE_URL` / `REDIS_URL`:

```bash
just test
```

Integration tests cover:
- Meta webhook subscription verification (valid/invalid token, wrong mode)
- Instagram POST ingestion (valid/invalid/missing signature, channel lookup, non-instagram object rejection)
- Telegram POST ingestion (valid/invalid secret, unknown channel → 404)
- WebSocket widget (connect, ACK, multiple messages, error recovery, unknown widget, invalid mid rejection)
- WebSocket edit action (edit ACK, Redis edit event, unknown action error)
- Operator WebSocket (connect, send message to widget client, edit reaches widget client)
- Read receipt forwarding (widget read → operator, operator read → widget)
- Operator REST API (list chats, chat messages, unknown chat → 404)
- Widget chat history (`find_last_chat` returns a closed chat, the newest of several, or None; `sender_name` resolution; chat event on reconnect and its absence for new clients)
- Redis pub/sub verification for all three providers
- Client identity upsert (create, update, conflict handling)
- Telegram client reuse (same client_id across messages from same user)
- Channel cache (read-through, per-channel invalidation, Redis Pub/Sub eviction, cross-channel isolation)
- Client cache invalidation via Redis Pub/Sub
- Channel CRUD (create widget/instagram/telegram, 409 on a live and on a deleted identity,
  `setWebhook` rollback, provider change rejected, token rotation vs different bot, soft delete
  idempotency, deleted channel invisible to the hot path and absent from the operator inbox)
- Connection status (match, hijacked URL, delivery errors, re-register, widget and deleted
  channels)
- Instagram login (authorize redirect and signed state, callback creating/restoring/refusing a
  channel, rollback when subscribing fails)
- External message ids (two Telegram clients both reaching `message_id: 1`, two widget clients
  sharing a `mid`)
- Id adoption (plain, over an echo that arrived first, on a deleted row, on a row that already
  has it)
- Read receipts (anchor and everything older, never the reader's own, never another chat, the
  chat-level fallback, an unknown mid end to end, `mark_seen` reaching Instagram and not
  reaching a widget)
- Echoes (a message sent from the Instagram app appears in history; an echo of our own reply
  adds nothing)
- Delivery outcomes (`delivered` status and notification, a refused reply marked `failed` under
  its local id, a status outside the ladder refused by the database)
- Token refresh (renewal, missing expiry, a rejected refresh leaving the token alone)

Test data cleanup uses RAII drop guards (`TestChannel`, `TestChannelKey`, `TestClient`,
`TestChat`, `TestMessage`, `TestOperator`) in `tests/common/` to ensure rows are deleted even if
a test panics. `TestChannel` cascades: it deletes the channel's messages and chats and then every
client they referenced.

### Linting

```bash
just lint
just fmt-check
```

`just check` runs fmt, clippy, and the full suite in one go.
