# Tech Debt

Known shortcuts and gaps, recorded when they are found rather than when they are fixed.
Each entry: what is wrong, why it matters, what a fix looks like.

## REST API has no authorization

**Status:** open (found 2026-08-21)

`GET /api/chats` and `GET /api/chats/{chat_id}` (`src/handler/api.rs`, wired in
`src/routes.rs`) accept any request. There is no token check and no ownership check,
so anyone who knows a chat UUID can read the whole conversation, and anyone at all can
list every active chat together with client names and last-message text.

Both sides of the app already have a JWT: `resolve_client` and `resolve_operator`
(`src/handler/mod.rs`) sign one with `WIDGET_JWT_SECRET`, and the frontends keep it in
localStorage (`chatbridge_token` / `operator_token`). Today it is used only for the
WebSocket handshake — the REST calls in `frontend/operator.html` send nothing.

This gets worse once the widget loads chat history over REST: the chat UUID starts
living in public client-side JS, so it stops being even an accidental secret.

**Fix:** require the JWT on the chat REST endpoints.
- Operator token → may list chats and read any chat.
- Client token → may read only chats where `chats.client_id` matches the token subject;
  anything else returns 404 (not 403, to avoid confirming that the chat exists).
- Distinguishing the two requires knowing whether a JWT subject is an operator or a
  client — either a role claim in the JWT or a lookup against `operators` / `clients`.

## `chats.status` is unenforced and never written

**Status:** open (found 2026-08-21)

`status TEXT NOT NULL DEFAULT 'new'` (`migrations/20260317000002_create_chats.sql`) has no
CHECK constraint and no Rust enum, and no code path ever writes it — every chat stays
`'new'` forever. Two behaviours already depend on the column: the partial unique index
`idx_chats_active (client_id, channel_id) WHERE status = 'new'` (one active chat per
client per channel) and the `WHERE c.status = 'new'` filter in `list_active_chats`.

So chat closing is designed for but not implemented, and nothing prevents a typo'd
status value from silently dropping a chat out of both the index and the operator inbox.

**Fix:** add chat closing (an endpoint or operator WS action that sets `'closed'`), a
`CHECK (status IN ('new', 'closed'))` constraint, and a `ChatStatus` enum in `model.rs`
mirroring `ProviderKind`.

## `get_chat_messages` returns the oldest 100 messages and has no pagination

**Status:** open (found 2026-08-21)

`get_chat_messages` (`src/db.rs:337`) is `ORDER BY created_at ASC LIMIT 100` with no offset,
no cursor, and no total count. Past 100 messages in a chat, the query keeps returning the
same oldest 100 and silently drops everything newer — the exact opposite of what a chat UI
needs. There is no way to ask for the rest.

`frontend/operator.html` already hits this: open a long-running chat and the operator sees
the start of the conversation with no indication that recent messages exist. Widget history
loading (`docs/superpowers/specs/2026-08-21-widget-chat-history-design.md`) inherits the
same behaviour, and there it is worse — the client's own recent messages would be missing
from their own transcript.

**Fix:** invert the query and paginate.
- Fetch the *newest* N (`ORDER BY created_at DESC LIMIT n`), reverse for display.
- Keyset pagination rather than OFFSET: `AND created_at < $cursor` (with `id` as tiebreaker,
  since `created_at` is not unique), returning the next cursor to the client.
- Both frontends then load older messages on scroll-up instead of assuming one request is
  the whole chat.
