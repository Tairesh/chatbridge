-- Collapse instagram_channels / telegram_channels / widget_channels into `channels`.
-- `external_key` is the provider's non-secret identity for the channel.
-- `config` holds provider-specific settings, secrets included. It carries no
-- provider tag: the `provider` column is the single source of truth.

ALTER TABLE channels
    ADD COLUMN name         TEXT,
    ADD COLUMN external_key TEXT,
    ADD COLUMN config       JSONB NOT NULL DEFAULT '{}'::jsonb,
    ADD COLUMN deleted_at   TIMESTAMPTZ;

UPDATE channels c
SET external_key = w.widget_id,
    name         = w.widget_id,
    config       = '{}'::jsonb
FROM widget_channels w
WHERE w.id = c.id;

-- The bot id is the numeric prefix of the token. This is the ONLY place the
-- token is parsed: at runtime the bot id always comes from getMe, which is
-- unavailable inside a migration.
UPDATE channels c
SET external_key = split_part(t.bot_token, ':', 1),
    name         = 'telegram:' || split_part(t.bot_token, ':', 1),
    config       = jsonb_build_object('bot_token', t.bot_token,
                                      'bot_secret', t.bot_secret)
FROM telegram_channels t
WHERE t.id = c.id;

UPDATE channels c
SET external_key = i.user_id,
    name         = 'instagram:' || i.user_id,
    config       = jsonb_strip_nulls(jsonb_build_object('access_token', i.access_token,
                                                        'refresh_time', i.refresh_time))
FROM instagram_channels i
WHERE i.id = c.id;

-- A `channels` row with no provider row cannot route traffic, but it may still be
-- referenced by chats/messages, so give it a key instead of deleting it.
UPDATE channels
SET external_key = id::text,
    name         = provider || ':' || id::text
WHERE external_key IS NULL;

ALTER TABLE channels
    ALTER COLUMN name         SET NOT NULL,
    ALTER COLUMN external_key SET NOT NULL,
    ALTER COLUMN config       DROP DEFAULT;

-- No deleted_at filter: one channel per (provider, identity) forever, so that a
-- delete-then-recreate cannot split a customer's history across two channel ids.
CREATE UNIQUE INDEX idx_channels_provider_external_key
    ON channels (provider, external_key);

DROP TABLE instagram_channels;
DROP TABLE telegram_channels;
DROP TABLE widget_channels;
