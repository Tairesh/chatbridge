-- Create unified channels table
CREATE TABLE channels (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    provider TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Backfill from existing provider tables
INSERT INTO channels (id, provider, created_at)
    SELECT id, 'instagram', COALESCE(created_at, now()) FROM instagram_channels
    UNION ALL
    SELECT id, 'telegram', COALESCE(created_at, now()) FROM telegram_channels
    UNION ALL
    SELECT id, 'widget', created_at FROM widget_channels;

-- Add FK constraints
ALTER TABLE instagram_channels
    ADD CONSTRAINT fk_instagram_channel FOREIGN KEY (id) REFERENCES channels(id);
ALTER TABLE telegram_channels
    ADD CONSTRAINT fk_telegram_channel FOREIGN KEY (id) REFERENCES channels(id);
ALTER TABLE widget_channels
    ADD CONSTRAINT fk_widget_channel FOREIGN KEY (id) REFERENCES channels(id);

-- Drop DEFAULT gen_random_uuid() from provider tables
ALTER TABLE instagram_channels ALTER COLUMN id DROP DEFAULT;
ALTER TABLE telegram_channels ALTER COLUMN id DROP DEFAULT;
ALTER TABLE widget_channels ALTER COLUMN id DROP DEFAULT;

-- Drop created_at from provider tables
ALTER TABLE instagram_channels DROP COLUMN created_at;
ALTER TABLE telegram_channels DROP COLUMN created_at;
ALTER TABLE widget_channels DROP COLUMN created_at;
