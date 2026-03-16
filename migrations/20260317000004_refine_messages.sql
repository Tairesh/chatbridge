ALTER TABLE messages
    ADD COLUMN status TEXT NOT NULL DEFAULT 'new',
    ADD COLUMN edited_at TIMESTAMPTZ,
    DROP COLUMN event,
    DROP COLUMN provider,
    DROP COLUMN timestamp;

CREATE UNIQUE INDEX idx_messages_channel_external_id
    ON messages (channel_id, external_message_id);
