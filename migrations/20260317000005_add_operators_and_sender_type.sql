CREATE TABLE operators (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE messages
    DROP CONSTRAINT messages_sender_id_fkey,
    ADD COLUMN sender_type TEXT NOT NULL DEFAULT 'client',
    ADD CONSTRAINT messages_sender_type_check CHECK (sender_type IN ('client', 'operator'));
