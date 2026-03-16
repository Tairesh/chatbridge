CREATE TABLE messages (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    chat_id UUID REFERENCES chats(id),
    external_message_id TEXT NOT NULL,
    channel_id UUID NOT NULL REFERENCES channels(id),
    sender_id UUID REFERENCES clients(id),
    provider TEXT NOT NULL,
    event TEXT NOT NULL,
    text TEXT,
    timestamp BIGINT NOT NULL,
    raw JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
