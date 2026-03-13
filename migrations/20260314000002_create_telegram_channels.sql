CREATE TABLE telegram_channels (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    bot_token TEXT NOT NULL UNIQUE,
    bot_secret TEXT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT now()
);
