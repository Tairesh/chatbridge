CREATE TABLE instagram_channels (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    instagram_user_id TEXT NOT NULL UNIQUE,
    user_id TEXT NOT NULL,
    access_token TEXT NOT NULL,
    refresh_time TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT now()
);
