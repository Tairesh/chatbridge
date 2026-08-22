-- `refresh_time` was backfilled from a column of the dead `instagram_channels`
-- table. No code has ever written or read it and its meaning is not recoverable.
-- Its replacement, `token_expires_at`, states a different fact (when the current
-- long-lived token dies), so this is a removal rather than a rename.
UPDATE channels SET config = config - 'refresh_time' WHERE provider = 'instagram';
