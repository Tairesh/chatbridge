ALTER TABLE instagram_channels DROP COLUMN user_id;
ALTER TABLE instagram_channels RENAME COLUMN instagram_user_id TO user_id;