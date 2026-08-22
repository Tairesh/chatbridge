-- `name` was only ever read, never written: `create_operator` was
-- `INSERT INTO operators DEFAULT VALUES`, so every operator was anonymous forever. The
-- panel rendered outbound messages with no author and the widget fell back to the
-- literal string "Operator".
--
-- Backfill from the id — short, stable, and unique enough to tell two operators apart —
-- then make the column mandatory so the next `create_operator` cannot skip it.
UPDATE operators
SET name = 'Operator ' || substr(replace(id::text, '-', ''), 1, 4)
WHERE name IS NULL;

ALTER TABLE operators ALTER COLUMN name SET NOT NULL;
