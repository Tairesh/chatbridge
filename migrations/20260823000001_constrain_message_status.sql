-- `status` has carried string literals with no constraint since it was added, and it
-- now has four values rather than two: new -> delivered -> read, plus the dead end
-- failed. A typo in any of the queries that write it is silent in the worst
-- direction — the read queries match `status IN ('new', 'delivered')`, so a row with
-- a misspelled status can never be marked read and never shows a tick.
ALTER TABLE messages
    ADD CONSTRAINT messages_status_check
    CHECK (status IN ('new', 'delivered', 'read', 'failed'));
