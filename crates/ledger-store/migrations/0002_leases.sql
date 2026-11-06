-- One gateway copy owns a session at a time. Taking a session over bumps `token`, and
-- every append must carry the current token, checked in the statement that writes the
-- event. A copy that was paused while another took over therefore cannot append, however
-- sure it is that it still owns the session. `expires_at` uses the database's clock, the
-- one clock every copy shares.
CREATE TABLE leases (
    session_id TEXT PRIMARY KEY CHECK (session_id <> ''),
    owner      TEXT NOT NULL CHECK (owner <> ''),
    token      BIGINT NOT NULL CHECK (token > 0),
    expires_at TIMESTAMPTZ NOT NULL
);

-- Events recorded before leases existed carry neither column, so the checks apply only
-- to new rows. `args_hash` lets a retry be recognised without comparing whole payloads.
ALTER TABLE events
    ADD COLUMN fence_token BIGINT CHECK (fence_token > 0),
    ADD COLUMN args_hash TEXT CHECK (args_hash ~ '^[0-9a-f]{64}$');

ALTER TABLE events
    ADD CONSTRAINT events_carry_a_fence_token CHECK (fence_token IS NOT NULL) NOT VALID,
    ADD CONSTRAINT events_requests_carry_an_args_hash
        CHECK ((kind = 'call_request') = (args_hash IS NOT NULL)) NOT VALID;
