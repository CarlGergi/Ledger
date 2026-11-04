-- One row per session. `last_seq` is the sequence allocator: bumping it takes the
-- row lock, which serialises appends within a session and leaves no gaps, because
-- the bump and the event insert commit or roll back together.
CREATE TABLE sessions (
    session_id TEXT PRIMARY KEY CHECK (session_id <> ''),
    last_seq   BIGINT NOT NULL CHECK (last_seq >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- `payload` is JSON rather than JSONB on purpose: JSONB rejects \u0000, which is legal
-- JSON, and the log has to be able to hold whatever a tool actually returned.
CREATE TABLE events (
    session_id  TEXT NOT NULL REFERENCES sessions (session_id),
    seq         BIGINT NOT NULL CHECK (seq > 0),
    kind        TEXT NOT NULL CHECK (kind IN ('call_request', 'call_response')),
    request_seq BIGINT,
    tool        TEXT NOT NULL,
    outcome     TEXT CHECK (
        outcome IN ('ok', 'tool_error', 'rpc_error', 'transport_error', 'timeout', 'not_routed')
    ),
    payload     JSON NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (session_id, seq),
    FOREIGN KEY (session_id, request_seq) REFERENCES events (session_id, seq),
    CHECK (
        (kind = 'call_request' AND request_seq IS NULL AND outcome IS NULL)
        OR (kind = 'call_response' AND request_seq IS NOT NULL AND request_seq < seq
            AND outcome IS NOT NULL)
    )
);

-- A call gets at most one recorded response.
CREATE UNIQUE INDEX events_one_response_per_request
    ON events (session_id, request_seq)
    WHERE request_seq IS NOT NULL;

CREATE FUNCTION events_reject_mutation() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'events is append-only: % is not allowed', TG_OP
        USING ERRCODE = 'integrity_constraint_violation';
END;
$$;

CREATE TRIGGER events_no_update_or_delete
    BEFORE UPDATE OR DELETE ON events
    FOR EACH ROW EXECUTE FUNCTION events_reject_mutation();

CREATE TRIGGER events_no_truncate
    BEFORE TRUNCATE ON events
    FOR EACH STATEMENT EXECUTE FUNCTION events_reject_mutation();
