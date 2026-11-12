-- Recognising a retry needs only a few events: the request or retry that names the same
-- step, or the same JSON-RPC id in the same transport session, the call it belongs to, and
-- that call's answers. Indexes find them without reading the whole session, so the cost
-- of a call does not grow with the session's length.
--
-- The keys are columns of their own, written with each request and retry, not expressions
-- over the payload: reading a JSON payload in PostgreSQL fails on \u0000, which the log
-- must be able to hold. The id is kept as compact JSON text, so 5 and "5" stay different.
ALTER TABLE events
    ADD COLUMN ask_step TEXT,
    ADD COLUMN ask_transport TEXT,
    ADD COLUMN ask_rpc_id TEXT;

-- Fill them in for requests and retries already logged. The log refuses updates, so the
-- refusal is lifted for this statement alone, inside this migration's transaction, and it
-- touches only the new columns. A payload holding \u0000 cannot be read here, so such a
-- row keeps no keys: an old call of that kind is not recognised when retried.
--
-- Rows logged before leases have no fence token or arguments hash. The checks for those
-- were added NOT VALID to let such rows stay, but an update checks its new row, so the two
-- checks are dropped for the update and added back exactly as they were.
ALTER TABLE events
    DROP CONSTRAINT events_carry_a_fence_token,
    DROP CONSTRAINT events_requests_carry_an_args_hash;
ALTER TABLE events DISABLE TRIGGER events_no_update_or_delete;
UPDATE events
SET ask_step = payload->>'step',
    ask_transport = payload->>'transport',
    ask_rpc_id = (payload->'rpc_id')::text
WHERE kind IN ('call_request', 'call_retry')
  AND position('\u0000' IN payload::text) = 0;
ALTER TABLE events ENABLE TRIGGER events_no_update_or_delete;
ALTER TABLE events
    ADD CONSTRAINT events_carry_a_fence_token CHECK (fence_token IS NOT NULL) NOT VALID,
    ADD CONSTRAINT events_requests_carry_an_args_hash
        CHECK ((kind IN ('call_request', 'model_request')) = (args_hash IS NOT NULL)) NOT VALID;

CREATE INDEX events_asks_by_step
    ON events (session_id, ask_step, seq)
    WHERE kind IN ('call_request', 'call_retry');

CREATE INDEX events_asks_by_rpc_id
    ON events (session_id, ask_transport, ask_rpc_id, seq)
    WHERE kind IN ('call_request', 'call_retry');

CREATE INDEX events_answers
    ON events (session_id, request_seq)
    WHERE request_seq IS NOT NULL;
