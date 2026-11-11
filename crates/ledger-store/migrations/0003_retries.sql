-- A retry is logged as an event of its own, linked to the call it repeats, so the log
-- shows every time an agent asked, while each call keeps a single final answer.
--
-- `in_doubt` is a final answer: the tool may have run, running it again is not safe, and
-- a person should check. `refused` is the gateway turning a call down before forwarding.
-- `transport_error` and `timeout` are not final: they say no answer came back, so a safe
-- retry may still record one.
ALTER TABLE events
    DROP CONSTRAINT events_kind_check,
    DROP CONSTRAINT events_outcome_check,
    DROP CONSTRAINT events_check;

ALTER TABLE events
    ADD CONSTRAINT events_kind_check
        CHECK (kind IN ('call_request', 'call_retry', 'call_response')),
    ADD CONSTRAINT events_outcome_check CHECK (
        outcome IN ('ok', 'tool_error', 'rpc_error', 'transport_error', 'timeout',
                    'not_routed', 'refused', 'in_doubt')
    ),
    ADD CONSTRAINT events_shape_check CHECK (
        (kind = 'call_request' AND request_seq IS NULL AND outcome IS NULL)
        OR (kind = 'call_retry' AND request_seq IS NOT NULL AND request_seq < seq
            AND outcome IS NULL)
        OR (kind = 'call_response' AND request_seq IS NOT NULL AND request_seq < seq
            AND outcome IS NOT NULL)
    );

-- A call gets at most one final answer.
DROP INDEX events_one_response_per_request;
CREATE UNIQUE INDEX events_one_final_response_per_request
    ON events (session_id, request_seq)
    WHERE kind = 'call_response' AND outcome NOT IN ('transport_error', 'timeout');
