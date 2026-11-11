-- Calls to a model are logged in the same session as the agent's tool calls, so that one
-- log holds everything the agent saw, in the order it saw it. A model call has no effects
-- in the world, so it is never in doubt and never deduplicated, but it follows the same
-- rules otherwise: the request is logged before it is forwarded, it carries a hash of what
-- was sent, and it gets at most one final answer.
ALTER TABLE events
    DROP CONSTRAINT events_kind_check,
    DROP CONSTRAINT events_shape_check,
    DROP CONSTRAINT events_requests_carry_an_args_hash;

ALTER TABLE events
    ADD CONSTRAINT events_kind_check CHECK (
        kind IN ('call_request', 'call_retry', 'call_response', 'model_request', 'model_response')
    ),
    ADD CONSTRAINT events_shape_check CHECK (
        (kind IN ('call_request', 'model_request') AND request_seq IS NULL AND outcome IS NULL)
        OR (kind = 'call_retry' AND request_seq IS NOT NULL AND request_seq < seq
            AND outcome IS NULL)
        OR (kind IN ('call_response', 'model_response') AND request_seq IS NOT NULL
            AND request_seq < seq AND outcome IS NOT NULL)
    ),
    ADD CONSTRAINT events_requests_carry_an_args_hash
        CHECK ((kind IN ('call_request', 'model_request')) = (args_hash IS NOT NULL)) NOT VALID;

CREATE UNIQUE INDEX events_one_final_model_response_per_request
    ON events (session_id, request_seq)
    WHERE kind = 'model_response' AND outcome NOT IN ('transport_error', 'timeout');
