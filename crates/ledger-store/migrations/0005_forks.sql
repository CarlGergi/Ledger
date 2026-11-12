-- A fork is a session that starts as a copy of another. Its calls before `at_step` are
-- answered from the source's log, and from there on it runs live, on another model or
-- prompt if one is given. `tools` says what happens to tool calls once it runs live:
-- `safe` answers those the source made from its log and runs only read-only ones,
-- `replay` runs none, and `live` runs them all.
--
-- Creating a fork inserts the session's row as well, so a session that has already
-- recorded anything can never become a fork, and a fork exists before its first event.
CREATE TABLE forks (
    session_id     TEXT PRIMARY KEY REFERENCES sessions (session_id),
    source_session TEXT NOT NULL REFERENCES sessions (session_id),
    at_step        BIGINT NOT NULL CHECK (at_step > 0),
    model          TEXT CHECK (model <> ''),
    system_prompt  TEXT,
    tools          TEXT NOT NULL CHECK (tools IN ('safe', 'replay', 'live')),
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (session_id <> source_session)
);
