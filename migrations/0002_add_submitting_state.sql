-- SQLite cannot alter a CHECK constraint in place. Rebuild the small durable
-- jobs table so installations created by the first MVP migration can recover
-- the persisted pre-create state as well.
CREATE TABLE jobs_next (
    id TEXT PRIMARY KEY NOT NULL,
    auth_hash TEXT NOT NULL,
    request_hash TEXT NOT NULL,
    session_key TEXT,
    model TEXT NOT NULL,
    status TEXT NOT NULL CHECK (
        status IN (
            'queued',
            'file_uploaded',
            'submitting',
            'submitted',
            'completed',
            'failed',
            'expired',
            'cancelled'
        )
    ),
    attempt INTEGER NOT NULL DEFAULT 1,
    input_file_id TEXT,
    batch_id TEXT,
    output_file_id TEXT,
    result_json TEXT,
    error_json TEXT,
    delivered_at TEXT,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    UNIQUE(auth_hash, request_hash)
);

INSERT INTO jobs_next (
    id, auth_hash, request_hash, session_key, model, status, attempt,
    input_file_id, batch_id, output_file_id, result_json, error_json,
    delivered_at, created_at, updated_at
)
SELECT
    id, auth_hash, request_hash, session_key, model, status, attempt,
    input_file_id, batch_id, output_file_id, result_json, error_json,
    delivered_at, created_at, updated_at
FROM jobs;

DROP TABLE jobs;
ALTER TABLE jobs_next RENAME TO jobs;
CREATE INDEX jobs_status_idx ON jobs(status);
CREATE INDEX jobs_session_idx ON jobs(auth_hash, session_key);
