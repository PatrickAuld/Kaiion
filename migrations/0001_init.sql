CREATE TABLE IF NOT EXISTS jobs (
    id TEXT PRIMARY KEY NOT NULL,
    auth_hash TEXT NOT NULL,
    request_hash TEXT NOT NULL,
    session_key TEXT,
    model TEXT NOT NULL,
    status TEXT NOT NULL CHECK (
        status IN (
            'queued',
            'file_uploaded',
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

CREATE INDEX IF NOT EXISTS jobs_status_idx ON jobs(status);
CREATE INDEX IF NOT EXISTS jobs_session_idx ON jobs(auth_hash, session_key);

