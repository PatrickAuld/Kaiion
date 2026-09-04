CREATE TABLE jobs_typed (
    id TEXT PRIMARY KEY NOT NULL,
    auth_hash TEXT NOT NULL,
    request_hash TEXT NOT NULL,
    model TEXT NOT NULL,
    status TEXT NOT NULL CHECK (
        status IN (
            'queued',
            'uploaded',
            'submitting',
            'submission_uncertain',
            'submitted',
            'completed',
            'failed',
            'incomplete',
            'expired',
            'cancelled'
        )
    ),
    input_file_id TEXT,
    batch_id TEXT,
    submission_started_at TEXT,
    outcome_json TEXT,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    UNIQUE(auth_hash, request_hash),
    CHECK (
        (status = 'queued' AND input_file_id IS NULL AND batch_id IS NULL AND submission_started_at IS NULL AND outcome_json IS NULL)
        OR (status = 'uploaded' AND input_file_id IS NOT NULL AND batch_id IS NULL AND submission_started_at IS NULL AND outcome_json IS NULL)
        OR (status IN ('submitting', 'submission_uncertain') AND input_file_id IS NOT NULL AND batch_id IS NULL AND submission_started_at IS NOT NULL AND outcome_json IS NULL)
        OR (status = 'submitted' AND input_file_id IS NULL AND batch_id IS NOT NULL AND submission_started_at IS NULL AND outcome_json IS NULL)
        OR (status IN ('completed', 'failed', 'incomplete', 'expired', 'cancelled') AND input_file_id IS NULL AND batch_id IS NULL AND submission_started_at IS NULL AND outcome_json IS NOT NULL AND json_valid(outcome_json))
    )
);

INSERT INTO jobs_typed (
    id, auth_hash, request_hash, model, status, input_file_id, batch_id,
    submission_started_at, outcome_json, created_at, updated_at
)
SELECT
    id,
    auth_hash,
    request_hash,
    model,
    CASE status
        WHEN 'file_uploaded' THEN 'uploaded'
        WHEN 'submitting' THEN 'submission_uncertain'
        ELSE status
    END,
    CASE WHEN status IN ('file_uploaded', 'submitting') THEN input_file_id END,
    CASE WHEN status = 'submitted' THEN batch_id END,
    CASE WHEN status = 'submitting' THEN updated_at END,
    CASE
        WHEN status = 'completed' THEN
            CASE WHEN result_json IS NOT NULL AND json_valid(result_json) THEN result_json ELSE json_quote(COALESCE(result_json, 'missing stored response')) END
        WHEN status IN ('failed', 'expired', 'cancelled') THEN
            CASE WHEN error_json IS NOT NULL AND json_valid(error_json) THEN error_json ELSE json_quote(COALESCE(error_json, 'batch request failed')) END
    END,
    created_at,
    updated_at
FROM jobs;

DROP TABLE jobs;
ALTER TABLE jobs_typed RENAME TO jobs;
CREATE INDEX jobs_status_idx ON jobs(status);
