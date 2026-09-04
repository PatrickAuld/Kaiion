CREATE TABLE job_requests (
    job_id TEXT PRIMARY KEY NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    provider TEXT NOT NULL,
    auth_hash TEXT NOT NULL,
    idempotency_hash TEXT,
    body_json TEXT NOT NULL CHECK(json_valid(body_json)),
    UNIQUE(auth_hash, idempotency_hash)
);
CREATE INDEX job_requests_owner_idx ON job_requests(provider, auth_hash);
