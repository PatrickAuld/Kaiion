use std::fmt;

use serde_json::Value;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct JobId(pub String);

impl fmt::Display for JobId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FileId(pub String);

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BatchId(pub String);

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Timestamp(pub String);

#[derive(Clone, Debug, PartialEq)]
pub enum JobState {
    Queued,
    Uploaded {
        input_file_id: FileId,
    },
    Submitting {
        input_file_id: FileId,
        started_at: Timestamp,
    },
    SubmissionUncertain {
        input_file_id: FileId,
        started_at: Timestamp,
    },
    Submitted {
        batch_id: BatchId,
    },
    Terminal(StoredOutcome),
}

#[derive(Clone, Debug, PartialEq)]
pub enum StoredOutcome {
    Completed(Value),
    Failed(Value),
    Incomplete(Value),
    Expired(Value),
    Cancelled(Value),
}

impl JobState {
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Terminal(_))
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct Job {
    pub id: JobId,
    pub model: String,
    pub state: JobState,
}

impl Job {
    pub fn custom_id(&self) -> String {
        format!("kaiion-{}", self.id)
    }
}
