use anyhow::anyhow;
use async_trait::async_trait;
use dyn_clone::DynClone;
use redis::ToRedisArgs;
use redis::{from_redis_value, FromRedisValue};
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::hash::Hash;
use std::str::FromStr;
use std::time::Duration;
use thiserror::Error;
use time::OffsetDateTime;
use uuid::Uuid;

/// Retry methods
#[derive(Copy, Clone, Debug, Eq, PartialEq, Deserialize, Serialize, Hash)]
pub enum Backoff {
    /// Retry, waiting duration between attempts
    Linear(Duration),

    /// Waits duration before retrying, then waits twice as long, then twice more, etc...
    Exponential(Duration),

    /// We use the same backoff algorithm as Sidekiq: (retry_count^4) + 15 + (rand(10) * (retry_count + 1))
    Default,
}

/// Retry policy for jobs
#[derive(Copy, Clone, Debug, Eq, PartialEq, Deserialize, Serialize, Hash)]
pub enum RetryPolicy {
    /// Keep retrying forever
    Forever { backoff: Backoff },

    /// Retry `x` times
    Count { backoff: Backoff, count: usize },
}

/// The various queues that exist in this system
#[derive(Copy, Clone, Debug, Eq, PartialEq, Deserialize, Serialize, Hash)]
pub enum Queue {
    /// Where scheduled work goes. When it's time to run, we send it to the work queue
    Scheduled,
    /// The work queue, this is what workers pull from, and execute
    Enqueued,
    /// Where failed jobs go. If retries are configured for the job, it'll get retried according to your policy
    Failed,
    /// A list of all jobs currently being worked
    Working,
    /// A list of jobs that are being retried according to their backoff strategy
    Retrying,
    /// A list of successfully processed jobs
    Processed,
    /// A list of dead jobs, that will be reaped in 30 days
    Dead,
}

impl Queue {
    /// Given a namespace, generates the queue identifier
    pub fn namespaced(self, name: &str) -> String {
        format!("{}:{}", self, name)
    }
}

impl Display for Queue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s: &'static str = match self {
            Queue::Scheduled => "scheduled",
            Queue::Enqueued => "enqueued",
            Queue::Failed => "failed",
            Queue::Working => "working",
            Queue::Retrying => "retrying",
            Queue::Processed => "processed",
            Queue::Dead => "dead",
        };
        f.write_str(s)
    }
}

impl FromStr for Queue {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let x: String = s.split(':').take(1).collect();
        match x.as_str() {
            "scheduled" => Ok(Queue::Scheduled),
            "enqueued" => Ok(Queue::Enqueued),
            "failed" => Ok(Queue::Failed),
            "working" => Ok(Queue::Working),
            "retrying" => Ok(Queue::Retrying),
            "processed" => Ok(Queue::Processed),
            "dead" => Ok(Queue::Dead),
            _ => Err(anyhow!("NO!")),
        }
    }
}

/// This is a thin wrapper around `anyhow::Error` to allow us to swap out the underlying error type someday, if needed
#[derive(Error, Debug)]
pub enum JobError {
    /// A string wrapper for panics that occur
    #[error("A panic occurred in the job")]
    PanicError { panic: String },
    /// This is a thin wrapper around `anyhow::Error` to allow us to swap out the underlying error type someday, if needed
    #[error(transparent)]
    AnyError(#[from] anyhow::Error),
}

/// The randomly generated ID for a given instance of a job
#[derive(Serialize, Deserialize, Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub struct JobId(Uuid);

impl Display for JobId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

/// A serializable representation of a job. This is what gets put into the queue.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
pub struct JobDescription {
    /// The format version of the job description. Used to handle breaking changes in the job description format.
    pub version: usize,
    /// The randomly generated globally unique ID for the job
    pub job_id: JobId,
    /// The type of job, used to determine which `Job` implementation to run
    pub job_type: String,
    /// The queue namespace that this job belongs to
    pub namespace: String,
    /// Optional input for the job
    pub input: Option<JobInput>,
    /// A uniqueness key. On the paid runtime, this is used to ensure that duplicate jobs are not scheduled to run simultainiously.
    pub uniqueness_key: Option<String>,
    /// A uniqueness policy. On the paid runtime, this is used to specify if you want uniqueness ensured on this job
    pub uniqueness_policy: Option<UniqunessPolicy>,
    /// A retry policy. Determines how many times the job should be retried, if at all
    pub retry_policy: Option<RetryPolicy>,
    /// Indicates how many times this job has been retried
    pub retry: Option<usize>,
    /// How long we've backed off in previous runs of this job, if applicable
    pub backoff: Option<Duration>,
    /// If this is a scheduled job, indicates when this job is scheduled to run
    pub at: Option<time::OffsetDateTime>,
    /// On the paid runtime, this allows you to specify a cron string to have the job run periodically
    pub period: Option<String>,
    /// When the job was created
    pub created_at: OffsetDateTime,
    /// When the job was added to the work queue
    pub enqueued_at: Option<OffsetDateTime>,
}

impl JobDescription {
    pub(crate) fn enqueue(&self) -> JobDescription {
        JobDescription {
            job_id: JobId(Uuid::new_v4()),
            at: None,
            ..self.clone()
        }
    }
}

impl FromRedisValue for JobDescription {
    fn from_redis_value(v: &redis::Value) -> redis::RedisResult<Self> {
        let v: String = from_redis_value(v)?;
        let parsed = serde_json::from_str(&v).map_err(std::io::Error::from)?;
        Ok(parsed)
    }
}

impl ToRedisArgs for JobDescription {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + redis::RedisWrite,
    {
        let s = serde_json::to_string(self).expect("INVARIANT VIOLATED: Failed to serialize job");
        s.write_redis_args(out)
    }
}

/// Represents input for a job, if present.
/// Allows for representing plaintext, and encrypted payloads.
/// The free implementation does not support encrypted payloads, and will panic if it sees one.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
pub enum JobInput {
    /// A plaintext string input. To get around a lot of typesystem headaches, inputs are stringly typed.
    /// Feel free to pass in a JSON string, and deserialize it in your job.
    Plaintext(String),
    /// An encrypted input. In the paid runtime, this gets decrypted and given to your job as plaintext at runtime.
    EncryptedInput {
        /// A unique nonce
        nonce: String,
        /// The encrypted ciphertext
        ciphertext: String,
    },
}

/// In the paid runtime. A uniqueness policy represents how you want to treat uniqueness for a job.
/// Currently, only duration is implemented as a possible uniqueness policy.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
pub enum UniqunessPolicy {
    /// How long from now until another job of the same type, and same input, can be scheduled to run
    Duration(Duration),
}

/// The description of a job. You'll be implementing a lot of these.
///
/// The only things you _need_ to implement are `name` and `perform`.
#[async_trait]
pub trait Job: Send + Sync + 'static + DynClone + std::fmt::Debug {
    /// The string name of this job. Must be unique amoungst all job types you register on a machine.
    fn name(&self) -> &'static str;

    /// Given input, do some work
    async fn perform(&self, _: Option<String>) -> Result<Option<String>, JobError>;

    /// The queue namespace to run on. Defaults to `"default"`
    fn queue(&self) -> &'static str {
        "default"
    }

    /// Given optional input, generates a unique hash of the job type, queue namespace, and input
    fn uniqueness_key(&self, input: Option<&JobInput>) -> String {
        let mut hasher = blake3::Hasher::new();
        hasher.update(self.queue().as_bytes());
        hasher.update(self.name().as_bytes());
        hasher.update(serde_json::to_string(&input).unwrap().as_bytes());
        hasher.finalize().to_string()
    }

    /// Paid feature. If uniqueness should be respected for this job, allows you to specify how
    fn uniqueness_policy(&self) -> Option<UniqunessPolicy> {
        None
    }

    /// Allows you to specify whether this job should be retries, and if so, how many times
    fn retry_policy(&self) -> Option<RetryPolicy> {
        Some(RetryPolicy::Count {
            backoff: Backoff::Default,
            count: 25,
        })
    }

    /// Allows you to define how long after a job has after it is created to get picked up by a worker.
    /// If a worker picks up a job after it's expired, it will go straight to the morgue (dead queue)
    fn expiry(&self) -> Option<Duration> {
        None
    }

    /// Serializes a job into a persistable representation to put on the queue.
    fn to_job_description(&self, input: Option<JobInput>) -> JobDescription {
        let now = time::OffsetDateTime::now_utc();
        let uniqueness_policy = self.uniqueness_policy();
        JobDescription {
            version: 1,
            job_id: JobId(Uuid::new_v4()),
            job_type: self.name().to_string(),
            retry: None,
            retry_policy: self.retry_policy(),
            backoff: None,
            uniqueness_key: uniqueness_policy
                .as_ref()
                .map(|_| self.uniqueness_key(input.as_ref())),
            uniqueness_policy,
            input,
            namespace: self.queue().to_string(),
            at: None,
            period: None,
            created_at: now,
            enqueued_at: None,
        }
    }
}

#[async_trait]
impl Job for Box<dyn Job> {
    fn name(&self) -> &'static str {
        self.as_ref().name()
    }

    async fn perform(&self, arg: Option<String>) -> Result<Option<String>, JobError> {
        self.as_ref().perform(arg).await
    }
}

dyn_clone::clone_trait_object!(Job);
