use crate::client::{CwabClient, CwabClientError};
use crate::job::{Job, JobError, JobId};
use crate::prelude::{JobInput, WorkerState};
use crate::worker::WorkerExt;
use crate::Config;
use async_trait::async_trait;
use base64::{engine::general_purpose, Engine as _};
use chacha20poly1305::{
    aead::{Aead, AeadCore, OsRng},
    XChaCha20Poly1305,
};
use cron::Schedule;
use dyn_clone::DynClone;
use itertools::Itertools;
use r2d2::Pool;
use redis::Commands;
use std::str::Utf8Error;
use std::time::Duration;
use thiserror::Error;
use time::OffsetDateTime;

/// This is the main interface through which you interact with Cwab.
/// It generates instances of the `WorkerExt` trait, and facilitates
/// dispatching of work to the various queues.
#[derive(Clone)]
pub struct Cwab {
    #[allow(dead_code)]
    config: Config,
    client: CwabClient,
    worker: WorkerState,
    middleware: Vec<Box<dyn ClientMiddleware>>,
}

impl Cwab {
    /// Creates a new instance of Cwab
    pub fn new(config: &Config) -> Result<Cwab, CwabError> {
        let redis_pool: Pool<redis::Client> = establish(config)?;
        let client = CwabClient::new(redis_pool);

        let mut config = config.clone();
        let namespaces: Vec<String> = config
            .namespaces
            .as_ref()
            .map(|x| x.iter().unique().cloned().collect())
            .unwrap_or_else(|| vec!["default".to_string()]);
        config.namespaces = Some(namespaces);

        Ok(Cwab {
            worker: WorkerState::new(&config, client.clone())?,
            middleware: vec![],
            config,
            client,
        })
    }

    /// Generates a worker, these are cheap, and share underlying resources
    pub fn worker(&self) -> impl WorkerExt {
        self.worker.clone()
    }

    /// Register middleware applied to all jobs
    pub fn register_middleware(&mut self, middleware: impl ClientMiddleware + 'static) {
        self.middleware.push(Box::new(middleware));
    }

    async fn handle_uniqueness(
        &self,
        job: &impl Job,
        input: Option<&String>,
        unique_time: Option<OffsetDateTime>,
    ) -> Result<(), CwabError> {
        let mut conn = self.client.redis_pool.get()?;
        let encoded = self.encode_input(input.cloned());
        let unique_key = job.uniqueness_key(encoded.as_ref());

        let ttl = conn.ttl::<_, i64>(&unique_key)?;
        let unexpired_key_exists = match ttl {
            -2 => false,
            -1 => false,
            expires_in => {
                let now = OffsetDateTime::now_utc();
                let expires_at = now + Duration::from_secs(expires_in.try_into().unwrap());
                unique_time.unwrap_or(now) < expires_at
            }
        };

        if unexpired_key_exists {
            return Err(CwabError::UniqenessViolation("BAD!".to_string()));
        }
        Ok(())
    }

    fn handle_bulk_uniqueness(
        &self,
        job: &impl Job,
        inputs: Vec<Option<String>>,
        unique_time: Option<OffsetDateTime>,
    ) -> Result<Vec<Option<String>>, CwabError> {
        let mut conn = self.client.redis_pool.get()?;

        Ok(inputs
            .into_iter()
            .filter(|input| {
                let encoded = self.encode_input(input.clone());
                let unique_key = job.uniqueness_key(encoded.as_ref());
                !conn
                    .ttl::<_, Option<usize>>(&unique_key)
                    .ok()
                    .flatten()
                    .map(|t| OffsetDateTime::from_unix_timestamp(t.try_into().unwrap()).unwrap())
                    .map(|t| unique_time.unwrap_or_else(OffsetDateTime::now_utc) < t)
                    .unwrap_or(false)
            })
            .collect())
    }
}

/// Middleware that transforms a job and input into another job and input
#[async_trait]
pub trait ClientMiddleware: Send + Sync + DynClone {
    /// Transform a given job and input into another job and input
    async fn transform(
        &self,
        from: Box<dyn Job>,
        input: Option<String>,
    ) -> Result<(Box<dyn Job>, Option<String>), CwabError>;
}

#[async_trait]
impl ClientMiddleware for Box<dyn ClientMiddleware> {
    async fn transform(
        &self,
        from: Box<dyn Job>,
        input: Option<String>,
    ) -> Result<(Box<dyn Job>, Option<String>), CwabError> {
        self.as_ref().transform(from, input).await
    }
}

dyn_clone::clone_trait_object!(ClientMiddleware);

/// The trait that implements putting jobs onto the various queues
#[async_trait]
pub trait CwabExt {
    /// Schedules a job with optional input to run at some duration from now in the future
    async fn perform_in(
        &self,
        job: impl Job,
        input: Option<String>,
        duration: Duration,
    ) -> Result<JobId, CwabError>;

    /// Schedules a job with optional input to run at a specific date and time
    async fn perform_at(
        &self,
        job: impl Job,
        input: Option<String>,
        time: time::OffsetDateTime,
    ) -> Result<JobId, CwabError>;

    /// Puts a job with optional input onto the queue immediately
    async fn perform_async(&self, job: impl Job, input: Option<String>)
        -> Result<JobId, CwabError>;

    /// Puts an arbitrary amount of inputs for one job onto the queue
    async fn perform_async_bulk(
        &self,
        job: impl Job + Clone,
        inputs: Vec<Option<String>>,
    ) -> Result<Vec<JobId>, CwabError>;

    /// Performs a job synchronously. Does not queue. Be careful with this.
    async fn perform_sync(
        &self,
        job: impl Job,
        input: Option<String>,
    ) -> Result<Option<String>, CwabError>;

    async fn perform_periodic(
        &self,
        job: impl Job,
        input: Option<String>,
        cron: &str,
    ) -> Result<JobId, CwabError>;
}

trait CwabExtInternal {
    fn encode_input(&self, input: Option<String>) -> Option<JobInput>;
}

impl CwabExtInternal for Cwab {
    fn encode_input(&self, input: Option<String>) -> Option<JobInput> {
        match &self.worker.cipher() {
            Some(cipher) => input.map(|i| encrypt_job_input(cipher, &i)),
            None => input.map(JobInput::Plaintext),
        }
    }
}

async fn apply_client_middleware(
    middleware: &[Box<dyn ClientMiddleware>],
    job: impl Job,
    input: Option<String>,
) -> Result<(impl Job, Option<String>), CwabError> {
    let (mut job, mut input): (Box<dyn Job>, Option<String>) = (Box::new(job), input);
    for m in middleware.iter() {
        let (x, y) = m.transform(job, input).await?;
        job = x;
        input = y;
    }
    Ok((job, input))
}

#[async_trait]
impl CwabExt for Cwab {
    async fn perform_in(
        &self,
        job: impl Job,
        input: Option<String>,
        duration: Duration,
    ) -> Result<JobId, CwabError> {
        self.handle_uniqueness(
            &job,
            input.as_ref(),
            Some(OffsetDateTime::now_utc() + duration),
        )
        .await?;
        let (job, input) = apply_client_middleware(&self.middleware, job, input).await?;
        self.perform_at(job, input, time::OffsetDateTime::now_utc() + duration)
            .await
    }

    async fn perform_at(
        &self,
        job: impl Job,
        input: Option<String>,
        time: time::OffsetDateTime,
    ) -> Result<JobId, CwabError> {
        self.handle_uniqueness(&job, input.as_ref(), Some(time))
            .await?;
        let (job, input) = apply_client_middleware(&self.middleware, job, input).await?;
        let mut job_desc = job.to_job_description(self.encode_input(input));
        job_desc.at = Some(time);
        Ok(self.client.raw_push(&[job_desc]).await?[0])
    }

    async fn perform_async(
        &self,
        job: impl Job,
        input: Option<String>,
    ) -> Result<JobId, CwabError> {
        self.handle_uniqueness(&job, input.as_ref(), None).await?;
        let (job, input) = apply_client_middleware(&self.middleware, job, input).await?;
        let job_desc = job.to_job_description(self.encode_input(input));
        Ok(self.client.raw_push(&[job_desc]).await?[0])
    }

    async fn perform_async_bulk(
        &self,
        job: impl Job + Clone,
        inputs: Vec<Option<String>>,
    ) -> Result<Vec<JobId>, CwabError> {
        let uniqueness_filtered_inputs = self.handle_bulk_uniqueness(&job, inputs, None)?;
        let mut results = vec![];
        for slice in uniqueness_filtered_inputs.windows(1000) {
            let mut window = vec![];
            for input in slice {
                let (job, input) =
                    apply_client_middleware(&self.middleware, job.clone(), input.clone()).await?;
                window.push(job.to_job_description(self.encode_input(input)));
            }
            results.append(&mut self.client.raw_push(&window).await?);
        }

        Ok(results)
    }

    async fn perform_sync(
        &self,
        job: impl Job,
        input: Option<String>,
    ) -> Result<Option<String>, CwabError> {
        self.handle_uniqueness(&job, input.as_ref(), None).await?;
        let (job, input) = apply_client_middleware(&self.middleware, job, input).await?;
        Ok(job.perform(input).await?)
    }

    async fn perform_periodic(
        &self,
        job: impl Job,
        input: Option<String>,
        cron: &str,
    ) -> Result<JobId, CwabError> {
        let mut job_desc = job.to_job_description(self.encode_input(input));

        // Get rid of "second" level in cron
        let split_cron: Vec<_> = cron.split_whitespace().collect();
        let schedule: Schedule = if split_cron.len() == 5 {
            format!("0 {cron}").parse()?
        } else {
            format!("0 {}", split_cron[1..].join(" ")).parse()?
        };

        job_desc.period = Some(schedule.to_string());
        job_desc.at = Some(next_time(&schedule));
        Ok(self.client.raw_push(&[job_desc]).await?[0])
    }
}

fn establish(config: &Config) -> Result<Pool<redis::Client>, anyhow::Error> {
    let client = redis::Client::open(&*config.redis_url)?;
    println!("Connecting to redis...");
    let pool = r2d2::Pool::builder().max_size(32).build(client)?;
    println!("Connected to redis!");
    Ok(pool)
}

/// The various errors that can be thrown by this library
#[derive(Error, Debug)]
pub enum CwabError {
    /// An error occurred while running a job
    #[error("An error occurred while running a job")]
    JobFailure(#[from] JobError),
    /// There was an error in the CwabClient
    #[error("There was an error in the CwabClient")]
    ClientError(#[from] CwabClientError),
    /// An IO error occurred
    #[error("An IO error occurred")]
    IoError(#[from] std::io::Error),
    /// A UTF8 error occurred
    #[error("A UTF8 error occurred")]
    Utf8Error(#[from] Utf8Error),
    /// A toml (de)serialization error occurred
    #[error("A toml (de)serialization error occurred")]
    TomlError(#[from] toml::de::Error),
    ///There was an error while interacting with the redis pool
    #[error("There was an error while interacting with the redis pool")]
    PoolError(#[from] r2d2::Error),
    /// Redis threw an error
    #[error("Redis threw an error")]
    RedisError(#[from] redis::RedisError),
    /// There was an error while (de)serializing JSON
    #[error("There was an error while (de)serializing JSON")]
    JsonError(#[from] serde_json::Error),
    /// Tried to schedule a job that would violate a uniqueness constraint
    #[error("Tried to schedule a job that would violate a uniqueness constraint")]
    UniqenessViolation(String),
    /// An error occurred while parsing a cron string
    #[error("An error occurred while parsing a cron string")]
    CronError(#[from] cron::error::Error),
    /// An error occurred when doing math on a time range
    #[error("An error occurred when doing math on a time range")]
    TimeError(#[from] time::error::ComponentRange),
    /// An unexpected error occurred
    #[error(transparent)]
    Unknown(#[from] anyhow::Error),
}

fn encrypt_job_input(cipher: &XChaCha20Poly1305, input: &str) -> JobInput {
    let nonce = XChaCha20Poly1305::generate_nonce(&mut OsRng); // 96-bits; unique per message
    let ciphertext = cipher
        .encrypt(&nonce, input.as_bytes())
        .expect("Failed to encrypted input");
    JobInput::EncryptedInput {
        nonce: general_purpose::STANDARD.encode(nonce),
        ciphertext: general_purpose::STANDARD.encode(ciphertext),
    }
}

pub(crate) fn next_time(s: &Schedule) -> time::OffsetDateTime {
    s.upcoming(chrono::offset::Utc)
        .take(1)
        .collect::<Vec<chrono::DateTime<_>>>()
        .first()
        .map(|x| time::OffsetDateTime::from_unix_timestamp(x.timestamp()))
        .expect("INVARIANT")
        .expect("INVARIANT")
}
