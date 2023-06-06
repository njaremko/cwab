use super::{Cwab, CwabError, CwabExt};
use crate::cwab::CwabExtInternal;
use crate::job::{Job, JobId};
use crate::prelude::{JobInput, Worker, WorkerExt};
use crate::Config;
use async_trait::async_trait;
use base64::{engine::general_purpose, Engine as _};
use chacha20poly1305::{
    aead::{Aead, AeadCore, OsRng},
    XChaCha20Poly1305,
};
use cron::Schedule;
use redis::Commands;
use std::time::Duration;
use time::OffsetDateTime;

pub struct CwabPro {
    cwab: Cwab,
    #[allow(dead_code)]
    worker: Worker,
}

impl CwabPro {
    #[allow(dead_code)]
    pub fn new(config: &Config) -> Result<CwabPro, CwabError> {
        let cwab = Cwab::new(config)?;
        Ok(CwabPro {
            worker: Worker::new(config, cwab.client.clone())?,
            cwab,
        })
    }

    #[allow(dead_code)]
    pub fn worker(&self) -> impl WorkerExt {
        self.worker.clone()
    }

    async fn handle_uniqueness(
        &self,
        job: &impl Job,
        input: Option<&String>,
        unique_time: Option<OffsetDateTime>,
    ) -> Result<(), CwabError> {
        let mut conn = self.cwab.client.redis_pool.get()?;
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
        let mut conn = self.cwab.client.redis_pool.get()?;

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

impl CwabExtInternal for CwabPro {
    fn encode_input(&self, input: Option<String>) -> Option<JobInput> {
        match &self.cwab.worker.cipher {
            Some(cipher) => input.map(|i| encrypt_job_input(cipher, &i)),
            None => input.map(JobInput::Plaintext),
        }
    }
}

#[async_trait]
impl CwabExt for CwabPro {
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
        self.cwab.perform_in(job, input, duration).await
    }

    async fn perform_at(
        &self,
        job: impl Job,
        input: Option<String>,
        time: time::OffsetDateTime,
    ) -> Result<JobId, CwabError> {
        self.handle_uniqueness(&job, input.as_ref(), Some(time))
            .await?;
        self.cwab.perform_at(job, input, time).await
    }

    async fn perform_async(
        &self,
        job: impl Job,
        input: Option<String>,
    ) -> Result<JobId, CwabError> {
        self.handle_uniqueness(&job, input.as_ref(), None).await?;
        self.cwab.perform_async(job, input).await
    }

    async fn perform_async_bulk(
        &self,
        job: impl Job + Clone,
        inputs: Vec<Option<String>>,
    ) -> Result<Vec<JobId>, CwabError> {
        let uniqueness_filtered_inputs = self.handle_bulk_uniqueness(&job, inputs, None)?;
        self.cwab
            .perform_async_bulk(job, uniqueness_filtered_inputs)
            .await
    }

    async fn perform_sync(
        &self,
        job: impl Job,
        input: Option<String>,
    ) -> Result<Option<String>, CwabError> {
        self.handle_uniqueness(&job, input.as_ref(), None).await?;
        Ok(job.perform(input).await?)
    }
}

#[async_trait]
pub trait CwabProExt: CwabExt {
    async fn perform_periodic(
        &self,
        job: impl Job,
        input: Option<String>,
        cron: &str,
    ) -> Result<JobId, CwabError>;
}

#[async_trait]
impl CwabProExt for CwabPro {
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
        Ok(self.cwab.client.raw_push(&[job_desc]).await?[0])
    }
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
