use crate::{
    job::{JobDescription, JobId, Queue, RetryPolicy},
    MAX_WAIT,
};
use redis::Commands;
use std::{num::TryFromIntError, time::Duration};
use thiserror::Error;
use time::OffsetDateTime;

/// The errors that can happen when the `CwabClient` is doing work
#[derive(Error, Debug)]
pub enum CwabClientError {
    /// An error when interacting with the pool
    #[error("Error occurred with the redis pool")]
    PoolError(#[from] r2d2::Error),
    /// An error occurred when interacting with redis
    #[error("Encountered a redis error")]
    RedisError(#[from] redis::RedisError),
    /// An error serializing/deserializing input
    #[error("An error occurred while (de)serializing job input")]
    JsonError(#[from] serde_json::Error),
    /// An error coalesing an int type
    #[error("Ran into an error coalesing an integer type")]
    IntCoaleseError(#[from] TryFromIntError),
    /// An unexpected error occurred
    #[error(transparent)]
    Unknown(#[from] anyhow::Error),
}

#[derive(Clone, Debug)]
pub struct CwabClient {
    pub(crate) redis_pool: r2d2::Pool<redis::Client>,
}

impl CwabClient {
    pub fn new(redis_pool: r2d2::Pool<redis::Client>) -> Self {
        CwabClient { redis_pool }
    }

    pub(crate) async fn raw_push(
        &self,
        jobs: &[JobDescription],
    ) -> Result<Vec<JobId>, CwabClientError> {
        if jobs.is_empty() {
            return Ok(vec![]);
        }

        let mut conn = self.redis_pool.get()?;

        let mut pipeline = redis::pipe();
        pipeline.atomic();

        if let Some(at) = jobs[0].at {
            let timestamp = at.unix_timestamp();
            for job in jobs.iter().cloned() {
                if let Some(crate::job::UniqunessPolicy::Duration(x)) = job.uniqueness_policy {
                    pipeline
                        .cmd("SET")
                        .arg(&job.uniqueness_key)
                        .arg(true)
                        .arg("EXAT")
                        // No scheduling any job until the next time + the uniqueness duration
                        .arg((at + x).unix_timestamp());
                }
                pipeline.zadd(Queue::Scheduled.namespaced(&job.namespace), job, timestamp);
            }
        } else {
            for mut job in jobs.iter().cloned() {
                if let Some(crate::job::UniqunessPolicy::Duration(x)) = job.uniqueness_policy {
                    pipeline
                        .cmd("SET")
                        .arg(&job.uniqueness_key)
                        .arg(true)
                        .arg("EX")
                        .arg(x.as_secs());
                }
                job.enqueued_at = Some(time::OffsetDateTime::now_utc());
                let namespace = job.namespace.clone();
                pipeline
                    .sadd("queues", &namespace)
                    .lpush(Queue::Enqueued.namespaced(&namespace), job);
            }
        }
        pipeline.query(&mut conn)?;

        Ok(jobs.iter().map(|x| x.job_id).collect())
    }

    pub(crate) fn retry_job_without_incrementing(
        &self,
        queue: Queue,
        job: &JobDescription,
    ) -> Result<(), anyhow::Error> {
        let mut conn = self.redis_pool.get()?;
        match queue {
            Queue::Enqueued => {
                conn.rpush(Queue::Enqueued.namespaced(&job.namespace), job)?;
            }
            Queue::Scheduled => {
                conn.zadd(
                    Queue::Scheduled.namespaced(&job.namespace),
                    job,
                    job.at.expect("INVARIANT VIOLATED: Rescheduling a job. It should have a scheduled time!").unix_timestamp(),
                )?;
            }
            _ => {
                panic!("INVARIANT VIOLATED, shouldn't be giving `retry_job_without_incrementing` another type of queue!")
            }
        }

        Ok(())
    }

    pub(crate) fn pop_job(
        &self,
        queue: Queue,
        namespace: &str,
    ) -> Result<Option<JobDescription>, anyhow::Error> {
        match queue {
            Queue::Enqueued => {
                self.pop_from_to_list_job(Queue::Enqueued, Queue::Working, namespace)
            }
            Queue::Retrying => self.pop_from_list_job(Queue::Retrying, namespace),
            Queue::Scheduled => self.pop_from_sorted_set(Queue::Scheduled, namespace),
            Queue::Dead => self.pop_from_sorted_set(Queue::Dead, namespace),
            _ => todo!(),
        }
    }

    fn pop_from_to_list_job(
        &self,
        from: Queue,
        to: Queue,
        namespace: &str,
    ) -> Result<Option<JobDescription>, anyhow::Error> {
        let mut conn = self.redis_pool.get()?;
        let q = conn
            .rpoplpush::<_, _, Option<JobDescription>>(
                from.namespaced(namespace),
                to.namespaced(namespace),
            )
            .ok()
            .flatten();
        Ok(q)
    }

    fn pop_from_list_job(
        &self,
        from: Queue,
        namespace: &str,
    ) -> Result<Option<JobDescription>, anyhow::Error> {
        let mut conn = self.redis_pool.get()?;
        let q = conn
            .rpop::<_, Option<JobDescription>>(from.namespaced(namespace), None)
            .ok()
            .flatten();
        Ok(q)
    }

    fn pop_from_sorted_set(
        &self,
        queue: Queue,
        namespace: &str,
    ) -> Result<Option<JobDescription>, anyhow::Error> {
        let mut conn = self.redis_pool.get()?;
        let queue_key = queue.namespaced(namespace);
        let q: Option<(JobDescription, usize)> = conn.zpopmin(&queue_key, 1).ok();
        if let Some((job, _time)) = &q {
            let t = job.at.unwrap();
            if time::OffsetDateTime::now_utc() < t {
                conn.zadd(&queue_key, job, t.unix_timestamp())?;
                return Ok(None);
            }
            return Ok(Some(job.clone()));
        }
        Ok(None)
    }

    pub(crate) fn send_heartbeat(&self, job: &JobDescription) -> Result<(), anyhow::Error> {
        let mut conn = self.redis_pool.get()?;
        let timestamp: usize = (OffsetDateTime::now_utc() + Duration::from_secs(60))
            .unix_timestamp()
            .try_into()
            .unwrap();
        conn.set_ex(format!("heartbeat:{}", job.job_id), true, timestamp)?;
        Ok(())
    }

    pub(crate) fn last_heartbeat_check(&self) -> Result<Option<OffsetDateTime>, anyhow::Error> {
        let mut conn = self.redis_pool.get()?;
        let s: Option<String> = conn.get("last_heartbeat_scan")?;
        Ok(s.map(|q| {
            serde_json::from_str(&q).expect("Tried to parse an invalid heartbeat timestamp")
        }))
    }

    pub(crate) fn check_heartbeat(&self, namespace: &str) -> Result<(), anyhow::Error> {
        let mut conn = self.redis_pool.get()?;
        conn.set(
            "last_heartbeat_scan",
            serde_json::to_string(&OffsetDateTime::now_utc())?,
        )?;
        for job in self
            .redis_pool
            .get()?
            .sscan::<String, JobDescription>(Queue::Enqueued.namespaced(namespace))?
        {
            let heart: Option<bool> = conn.get(format!("heartbeat:{}", job.job_id))?;
            if !heart.unwrap_or(false) {
                if let Some(policy) = job.retry_policy {
                    match policy {
                        RetryPolicy::Forever { .. } => {
                            self.change_status(&job, Queue::Retrying)?;
                        }
                        RetryPolicy::Count { count, .. } => {
                            if job.retry.unwrap_or(0) < count {
                                self.change_status(&job, Queue::Retrying)?;
                            } else {
                                self.change_status(&job, Queue::Dead)?;
                            }
                        }
                    }
                } else {
                    self.change_status(&job, Queue::Dead)?;
                }
            }
        }

        Ok(())
    }

    pub(crate) async fn pop_dead_job(&self, namespace: &str) -> Result<(), anyhow::Error> {
        let mut conn = self.redis_pool.get()?;
        let queue_key = Queue::Dead.namespaced(namespace);
        let q: Option<(JobDescription, usize)> = conn.zpopmin(&queue_key, 1).ok();
        if let Some((job, _time)) = &q {
            let t = job.at.unwrap();
            // If the oldest job isn't old enough to be deleted, then we put it back
            let current_time = time::OffsetDateTime::now_utc();
            if current_time < t {
                conn.zadd(&queue_key, job, t.unix_timestamp())?;

                let wait_time =
                    std::cmp::min(MAX_WAIT, Duration::try_from(t - current_time).unwrap());

                tokio::time::sleep(wait_time).await
            }
        } else {
            tokio::time::sleep(MAX_WAIT).await
        }

        Ok(())
    }

    pub(crate) fn change_status(
        &self,
        job: &JobDescription,
        to: Queue,
    ) -> Result<(), anyhow::Error> {
        let mut conn = self.redis_pool.get()?;

        let mut pipe = redis::pipe();
        pipe.lrem(Queue::Working.namespaced(&job.namespace), 1, job);

        match to {
            Queue::Processed => {
                pipe.sadd(Queue::Processed.namespaced(&job.namespace), job);
            }
            Queue::Retrying => {
                let mut new_job = job.clone();
                new_job.retry = new_job.retry.map(|x| x + 1);
                pipe.lpush(Queue::Retrying.namespaced(&job.namespace), new_job);
            }
            Queue::Dead => {
                // Expire dead jobs in 30 days
                let expires_at = OffsetDateTime::now_utc() + Duration::from_secs(60 * 60 * 24 * 30);
                pipe.zadd(
                    Queue::Dead.namespaced(&job.namespace),
                    job,
                    expires_at.unix_timestamp(),
                );
            }
            _ => panic!(
                "INVARIANT VIOLATED. Unexpected state transition from Working -> {}",
                to
            ),
        }
        pipe.query(&mut conn)?;
        Ok(())
    }
}
