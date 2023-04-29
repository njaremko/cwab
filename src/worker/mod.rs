pub mod simple;

use crate::client::CwabClient;
use crate::prelude::{
    Backoff, CwabError, Job, JobDescription, JobError, JobId, Queue, RetryPolicy,
};
use crate::{Config, MAX_WAIT};
use anyhow::anyhow;
use async_trait::async_trait;
use futures::{join, FutureExt};
use rand::Rng;
use signal_hook::consts::TERM_SIGNALS;
use signal_hook::flag;
use std::collections::{HashMap, HashSet};
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use time::OffsetDateTime;
use tokio::task::JoinHandle;

/// This is the trait that describes the basic functionality that a worker needs to support
#[async_trait]
pub trait WorkerExt {
    /// Registers a job as workable by this worker. All job types must be registered before you start the worker.
    fn register(&mut self, job: impl Job + Copy);
    /// Returns a map of job names to job instances
    fn registered_jobs(&self) -> &HashMap<String, Box<dyn Job>>;
    /// Given a namespace, pops a job off of the work queue, if available
    async fn reserve_work(&self, namespace: &str) -> Result<Option<JobDescription>, CwabError>;
    /// Starts the bookkeeping work threads.
    /// This should be run on a separate machine than your workers, if you have more than one.
    /// - Scheduling jobs for retry
    /// - Deleting old dead jobs
    /// - Looking for scheduled jobs that need to be queued
    async fn start_bookkeeping(self) -> Result<(), CwabError>;
    /// Starts pulling work off the queue and executing it
    async fn start_working(self) -> Result<(), CwabError>;
}

#[async_trait]
pub(super) trait InternalWorkerExt {
    fn read_input(&self, job_description: &JobDescription) -> Option<String>;
    async fn reschedule_periodic_job(&self, job: &JobDescription) -> Result<(), anyhow::Error>;
    fn client(&self) -> CwabClient;
    async fn run_workers(
        &self,
        term_bool: Arc<AtomicBool>,
        queue_worker: Arc<impl WorkerExt + InternalWorkerExt + Sync + Send + 'static>,
        queue_task_namespaces: &HashSet<&'static str>,
    ) -> JoinHandle<Result<(), anyhow::Error>>;
    fn term_bool(&self) -> Arc<AtomicBool>;
}

#[derive(Clone, Debug)]
pub(crate) struct Worker {
    registered_jobs: HashMap<String, Box<dyn Job>>,
    client: CwabClient,
    #[allow(dead_code)]
    config: Config,
    termination_bool: Arc<AtomicBool>,
}

impl Worker {
    pub(crate) fn new(config: &Config, client: CwabClient) -> Result<Self, anyhow::Error> {
        let termination_bool = Arc::new(AtomicBool::new(false));
        for sig in TERM_SIGNALS {
            // When terminated by a second term signal, exit with exit code 1.
            // This will do nothing the first time (because term_now is false).
            flag::register_conditional_shutdown(*sig, 1, Arc::clone(&termination_bool))?;
            // But this will "arm" the above for the second time, by setting it to true.
            // The order of registering these is important, if you put this one first, it will
            // first arm and then terminate ‒ all in the first round.
            flag::register(*sig, Arc::clone(&termination_bool))?;
        }
        Ok(Worker {
            registered_jobs: HashMap::new(),
            client,
            config: config.clone(),
            termination_bool,
        })
    }
}

pub(super) async fn reserve_work<W: WorkerExt + InternalWorkerExt>(
    w: &W,
    namespace: &str,
) -> Result<Option<JobDescription>, CwabError> {
    let q = w.client().pop_job(Queue::Enqueued, namespace)?;
    if let Some(job) = &q {
        if !w.registered_jobs().contains_key(&job.job_type) {
            // Put it back on the queue
            w.client()
                .retry_job_without_incrementing(Queue::Enqueued, job)?;
            return Ok(None);
        }
    }

    Ok(q)
}

pub(super) async fn enqueue_scheduled_work<W: WorkerExt + InternalWorkerExt>(
    w: &W,
    namespace: &str,
) -> Result<Option<JobId>, CwabError> {
    let client = w.client();
    let q = client.pop_job(Queue::Scheduled, namespace)?;
    if let Some(job) = q {
        let current = OffsetDateTime::now_utc();
        let job_scheduled_at = job.at.expect("Got scheduled job with no timestamp!");

        if !w.registered_jobs().contains_key(&job.job_type) || current < job_scheduled_at {
            client.retry_job_without_incrementing(Queue::Scheduled, &job)?;
            // We wait until next scheduled job, or 5 seconds, whichever is sooner.
            let clamped_duration = std::cmp::min(
                std::time::Duration::from_secs_f64((job_scheduled_at - current).as_seconds_f64()),
                MAX_WAIT,
            );
            tokio::time::sleep(clamped_duration).await;
            return Ok(None);
        }

        w.reschedule_periodic_job(&job).await?;

        let pushed = client.raw_push(&[job.enqueue()]).await?;

        Ok(Some(pushed[0]))
    } else {
        tokio::time::sleep(MAX_WAIT).await;
        Ok(q.map(|x| x.job_id))
    }
}

pub(super) async fn enqueue_retry_work<W: WorkerExt + InternalWorkerExt>(
    w: &W,
    namespace: &str,
) -> Result<Option<JobId>, CwabError> {
    let client = w.client();
    let q = client.pop_job(Queue::Retrying, namespace)?;
    if let Some(mut job) = q {
        let mut apply_backoff = |retry_count: u64, backoff| match backoff {
            Backoff::Default => {
                let mut rng = rand::thread_rng();
                let rand_int: u64 = rng.gen_range(0..10);
                let duration =
                    Duration::from_secs(retry_count.pow(4) + 15 + (rand_int * (retry_count + 1)));
                job.at = Some(OffsetDateTime::now_utc() + duration);
            }
            Backoff::Linear(d) => {
                job.at = Some(OffsetDateTime::now_utc() + d);
            }
            Backoff::Exponential(d) => {
                job.at = Some(if let Some(previous_backoff) = job.backoff {
                    OffsetDateTime::now_utc() + (previous_backoff * 2)
                } else {
                    OffsetDateTime::now_utc() + d
                })
            }
        };
        let retry_count = job.retry.unwrap_or(0);
        match job.retry_policy {
            Some(RetryPolicy::Forever { backoff }) => Ok({
                job.retry = Some(retry_count + 1);
                apply_backoff(retry_count as u64, backoff);
                let res = w.client().raw_push(&[job]).await?;
                Some(res[0])
            }),
            Some(RetryPolicy::Count { count, backoff }) => {
                if retry_count < count - 1 {
                    job.retry = Some(retry_count + 1);
                    apply_backoff(retry_count as u64, backoff);
                    let res = w.client().raw_push(&[job]).await?;
                    Ok(Some(res[0]))
                } else {
                    Ok(None)
                }
            }
            _ => Ok(None),
        }
    } else {
        tokio::time::sleep(MAX_WAIT).await;
        Ok(q.map(|x| x.job_id))
    }
}

pub(super) async fn start_bookkeeping<
    W: WorkerExt + InternalWorkerExt + Clone + Send + Sync + 'static,
>(
    w: W,
) -> Result<(), CwabError> {
    let term_now = w.term_bool();
    let worker = Arc::new(w);

    let namespaces: HashSet<&'static str> = worker
        .registered_jobs()
        .values()
        .map(|v| v.queue())
        .collect();

    let heartbeat_client = worker.client();
    let loop_term = term_now.clone();
    let heartbeat_namespaces = namespaces.clone();
    let heartbeat_task = tokio::spawn(async move {
        let mut last_check = heartbeat_client
            .last_heartbeat_check()?
            .unwrap_or_else(OffsetDateTime::now_utc);
        let an_hour = Duration::from_secs(3600);
        'outer: loop {
            if loop_term.load(Ordering::Relaxed) {
                break 'outer;
            }
            for namespace in &heartbeat_namespaces {
                if loop_term.load(Ordering::Relaxed) {
                    break 'outer;
                }
                let now = OffsetDateTime::now_utc();
                // Only check once an hour
                if last_check + an_hour < now {
                    heartbeat_client.check_heartbeat(namespace)?;
                    last_check = now;
                }
                tokio::time::sleep(MAX_WAIT).await;
            }
        }
        Ok::<(), anyhow::Error>(())
    });

    let dead_job_client = worker.client();
    let loop_term = term_now.clone();
    let dead_job_namespaces = namespaces.clone();
    let dead_job_task = tokio::spawn(async move {
        'outer: loop {
            if loop_term.load(Ordering::Relaxed) {
                break 'outer;
            }
            for namespace in &dead_job_namespaces {
                if loop_term.load(Ordering::Relaxed) {
                    break 'outer;
                }
                dead_job_client.pop_dead_job(namespace).await?;
            }
        }
        Ok::<(), anyhow::Error>(())
    });

    let loop_term = term_now.clone();
    let scheduled_worker = worker.clone();
    let scheduled_task_namespaces = namespaces.clone();
    let scheduled_task = tokio::spawn(async move {
        'outer: loop {
            if loop_term.load(Ordering::Relaxed) {
                break 'outer;
            }
            for namespace in &scheduled_task_namespaces {
                if loop_term.load(Ordering::Relaxed) {
                    break 'outer;
                }
                enqueue_scheduled_work(&*scheduled_worker, namespace).await?;
            }
        }
        Ok::<(), anyhow::Error>(())
    });

    let loop_term = term_now.clone();
    let retry_worker = worker.clone();
    let retry_task_namespaces = namespaces.clone();
    let retry_task = tokio::spawn(async move {
        'outer: loop {
            if loop_term.load(Ordering::Relaxed) {
                break 'outer;
            }
            for namespace in &retry_task_namespaces {
                if loop_term.load(Ordering::Relaxed) {
                    break 'outer;
                }
                enqueue_retry_work(&*retry_worker, namespace).await?;
            }
        }
        Ok::<(), anyhow::Error>(())
    });

    let loop_term = term_now.clone();
    let dying_task = tokio::spawn(async move {
        loop {
            if loop_term.load(Ordering::Relaxed) {
                println!("Putting all the books back on the shelf...");
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    });

    let (_, _, _, _, _) = futures::join!(
        heartbeat_task,
        retry_task,
        scheduled_task,
        dead_job_task,
        dying_task
    );
    Ok(())
}

pub(super) async fn start_working<
    W: WorkerExt + InternalWorkerExt + Clone + Send + Sync + 'static,
>(
    w: W,
) -> Result<(), CwabError> {
    let worker = Arc::new(w);

    let namespaces: HashSet<&'static str> = worker
        .registered_jobs()
        .values()
        .map(|v| v.queue())
        .collect();

    let loop_term = worker.term_bool();
    let dying_task = tokio::spawn(async move {
        loop {
            if loop_term.load(Ordering::Relaxed) {
                println!("Shutting down...");
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    });

    println!("Starting worker");
    let queue_task = worker
        .run_workers(worker.term_bool(), worker.clone(), &namespaces)
        .await;

    let (_, _) = join!(queue_task, dying_task);
    Ok(())
}

fn start_heartbeat<W: WorkerExt + InternalWorkerExt>(
    w: &W,
    job_description: &JobDescription,
) -> JoinHandle<()> {
    let heartbeat_client = w.client();
    let heartbeat_job_desc = job_description.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(5));
        loop {
            interval.tick().await;
            heartbeat_client.send_heartbeat(&heartbeat_job_desc).ok();
        }
    })
}

async fn wrapped_platform<W: WorkerExt + InternalWorkerExt>(
    w: &W,
    job_description: &JobDescription,
    job_input: Option<String>,
) -> Result<Option<String>, JobError> {
    let job = w
        .registered_jobs()
        .get(&job_description.job_type)
        .ok_or_else(|| {
            anyhow!(
                "INVARIANT VIOLATED: Reserved work that we can't do!\n{:?}",
                job_description
            )
        })?;

    // Send a heartbeat while the job runs
    let heartbeat = start_heartbeat(w, job_description);

    let result = AssertUnwindSafe(job.perform(job_input))
        .catch_unwind()
        .await;

    // Kill the heartbeat
    heartbeat.abort();

    match result {
        Ok(success) => success,
        Err(panic) => Err(JobError::PanicError {
            panic: format!("{:?}", panic),
        }),
    }
}

pub(crate) async fn do_work<W: WorkerExt + InternalWorkerExt>(
    w: &W,
    found: JobDescription,
) -> Result<(), anyhow::Error> {
    let job_arc = Arc::new(found);

    let job_description = job_arc.clone();
    let job_input = w.read_input(&job_description);

    let result = wrapped_platform(w, &job_description, job_input).await;

    let handle_error = || {
        if job_description.retry_policy.is_some() {
            w.client()
                .change_status(&job_description, Queue::Retrying)
                .expect("INVARIANT VIOLATED: Failed to change status");
        } else {
            w.client()
                .change_status(&job_description, Queue::Dead)
                .expect("INVARIANT VIOLATED: Failed to change status");
        }
    };
    match &result {
        Ok(_job_output) => {
            w.client()
                .change_status(&job_description, Queue::Processed)
                .expect("INVARIANT VIOLATED: Failed to change status");
        }
        Err(_) => handle_error(),
    }
    Ok(())
}
