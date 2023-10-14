use crate::prelude::{
    Backoff, CwabError, Job, JobDescription, JobError, JobId, Queue, RetryPolicy,
};
use crate::{client::CwabClient, cwab::next_time, job::JobInput, Config, MAX_WAIT};
use anyhow::anyhow;
use async_trait::async_trait;
use base64::{engine::general_purpose, Engine as _};
use chacha20poly1305::{
    aead::{Aead, KeyInit},
    XChaCha20Poly1305,
};
use cron::Schedule;
use futures::{join, FutureExt};
use pbkdf2::pbkdf2_hmac;
use rand::Rng;
use sha2::Sha256;
use signal_hook::consts::TERM_SIGNALS;
use signal_hook::flag;
use std::{
    collections::{HashMap, HashSet},
    panic::AssertUnwindSafe,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use time::OffsetDateTime;
use tokio::task::JoinHandle;

const ROUNDS: u32 = 600_000;
const SALT: &'static [u8; 20] = b"cwab is a tasty food";

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
pub(crate) trait InternalWorkerExt {
    fn read_input(&self, job_description: &JobDescription) -> Option<String>;
    async fn reschedule_periodic_job(&self, job: &JobDescription) -> Result<(), anyhow::Error>;
    fn client(&self) -> CwabClient;
    fn config(&self) -> Config;
    async fn run_workers(
        &self,
        term_bool: Arc<AtomicBool>,
        queue_worker: Arc<impl WorkerExt + InternalWorkerExt + Sync + Send + 'static>,
        queue_task_namespaces: &HashSet<&'static str>,
    ) -> JoinHandle<Result<(), anyhow::Error>>;
    fn term_bool(&self) -> Arc<AtomicBool>;
}

#[derive(Clone)]
pub struct WorkerState {
    registered_jobs: HashMap<String, Box<dyn Job>>,
    client: CwabClient,
    #[allow(dead_code)]
    config: Config,
    termination_bool: Arc<AtomicBool>,
    cipher: Option<XChaCha20Poly1305>,
}

impl WorkerState {
    pub fn new(config: &Config, client: CwabClient) -> Result<Self, anyhow::Error> {
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

        let config = config.clone();
        println!(
            "Watching namespaces: {}",
            config
                .namespaces
                .as_ref()
                .expect("INVARIANT VIOLATED: No namespaces when creating worker!")
                .iter()
                .cloned()
                .reduce(|acc, x| format!("{}, {}", acc, x))
                .expect("INVARIANT VIOLATED: No namespaces provided. Can't watch anything!")
        );

        Ok(WorkerState {
            registered_jobs: HashMap::new(),
            client,
            termination_bool,
            cipher: config.secret.as_ref().map(|s| {
                let mut key = [0u8; 32];
                pbkdf2_hmac::<Sha256>(s.as_bytes(), SALT, ROUNDS, &mut key);
                XChaCha20Poly1305::new(&key.into())
            }),
            config,
        })
    }

    pub(crate) fn cipher(&self) -> &Option<XChaCha20Poly1305> {
        &self.cipher
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

        if current < job_scheduled_at {
            client.retry_job_without_incrementing(Queue::Scheduled, &job)?;
            // We wait until next scheduled job, or 5 seconds, whichever is sooner.
            let clamped_duration = std::cmp::min(
                std::time::Duration::from_secs_f64(
                    (job_scheduled_at - current).abs().as_seconds_f64(),
                ),
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

    let namespaces: Vec<String> = worker
        .config()
        .namespaces
        .unwrap_or_else(|| vec!["default".to_string()]);

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
        Ok(success) => success.map_err(JobError::AnyError),
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

    match &result {
        Ok(_job_output) => {
            w.client()
                .change_status(&job_description, Queue::Processed)
                .expect("INVARIANT VIOLATED: Failed to change status");
        }
        Err(e) => {
            let mut new_job = job_description.as_ref().clone();
            new_job.error_message = Some(format!("{}", e));
            println!(
                "Job {} failed with error: {}",
                &new_job.job_id,
                &new_job
                    .error_message
                    .as_ref()
                    .expect("INVARIANT VIOLATED: Error message must exist here.")
            );
            if job_description.retry_policy.is_some() {
                w.client()
                    .change_status(&new_job, Queue::Retrying)
                    .expect("INVARIANT VIOLATED: Failed to change status");
            } else {
                w.client()
                    .change_status(&new_job, Queue::Dead)
                    .expect("INVARIANT VIOLATED: Failed to change status");
            }
        }
    }
    Ok(())
}

#[async_trait]
impl InternalWorkerExt for WorkerState {
    fn client(&self) -> CwabClient {
        self.client.clone()
    }

    fn config(&self) -> Config {
        self.config.clone()
    }

    fn read_input(&self, job_description: &JobDescription) -> Option<String> {
        // let secret = self.worker.config.secret.to_owned();
        // We do this first, because we don't want to risk it blowing up and us losing our heartbeat handle
        let cipher = self.cipher.clone();
        std::panic::catch_unwind(|| {
            job_description.input.clone().map(|x| match x {
                JobInput::Plaintext(i) => i,
                JobInput::EncryptedInput { nonce, ciphertext } => cipher
                    .map(|s| decrypt_job_input(&s, &JobInput::EncryptedInput { nonce, ciphertext }))
                    .expect("Encountered an encrypted job, but no secret configured!"),
            })
        })
        .ok()
        .flatten()
    }

    fn term_bool(&self) -> Arc<AtomicBool> {
        self.termination_bool.clone()
    }

    async fn reschedule_periodic_job(&self, job: &JobDescription) -> Result<(), anyhow::Error> {
        // If periodic, schedule next run
        if let Some(p) = &job.period {
            // TODO we're ignoring failures here.
            let _ = AssertUnwindSafe(async move {
                let schedule: Schedule = p.parse().expect("");
                let next = next_time(&schedule);
                let mut next_job = job.clone();
                next_job.at = Some(next);
                self.client().raw_push(&[next_job]).await.expect("");
            })
            .catch_unwind()
            .await
            .ok();
        }
        Ok(())
    }

    async fn run_workers(
        &self,
        term_bool: Arc<AtomicBool>,
        queue_worker: Arc<impl WorkerExt + InternalWorkerExt + Sync + Send + 'static>,
        queue_task_namespaces: &HashSet<&'static str>,
    ) -> JoinHandle<Result<(), anyhow::Error>> {
        let thread_count = 4;
        let (sx, rx) = crossbeam::channel::bounded::<JobDescription>(thread_count);

        let queue_worker = queue_worker;
        let new_worker = queue_worker.clone();
        let queue_task_namespaces = queue_task_namespaces.clone();
        let loop_bool = term_bool.clone();
        tokio::spawn(async move {
            let work_poller = tokio::spawn(async move {
                'outer: loop {
                    for namespace in &queue_task_namespaces {
                        if loop_bool.load(Ordering::Relaxed) {
                            break 'outer;
                        }
                        if let Some(found) = queue_worker.reserve_work(namespace).await.unwrap() {
                            sx.send(found).ok();
                        } else {
                            tokio::time::sleep(MAX_WAIT).await;
                        }
                    }
                }
            });

            let parallel_workers = (0..thread_count)
                .map(|_thread_num| {
                    let loop_bool = term_bool.clone();
                    let queue_worker = new_worker.clone();
                    let rx = rx.clone();
                    tokio::spawn(async move {
                        'outer: loop {
                            if loop_bool.load(Ordering::Relaxed) {
                                break 'outer;
                            }
                            if let Ok(found) = rx.recv() {
                                crate::worker::do_work(&*queue_worker, found).await.ok();
                            } else {
                                tokio::time::sleep(Duration::from_millis(500)).await;
                            }
                        }
                    })
                })
                .collect::<Vec<_>>();

            let (_, _) = futures::join!(work_poller, futures::future::join_all(parallel_workers));

            Ok::<(), anyhow::Error>(())
        })
    }
}

#[async_trait]
impl WorkerExt for WorkerState {
    fn register(&mut self, job: impl Job) {
        self.registered_jobs
            .insert(job.name().to_string(), Box::new(job));
    }

    fn registered_jobs(&self) -> &HashMap<String, Box<dyn Job>> {
        &self.registered_jobs
    }

    async fn reserve_work(&self, namespace: &str) -> Result<Option<JobDescription>, CwabError> {
        reserve_work(self, namespace).await
    }

    async fn start_working(self) -> Result<(), CwabError> {
        start_working(self).await
    }

    async fn start_bookkeeping(self) -> Result<(), CwabError> {
        start_bookkeeping(self).await
    }
}

fn decrypt_job_input(cipher: &XChaCha20Poly1305, input: &JobInput) -> String {
    match input {
        JobInput::Plaintext(x) => x.clone(),
        JobInput::EncryptedInput { nonce, ciphertext } => {
            let nonce: [u8; 24] = general_purpose::STANDARD
                .decode(nonce)
                .expect("INVARIANT VIOLATED: Nonce isn't base64 encoded!")
                .try_into()
                .expect("INVARIANT VIOLATED: Nonce is the wrong size!");

            let ciphertext = general_purpose::STANDARD
                .decode(ciphertext)
                .expect("INVARIANT VIOLATED: Ciphertext isn't base64 encoded!");
            let plaintext = cipher
                .decrypt(&nonce.into(), ciphertext.as_ref())
                .expect("INVARIANT VIOLATED");
            String::from_utf8(plaintext).expect("")
        }
    }
}
