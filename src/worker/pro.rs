use super::{InternalWorkerExt, WorkerExt, WorkerState, MAX_WAIT};
use crate::{
    client::CwabClient,
    cwab::pro::next_time,
    job::{Job, JobDescription, JobInput},
    prelude::CwabError,
    Config,
};
use async_trait::async_trait;
use base64::{engine::general_purpose, Engine as _};
use chacha20poly1305::{
    aead::{Aead, KeyInit},
    XChaCha20Poly1305,
};
use cron::Schedule;
use futures::FutureExt;
use pbkdf2::pbkdf2_hmac;
use sha2::Sha256;
use std::{
    collections::{HashMap, HashSet},
    panic::AssertUnwindSafe,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::task::JoinHandle;

const ROUNDS: u32 = 600_000;
const SALT: &'static [u8; 20] = b"cwab is a tasty food";

#[derive(Clone)]
pub struct Worker {
    worker: WorkerState,
    pub(crate) cipher: Option<XChaCha20Poly1305>,
}

impl Worker {
    #[allow(dead_code)]
    pub(crate) fn new(config: &Config, client: CwabClient) -> Result<Self, anyhow::Error> {
        Ok(Worker {
            worker: WorkerState::new(config, client)?,
            cipher: config.secret.as_ref().map(|s| {
                let mut key = [0u8; 32];
                pbkdf2_hmac::<Sha256>(s.as_bytes(), SALT, ROUNDS, &mut key);
                XChaCha20Poly1305::new(&key.into())
            }),
        })
    }
}

#[async_trait]
impl InternalWorkerExt for Worker {
    fn client(&self) -> CwabClient {
        self.worker.client.clone()
    }

    fn config(&self) -> Config {
        self.worker.config.clone()
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
        self.worker.termination_bool.clone()
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
impl WorkerExt for Worker {
    fn register(&mut self, job: impl Job) {
        self.worker
            .registered_jobs
            .insert(job.name().to_string(), Box::new(job));
    }

    fn registered_jobs(&self) -> &HashMap<String, Box<dyn Job>> {
        &self.worker.registered_jobs
    }

    async fn reserve_work(&self, namespace: &str) -> Result<Option<JobDescription>, CwabError> {
        super::reserve_work(self, namespace).await
    }

    async fn start_working(self) -> Result<(), CwabError> {
        super::start_working(self).await
    }

    async fn start_bookkeeping(self) -> Result<(), CwabError> {
        super::start_bookkeeping(self).await
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
