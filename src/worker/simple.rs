use crate::{
    client::CwabClient,
    job::{Job, JobDescription, JobInput},
    prelude::CwabError,
    Config,
};
use async_trait::async_trait;
use std::{
    collections::{HashMap, HashSet},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};
use tokio::task::JoinHandle;

use super::{InternalWorkerExt, Worker, WorkerExt, MAX_WAIT};

/// The free implementation of the worker.
/// It supports most features, but isn't parallel. All work is done sequentially.
#[derive(Clone, Debug)]
pub struct SimpleWorker(Worker);

impl SimpleWorker {
    pub(crate) fn new(config: &Config, client: CwabClient) -> Result<Self, anyhow::Error> {
        Ok(SimpleWorker(Worker::new(config, client)?))
    }
}

#[async_trait]
impl InternalWorkerExt for SimpleWorker {
    fn read_input(&self, job_description: &JobDescription) -> Option<String> {
        // We do this because we don't want to risk it blowing up and us losing our heartbeat handle
        std::panic::catch_unwind(|| {
            job_description.input.clone().map(|x| match x {
                JobInput::Plaintext(i) => i,
                JobInput::EncryptedInput {
                    nonce: _,
                    ciphertext: _,
                } => panic!("Encrypted job inputs are a Pro feature"),
            })
        })
        .ok()
        .flatten()
    }

    async fn reschedule_periodic_job(&self, _job: &JobDescription) -> Result<(), anyhow::Error> {
        Ok(())
    }

    fn client(&self) -> CwabClient {
        self.0.client.clone()
    }

    fn term_bool(&self) -> Arc<AtomicBool> {
        self.0.termination_bool.clone()
    }

    async fn run_workers(
        &self,
        term_bool: Arc<AtomicBool>,
        worker: Arc<impl WorkerExt + InternalWorkerExt + Sync + Send + 'static>,
        queue_task_namespaces: &HashSet<&'static str>,
    ) -> JoinHandle<Result<(), anyhow::Error>> {
        let queue_task_namespaces = queue_task_namespaces.clone();
        tokio::spawn(async move {
            'outer: loop {
                for namespace in &queue_task_namespaces {
                    if term_bool.load(Ordering::Relaxed) {
                        break 'outer;
                    }
                    if let Some(found) = worker.reserve_work(namespace).await? {
                        crate::worker::do_work(&*worker, found).await.ok();
                    } else {
                        tokio::time::sleep(MAX_WAIT).await;
                    }
                }
            }
            Ok::<(), anyhow::Error>(())
        })
    }
}

#[async_trait]
impl WorkerExt for SimpleWorker {
    fn register(&mut self, job: impl Job + Copy) {
        self.0
            .registered_jobs
            .insert(job.name().to_string(), Box::new(job));
    }

    fn registered_jobs(&self) -> &HashMap<String, Box<dyn Job>> {
        &self.0.registered_jobs
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
