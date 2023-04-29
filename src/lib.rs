//! This is a simple example of how you'd implement a worker.
//! Here we spawn jobs before we create a worker, but that would generally be done on other machines.
//!
//! By default, `cwab` will look for configuration in the following order:
//! - Attempts to load the file `cwab.toml`, or a file passed in with `--config`
//! - Looks for ENV vars like `REDIS_URL`
//! - Fallback to using `redis://127.0.0.1:6379`
//!
//! The config format is as follows:
//! ```toml
//! version = 1
//! redis_url = "redis://127.0.0.1:6379"
//! ```
//!
//! NOTE: This doesn't include running `cwab librarian start`, which is a separate binary this library provides, which handles bookkeeping work not covered by workers (Scheduled job polling, retry scheduling, etc...).
//! You should probably only run one instance of `cwab` somewhere, since it doesn't need much resources, but could put a lot of load on your redis instance otherwise.
//!```ignore
//! use anyhow::Result;
//! use cwab::prelude::*;
//! use async_trait::async_trait;
//! use serde::{Serialize, Deserialize};
//!
//! #[derive(Serialize, Deserialize, Copy, Clone, Debug)]
//! pub struct HelloJob;
//!
//! #[async_trait]
//! impl Job for HelloJob {
//!     fn name(&self) -> &'static str {
//!         "HelloJob"
//!     }
//!
//!     // Note that input is an optional arbitrary string.
//!     // You could pass in JSON and parse it in your job.
//!     async fn perform(&self, input: Option<String>) -> Result<Option<String>, JobError> {
//!         let to_print = if let Some(i) = input {
//!             format!("Hello {:?}", i)
//!         } else {
//!             format!("Hello World")
//!         };
//!         println!("{}", to_print);
//!         Ok(None)
//!     }
//! }
//!
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!     let config = Config::new(None)?;
//!     let cwab = Cwab::new(&config)?;
//!     let mut worker = cwab.worker();
//!     worker.register(HelloJob);
//!
//!     cwab.perform_async(HelloJob, None)
//!         .await?;
//!     cwab.perform_async(HelloJob, Some("Bob".to_string()))
//!         .await?;
//!
//!     worker.start_working().await?;
//!     Ok(())
//! }
//!```

use std::time::Duration;

use serde::{Deserialize, Serialize};

pub(crate) mod client;
pub(crate) mod cwab;
pub(crate) mod job;
pub(crate) mod worker;
pub(crate) const MAX_WAIT: Duration = Duration::from_secs(5);

/// This is what you'll import to get started. Click in for more documentation.
pub mod prelude;

/// Configuration used to (de)serialize a TOML configuration file.
/// Used to customize various aspects of the runtime.
#[derive(Serialize, Deserialize, Clone, Debug, Hash, PartialEq, Eq)]
pub struct Config {
    /// Used to version the configuration format
    version: usize,
    /// A `redis://...` url
    redis_url: String,
    /// A secret used to encrypt job inputs
    secret: Option<String>,
}

impl Config {
    pub fn new(config_path: Option<&str>) -> Result<Config, anyhow::Error> {
        let path_str = config_path.unwrap_or("cwab.toml");
        let path = std::path::Path::new(path_str);
        let config = if path.exists() {
            toml::from_str::<Config>(std::str::from_utf8(&std::fs::read(path_str)?)?)?
        } else {
            println!("No config file found, falling back to ENV vars");
            let redis_url = std::env::var("REDIS_URL").unwrap_or_else(|_| {
                println!("REDIS_URL is not set, falling back to redis://127.0.0.1:6379");
                "redis://127.0.0.1:6379".to_string()
            });

            let secret = std::env::var("CWAB_SECRET").ok();
            if secret.is_none() {
                println!("CWAB_SECRET is not set, disabling encryption");
            }

            Config {
                version: 1,
                redis_url,
                secret,
            }
        };
        Ok(config)
    }
}
