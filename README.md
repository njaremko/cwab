# Cwab

<img src="https://user-images.githubusercontent.com/3753178/233761007-ececf241-e084-4627-abf4-0250d55772bb.svg" alt="Adorable 2D cartoon crab fumbling with papers" width="512" height="512" />

Correct, efficient, and simple. Lets process some jobs.

# Installation
The `cwab` library is installed by adding the following to your `Cargo.toml`
```toml
cwab = "^0.5"
```

You'll also need to run the following to install the CLI
```bash
cargo install cwab
```

# Free Features
- [x] Reliable job dispatch
- [x] Error handling with retries
- [x] Scheduled jobs
- [x] An easy to use Rust API
- [x] Middleware support

# Paid Features
- [x] Batched jobs
- [x] Cron support
- [x] Expiring jobs
- [x] Unique jobs
- [x] Encryption
- [x] Rate limiting
- [x] Parallelism
- [x] Web UI
- [x] Metrics
- [x] Dedicated support
- [x] A commercial license

### To purchase

Use the below links to purchase a commercial license. You'll receive access to Cwab Pro within 24 hours after payment.

[Click this link](https://buy.stripe.com/bIYg2H8qP2CJ21q000) to pay $99 USD monthly

[Click this link](https://buy.stripe.com/fZeaIn7mL3GN35u9AB) to pay $995 USD yearly

# Documentation

You can find our documentation [here](https://github.com/cwabcorp/cwab/wiki)

# Basic use

Below is a basic implementation of a worker.

This example includes scheduling some jobs before starting the worker, which you would usually do on another machine.

To run, you'd run `cwab librarian start` in a separate shell to start the bookkeeping worker, and then run `cargo run` in your project.

```rust
use anyhow::Result;
use async_trait::async_trait;
use cwab::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Copy, Clone, Debug)]
pub struct HelloJob;

#[async_trait]
impl Job for HelloJob {
    fn name(&self) -> &'static str {
        "HelloJob"
    }

    async fn perform(&self, input: Option<String>) -> Result<Option<String>, JobError> {
        let to_print = if let Some(i) = input {
            format!("Hello {:?}", i)
        } else {
             format!("Hello World")
        };
        println!("{}", to_print);
        Ok(None)
    }
}


#[tokio::main]
async fn main() -> Result<()> {
    let config = Config::new(None)?;
    let cwab = Cwab::new(&config)?;
    let mut worker = cwab.worker();
    worker.register(HelloJob)?;

    cwab.perform_async(HelloJob, None)
        .await
        .expect("Failed to schedule job");
    cwab.perform_async(HelloJob, Some("Bob".to_string()))
        .await
        .expect("Failed to schedule job");

    worker.start().await.expect("An unexpected error occurred");
    Ok(())
}
```

# Contributing
If you want to contribute, I recommend looking at our [good first issues](https://github.com/cwabcorp/cwab/contribute). Our [contributing file](.github/CONTRIBUTING.md) has some tips for getting started.
