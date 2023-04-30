use anyhow::Result;
use clap::{Parser, Subcommand};
use cwab::prelude::*;

/// All the top level commands / subcommands
#[derive(Subcommand, Debug)]
pub enum Commands {
    #[command(subcommand, about = "Subcommand to handle bookkeeping worker")]
    Librarian(LibrarianCommands),
}

/// Commands specific to the bookkeeper
#[derive(Subcommand, Debug)]
pub enum LibrarianCommands {
    #[command(about = "Start the librarian")]
    Start {
        #[arg(long, default_value = None)]
        queues: Option<String>,
    },
}

/// CwabCli parser struct
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct CwabCli {
    #[command(subcommand)]
    pub command: Commands,
    #[arg(short, long, default_value = None, global = true)]
    pub config: Option<String>,
}

fn parse_namespaces(config: &Config, queues: Option<String>) -> Vec<String> {
    if let Some(queues) = queues {
        queues.split(',').map(|x| x.trim().to_string()).collect()
    } else {
        // Default to watching 'default'
        config
            .namespaces
            .clone()
            .unwrap_or_else(|| vec!["default".to_string()])
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = CwabCli::parse();
    let mut config = Config::new(args.config.as_deref())?;
    match args.command {
        Commands::Librarian(librarian) => [match librarian {
            LibrarianCommands::Start { queues } => {
                let namespaces = parse_namespaces(&config, queues);
                println!("Starting librarian...");
                config.namespaces = Some(namespaces);

                let cwab = Cwab::new(&config)?;
                let worker = cwab.worker();
                worker.start_bookkeeping().await?;
            }
        }],
    };

    Ok(())
}
