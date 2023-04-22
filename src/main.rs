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
    Start,
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

#[tokio::main]
async fn main() -> Result<()> {
    let args = CwabCli::parse();
    let config = Config::new(args.config.as_deref())?;
    match args.command {
        Commands::Librarian(librarian) => [match librarian {
            LibrarianCommands::Start => {
                let cwab = Cwab::new(&config)?;
                let worker = cwab.worker();
                println!("Starting librarian...");
                worker.start_bookkeeping().await?;
            }
        }],
    };

    Ok(())
}
