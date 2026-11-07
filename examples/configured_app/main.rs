//! Load application settings, assemble a file-backed sampler, and stop with Ctrl-C.

mod bootstrap;
mod services;
mod settings;

use clap::Parser;
use miju_services::BoxError;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), BoxError> {
    let cli = settings::Cli::parse();
    let loaded = settings::load(&cli, settings::environment(std::env::vars_os())?)?;
    if cli.explain {
        println!("{}", loaded.explain());
        return Ok(());
    }
    let report = bootstrap::assemble(loaded.settings)?
        .run_until(tokio::signal::ctrl_c())
        .await?;
    println!("{report:?}");
    Ok(())
}
