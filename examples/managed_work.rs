//! Start after a finite prerequisite and drain service-owned background work.

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), BoxError> {
    let report = assemble()?.run_until(tokio::signal::ctrl_c()).await?;
    println!("{report:?}");
    Ok(())
}

use miju_services::{
    App, BoxError, BuildError,
    service::{Lifetime, ServiceSpec},
    work::WorkSpec,
};

pub(crate) fn assemble() -> Result<App, BuildError> {
    App::builder()
        .service_fn(
            ServiceSpec::new("prepare").lifetime(Lifetime::Finite),
            |_context| async {
                println!("prepare shared state");
                Ok(())
            },
        )
        .service_fn(
            ServiceSpec::new("worker").depends_on("prepare"),
            |context| async move {
                context
                    .work()
                    .spawn(WorkSpec::new("background"), |work| async move {
                        work.stopping().await;
                        println!("background drained");
                        Ok::<(), BoxError>(())
                    })?;
                context.ready();
                context.stopping().await;
                // Returning closes admission and waits for this service's accepted work.
                Ok(())
            },
        )
        .build()
}
