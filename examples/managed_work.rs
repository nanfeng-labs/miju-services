//! Start after a finite prerequisite and drain service-owned background work.

use miju_services::{App, BoxError, Result, Spec};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result {
    let report = assemble().run_until(tokio::signal::ctrl_c()).await?;
    println!("{report}");
    Ok(())
}

pub(crate) fn assemble() -> App {
    let mut app = App::new();
    app.service_fn(Spec::new("prepare").finite(), |_ctx| async {
        println!("prepare shared state");
        Ok(())
    });
    app.service_fn(Spec::new("worker").after("prepare"), |ctx| async move {
        let background = ctx.clone();
        ctx.spawn("background", async move {
            background.stopping().await;
            println!("background drained");
            Ok::<_, BoxError>(())
        })?;
        ctx.ready();
        ctx.stopping().await;
        // Returning closes admission; the application waits for accepted work.
        Ok(())
    });
    app
}
