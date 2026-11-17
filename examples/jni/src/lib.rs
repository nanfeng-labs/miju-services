//! Application-owned settings; App owns the admitted services and their cleanup.

#[path = "../../support/udp.rs"]
mod udp;

use miju_services::{
    App, BoxError, Spec,
    jni::{AppFactory, BuildError, BuildErrorKind},
};
use std::{io::Write, net::SocketAddr, path::PathBuf};
use tokio::sync::oneshot;

struct UdpApp;

impl AppFactory for UdpApp {
    const NAME: &'static str = "miju-jni-udp";
    const VERSION: &'static str = env!("CARGO_PKG_VERSION");

    fn build(args: Vec<String>) -> Result<App, BuildError> {
        let [address, output] = args.as_slice() else {
            return Err(BuildError::new(
                BuildErrorKind::Usage,
                "expected address and output path",
            ));
        };
        let address: SocketAddr = address.parse().map_err(|_| {
            BuildError::new(BuildErrorKind::Configuration, "invalid socket address")
        })?;
        if !address.ip().is_loopback() {
            return Err(BuildError::new(
                BuildErrorKind::Configuration,
                "loopback required",
            ));
        }
        Ok(assemble(address, PathBuf::from(output)))
    }
}

fn assemble(address: SocketAddr, output: PathBuf) -> App {
    // The bound address is learned while the UDP service runs, not returned by
    // an acquirer, so it travels in a one-shot rather than a `Resource`.
    let (bound, received) = oneshot::channel();
    let mut udp = udp::UdpService::new(address, |packet, peer, socket, _| async move {
        socket.send_to(&packet, peer).await?;
        Ok(())
    });
    udp.bound = Some(bound);
    let mut app = App::new();
    app.service("udp", udp);
    app.service_fn(
        Spec::new("publish-address").after("udp").finite(),
        move |ctx| async move {
            let address = received.await?;
            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&output)?;
            // Registered right after the file exists, before anything can fail.
            ctx.defer("remove address file", async move {
                std::fs::remove_file(output)?;
                Ok::<(), BoxError>(())
            })?;
            write!(file, "{address}")?;
            Ok(())
        },
    );
    app
}

#[allow(unsafe_code)]
mod exports {
    miju_services::jni::export!(class = "com/example/MijuBridge", app = super::UdpApp);
}
