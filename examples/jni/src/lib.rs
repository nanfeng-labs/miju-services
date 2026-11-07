//! Application-owned settings; App owns the admitted services and their cleanup.

#[path = "../../support/udp.rs"]
mod udp;

use miju_services::{
    App, BoxError,
    jni::{AppFactory, BuildError, BuildErrorKind},
    service::{Lifetime, ServiceSpec},
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
        assemble(address, PathBuf::from(output))
            .map_err(|_| BuildError::new(BuildErrorKind::Startup, "assembly failed"))
    }
}

fn assemble(address: SocketAddr, output: PathBuf) -> Result<App, miju_services::BuildError> {
    let (bound, received) = oneshot::channel();
    let mut udp = udp::UdpService::new(address, |packet, peer, socket, _| async move {
        socket.send_to(&packet, peer).await?;
        Ok(())
    });
    udp.bound = Some(bound);
    App::builder()
        .service("udp", udp)
        .service_fn(
            ServiceSpec::new("publish-address")
                .depends_on("udp")
                .lifetime(Lifetime::Finite),
            move |context| async move {
                let address = received.await?;
                let mut file = std::fs::OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(&output)?;
                let close = move || async move {
                    std::fs::remove_file(output)?;
                    Ok::<(), BoxError>(())
                };
                if let Err(rejected) = context.defer_cleanup("remove address file", close) {
                    rejected.into_inner()().await?;
                    return Err(std::io::Error::other("cleanup registration closed").into());
                }
                write!(file, "{address}")?;
                Ok(())
            },
        )
        .build()
}

#[allow(unsafe_code)]
mod exports {
    miju_services::jni::export!(class = "com/example/MijuBridge", app = super::UdpApp);
}
