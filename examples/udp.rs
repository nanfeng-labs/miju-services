//! A bounded UDP echo service, with no application-owned task handles.

#[path = "support/udp.rs"]
mod udp;

use miju_services::{App, BoxError, service::ServiceSpec};
use udp::UdpService;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), BoxError> {
    let service = UdpService::new(
        "127.0.0.1:4000".parse()?,
        |packet, peer, socket, _| async move {
            socket.send_to(&packet, peer).await?;
            Ok(())
        },
    );
    App::builder()
        .service(ServiceSpec::new("udp").max_work(64.try_into()?), service)
        .build()?
        .run_until(tokio::signal::ctrl_c())
        .await?;
    Ok(())
}
