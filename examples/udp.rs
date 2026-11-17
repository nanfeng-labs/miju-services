//! A bounded UDP echo service, with no application-owned task handles.

#[path = "support/udp.rs"]
mod udp;

use miju_services::{App, Result, Spec};
use udp::UdpService;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result {
    let service = UdpService::new(
        "127.0.0.1:4000".parse()?,
        |packet, peer, socket, _ctx| async move {
            socket.send_to(&packet, peer).await?;
            Ok(())
        },
    );
    let mut app = App::new();
    // At most 64 datagrams in flight; more are dropped, not queued.
    app.service(Spec::new("udp").capacity(64.try_into()?), service);
    let report = app.run_until(tokio::signal::ctrl_c()).await?;
    println!("{report}");
    Ok(())
}
