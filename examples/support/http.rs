//! Example-local Axum HTTP/1 service whose release barrier is two leases.
//!
//! - **One lease per connection.** axum runs each connection in a task of its
//!   own, with its own clone of the router, and tells `serve` the connection
//!   has closed before it drops that clone. If the service root is aborted at
//!   grace expiry, a connection task can hold the last clone of the router and
//!   its state; only this lease covers it.
//! - **One lease per request, moved into the response body.** It bounds
//!   admission (a request that cannot get one is answered 503 and the handler
//!   does not run), covers the handler, and travels with the body, which hyper
//!   may still be streaming, or dropping after a disconnect, after the
//!   connection's service is gone.
//!
//! Tasks a handler detaches itself and clones kept elsewhere remain the
//! application's responsibility. This is example code, not library API.

use axum::{
    Router,
    body::{Body, Bytes},
    extract::Request,
    http::StatusCode,
    response::{IntoResponse, Response},
    serve::{IncomingStream, Listener},
};
use http_body::{Body as HttpBody, Frame, SizeHint};
use miju_services::{BoxError, Context, Lease, Scope, Service};
use std::{
    convert::Infallible,
    fmt,
    future::{Future, Ready, ready},
    io,
    net::SocketAddr,
    pin::Pin,
    sync::Arc,
    task::{Context as TaskContext, Poll},
};
use tokio::{net::TcpListener, sync::oneshot};
use tower::Service as TowerService;

/// Bind, report ready, serve until stop, and return when axum returns.
pub(crate) struct HttpService {
    address: SocketAddr,
    router: Router,
    bound: Option<oneshot::Sender<SocketAddr>>,
}

impl HttpService {
    pub(crate) fn new(address: SocketAddr, router: Router) -> Self {
        Self {
            address,
            router,
            bound: None,
        }
    }

    /// Report the bound address, for callers that bind port 0.
    #[allow(dead_code)]
    pub(crate) fn report_bound(mut self, bound: oneshot::Sender<SocketAddr>) -> Self {
        self.bound = Some(bound);
        self
    }
}

impl Service for HttpService {
    type Error = BoxError;

    async fn run(self, ctx: Context) -> Result<(), BoxError> {
        let listener = TcpListener::bind(self.address).await?;
        let address = listener.local_addr()?;
        if let Some(bound) = self.bound {
            let _ = bound.send(address);
        }
        // Ready only after the real initialization: the bind.
        ctx.ready();
        serve(&ctx, listener, self.router).await?;
        Ok(())
    }
}

/// Serve `router` on any listener axum accepts, under `ctx`'s scope and stop signal.
pub(crate) async fn serve<L>(ctx: &Context, listener: L, router: Router) -> io::Result<()>
where
    L: Listener,
    L::Addr: fmt::Debug,
{
    axum::serve(
        listener,
        Connections {
            router,
            scope: ctx.scope(),
        },
    )
    .with_graceful_shutdown(ctx.stop_token().cancelled_owned())
    .await
}

/// The make-service: one `Connection` per accepted connection.
struct Connections {
    router: Router,
    scope: Scope,
}

impl<L: Listener> TowerService<IncomingStream<'_, L>> for Connections {
    type Response = Connection;
    type Error = Infallible;
    type Future = Ready<Result<Connection, Infallible>>;

    fn poll_ready(&mut self, _: &mut TaskContext<'_>) -> Poll<Result<(), Infallible>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _: IncomingStream<'_, L>) -> Self::Future {
        ready(Ok(match self.scope.lease("http-connection") {
            Ok(lease) => Connection::Admitted {
                router: self.router.clone(),
                scope: self.scope.clone(),
                _connection: Arc::new(lease),
            },
            Err(_) => Connection::Refused,
        }))
    }
}

/// One connection's service. axum clones it once per request; every clone
/// shares the connection's lease.
#[derive(Clone)]
enum Connection {
    Admitted {
        // Field order is drop order: the router and its state go first, the
        // connection's lease last.
        router: Router,
        scope: Scope,
        _connection: Arc<Lease>,
    },
    /// The scope was closed or full when the connection arrived; every
    /// request is answered 503 and no router clone is held.
    Refused,
}

type ResponseFuture = Pin<Box<dyn Future<Output = Result<Response, Infallible>> + Send>>;

impl TowerService<Request> for Connection {
    type Response = Response;
    type Error = Infallible;
    type Future = ResponseFuture;

    fn poll_ready(&mut self, _: &mut TaskContext<'_>) -> Poll<Result<(), Infallible>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: Request) -> ResponseFuture {
        let Self::Admitted { router, scope, .. } = self else {
            return Box::pin(ready(Ok(unavailable())));
        };
        // Admission before the handler runs and before the router is cloned.
        let Ok(lease) = scope.lease("http-request") else {
            return Box::pin(ready(Ok(unavailable())));
        };
        let router = router.clone();
        Box::pin(async move {
            // Declared in this order so that, if the connection drops this
            // future, the handler and the router clone go before the lease.
            let lease = lease;
            let mut router = router;
            let response = router.call(request).await?;
            let (parts, body) = response.into_parts();
            // The lease moves on: from the handler to the body it produced.
            let body = LeasedBody {
                body,
                _lease: lease,
            };
            Ok(Response::from_parts(parts, Body::new(body)))
        })
    }
}

fn unavailable() -> Response {
    StatusCode::SERVICE_UNAVAILABLE.into_response()
}

/// A response body that keeps its request's lease until it is dropped.
struct LeasedBody {
    // Field order is drop order: the body first, then the lease.
    body: Body,
    _lease: Lease,
}

impl HttpBody for LeasedBody {
    type Data = Bytes;
    type Error = axum::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, axum::Error>>> {
        Pin::new(&mut self.get_mut().body).poll_frame(cx)
    }

    fn is_end_stream(&self) -> bool {
        self.body.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.body.size_hint()
    }
}
