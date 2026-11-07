//! Example-local Axum HTTP/1 service and actual user-value release barrier.
//!
//! Supply the final Router without retaining another template. Requests, bodies
//! and router state are tracked; detached tasks and externally retained clones
//! remain the application's responsibility. Upgrades need their own WorkScope.

use axum::{
    Router, ServiceExt,
    body::{Body, Bytes, HttpBody},
    http::Request,
    response::Response,
};
use miju_services::{
    BoxError, Service, ServiceContext,
    work::{WorkContext, WorkSpec},
};
use std::{
    convert::Infallible,
    future::Future,
    net::SocketAddr,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::{net::TcpListener, sync::oneshot};
use tokio_util::task::{TaskTracker, task_tracker::TaskTrackerToken};
use tower::Service as TowerService;

pub(crate) struct HttpService {
    address: SocketAddr,
    router: Router,
    pub(crate) bound: Option<oneshot::Sender<SocketAddr>>,
    pub(crate) users: TaskTracker,
}

impl HttpService {
    pub(crate) fn new(address: SocketAddr, router: Router) -> Self {
        Self {
            address,
            router,
            bound: None,
            users: TaskTracker::new(),
        }
    }
}

impl Service for HttpService {
    type Error = BoxError;

    async fn run(self, context: ServiceContext) -> Result<(), BoxError> {
        let listener = TcpListener::bind(self.address).await?;
        let address = listener.local_addr()?;
        let handle = context
            .work()
            .spawn(WorkSpec::aggregate("http"), move |work| {
                serve(listener, self.router, self.users, work)
            })?;
        if let Some(bound) = self.bound {
            let _ = bound.send(address);
        }
        context.ready();
        // Even a failed waiter cannot release the scope's aggregate responsibility.
        handle.wait().await;
        Ok(())
    }
}

async fn serve(
    listener: TcpListener,
    router: Router,
    users: TaskTracker,
    work: WorkContext,
) -> std::io::Result<()> {
    {
        let router = OwnedRouter::new(router, users.clone());
        axum::serve(listener, router.into_make_service())
            .with_graceful_shutdown(async move { work.stopping().await })
            .await?;
    }
    users.close();
    users.wait().await;
    Ok(())
}

struct OwnedRouter {
    // Field drop order releases user state before its completion witness.
    router: Router,
    users: TaskTracker,
    _lease: TaskTrackerToken,
}

impl OwnedRouter {
    fn new(router: Router, users: TaskTracker) -> Self {
        Self {
            router,
            _lease: users.token(),
            users,
        }
    }
}

impl Clone for OwnedRouter {
    fn clone(&self) -> Self {
        Self::new(self.router.clone(), self.users.clone())
    }
}

impl TowerService<Request<Body>> for OwnedRouter {
    type Response = Response;
    type Error = Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Response, Infallible>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        <Router as TowerService<Request<Body>>>::poll_ready(&mut self.router, cx)
    }

    fn call(&mut self, request: Request<Body>) -> Self::Future {
        let mut router = self.router.clone();
        let users = self.users.clone();
        Box::pin(self.users.track_future(async move {
            let response = router.call(request).await?;
            let (parts, body) = response.into_parts();
            // Transfer the lease before the request future can release its own.
            let body = OwnedBody {
                body,
                _lease: users.token(),
            };
            Ok(Response::from_parts(parts, Body::new(body)))
        }))
    }
}

struct OwnedBody {
    body: Body,
    _lease: TaskTrackerToken,
}

impl HttpBody for OwnedBody {
    type Data = Bytes;
    type Error = axum::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Bytes>, Self::Error>>> {
        Pin::new(&mut self.get_mut().body).poll_frame(cx)
    }

    fn is_end_stream(&self) -> bool {
        self.body.is_end_stream()
    }

    fn size_hint(&self) -> http_body::SizeHint {
        self.body.size_hint()
    }
}
