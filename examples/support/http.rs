//! Private Axum HTTP/1 completion witness shared by the HTTP and WS examples.
//!
//! Pass the final Router, outside all business layers, with no extra template kept
//! by the owner. Business resources belong in tracked Router/Future/Body values,
//! not untracked extensions, owner-backed byte buffers, or detached tasks. Upgraded
//! WebSockets require separate managed work; this HTTP witness cannot drain them.

use axum::{
    Router, ServiceExt,
    body::{Body, Bytes, HttpBody},
    http::Request,
    response::Response,
};
use miju_services::WorkContext;
use std::{
    convert::Infallible,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::net::TcpListener;
use tokio_util::task::{TaskTracker, task_tracker::TaskTrackerToken};
use tower::Service;

/// Run an already-admitted aggregate until serve and all tracked user values end.
///
/// Errors propagate without claiming external completion. The aggregate caller
/// therefore retains Unconfirmed responsibility when this function fails/unwinds.
pub(crate) async fn serve_http(
    listener: TcpListener,
    final_router: Router,
    work: WorkContext,
) -> std::io::Result<()> {
    let tracker = TaskTracker::new();
    {
        let service = TrackedRouter::new(final_router, tracker.clone());
        axum::serve(listener, service.into_make_service())
            .with_graceful_shutdown(async move { work.stopping().await })
            .await?;
    }
    tracker.close();
    tracker.wait().await;
    Ok(())
}

struct TrackedRouter {
    // Rust drops fields in declaration order: user state before its lease.
    inner: Router,
    tracker: TaskTracker,
    _lease: TaskTrackerToken,
}

impl TrackedRouter {
    fn new(inner: Router, tracker: TaskTracker) -> Self {
        Self {
            inner,
            _lease: tracker.token(),
            tracker,
        }
    }
}

impl Clone for TrackedRouter {
    fn clone(&self) -> Self {
        Self::new(self.inner.clone(), self.tracker.clone())
    }
}

impl Service<Request<Body>> for TrackedRouter {
    type Response = Response;
    type Error = Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Response, Infallible>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        <Router as Service<Request<Body>>>::poll_ready(&mut self.inner, cx)
    }

    fn call(&mut self, request: Request<Body>) -> Self::Future {
        let mut inner = self.inner.clone();
        let tracker = self.tracker.clone();
        Box::pin(self.tracker.track_future(async move {
            let response = inner.call(request).await?;
            let (parts, body) = response.into_parts();
            // Transfer responsibility without a momentary empty tracker.
            let body = TrackedBody {
                inner: body,
                _lease: tracker.token(),
            };
            Ok(Response::from_parts(parts, Body::new(body)))
        }))
    }
}

struct TrackedBody {
    inner: Body,
    _lease: TaskTrackerToken,
}

impl HttpBody for TrackedBody {
    type Data = Bytes;
    type Error = axum::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Bytes>, Self::Error>>> {
        Pin::new(&mut self.get_mut().inner).poll_frame(cx)
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> http_body::SizeHint {
        self.inner.size_hint()
    }
}
