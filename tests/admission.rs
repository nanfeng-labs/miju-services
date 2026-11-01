//! Public admission ordering without private hooks or scheduler timing assumptions.

use miju_services::{
    AppObserver, Application, BoxError, DrainObservation, Health, ServiceContext, ServiceSpec,
    SpawnError, WorkCompletion, WorkOutcome, WorkScope, WorkSpec, service_fn,
};
use std::{
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::oneshot;

async fn bounded<F: Future>(future: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(3), future)
        .await
        .expect("admission test timed out")
}

struct Released(Arc<AtomicUsize>);

impl Drop for Released {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn accepted_before_stop_remains_owned_until_capture_release() {
    let (send_scope, receive_scope) = oneshot::channel();
    let (root_stopped, root_stop) = oneshot::channel();
    let app = Application::builder()
        .service(
            ServiceSpec::new("owner"),
            service_fn(move |context: ServiceContext| async move {
                context.set_health(Health::Healthy)?;
                context.ready()?;
                send_scope.send(context.work().clone()).unwrap();
                context.stopping().await;
                root_stopped.send(()).unwrap();
                Ok::<(), BoxError>(())
            }),
        )
        .build()
        .unwrap();
    let control = app.start().unwrap();
    let observer = control.observer();
    let scope = bounded(receive_scope).await.unwrap();
    let released = Arc::new(AtomicUsize::new(0));
    let capture = Released(released.clone());
    let (started, start) = oneshot::channel();
    let (stopping, stop) = oneshot::channel();
    let (release, wait_release) = oneshot::channel();
    let work = scope
        .spawn(WorkSpec::new("accepted"), move |work| async move {
            let _capture = capture;
            started.send(()).unwrap();
            work.stopping().await;
            stopping.send(()).unwrap();
            wait_release.await?;
            Ok::<(), BoxError>(())
        })
        .unwrap();
    // Successful spawn and observed entry both precede the stop request.
    bounded(start).await.unwrap();
    control.stop();
    bounded(stop).await.unwrap();
    bounded(root_stop).await.unwrap();
    assert_eq!(released.load(Ordering::SeqCst), 0);
    assert!(matches!(
        observer.drain(Duration::ZERO).await,
        DrainObservation::Pending(_)
    ));
    release.send(()).unwrap();
    let result = bounded(work.wait()).await;
    assert_eq!(released.load(Ordering::SeqCst), 1);
    assert_eq!(result.completion(), WorkCompletion::Confirmed);
    assert!(matches!(result.outcome(), WorkOutcome::Succeeded));
    let exit = bounded(observer.wait()).await.unwrap();
    let owner = &exit.services[0];
    assert!(owner.work.active.is_empty());
    assert_eq!(owner.work.completed, 1);
    assert_eq!(owner.work.recent.len(), 1);
    assert_eq!(owner.work.recent[0].id(), work.id());
    assert!(exit.is_success());
}

struct ReentrantRejected {
    scope: WorkScope,
    observer: AppObserver,
    released: Arc<AtomicUsize>,
}

impl Drop for ReentrantRejected {
    fn drop(&mut self) {
        // Both calls would deadlock if rejected user captures were dropped under the gate.
        self.scope.close();
        let _snapshot = self.observer.snapshot();
        self.released.fetch_add(1, Ordering::SeqCst);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stop_stage_before_submit_rejects_and_releases_reentrant_capture() {
    let (send_context, receive_context) = oneshot::channel();
    let (stopped, stop) = oneshot::channel();
    let (release_root, root_release) = oneshot::channel();
    let app = Application::builder()
        .service(
            ServiceSpec::new("owner"),
            service_fn(move |context: ServiceContext| async move {
                context.set_health(Health::Healthy)?;
                context.ready()?;
                send_context.send(context.clone()).unwrap();
                context.stopping().await;
                stopped.send(()).unwrap();
                root_release.await?;
                Ok::<(), BoxError>(())
            }),
        )
        .build()
        .unwrap();
    let control = app.start().unwrap();
    let observer = control.observer();
    let context = bounded(receive_context).await.unwrap();
    control.stop();
    // This acknowledgment observes the owner's stop stage, rather than only global stop intent.
    bounded(stop).await.unwrap();
    assert!(context.is_stopping());
    let released = Arc::new(AtomicUsize::new(0));
    let invoked = Arc::new(AtomicUsize::new(0));
    let capture = ReentrantRejected {
        scope: context.work().clone(),
        observer: observer.clone(),
        released: released.clone(),
    };
    let entry_invoked = invoked.clone();
    let scope = context.work().clone();
    let submission = tokio::task::spawn_blocking(move || {
        scope.spawn(WorkSpec::new("late"), move |_| {
            entry_invoked.fetch_add(1, Ordering::SeqCst);
            async move {
                drop(capture);
                Ok::<(), BoxError>(())
            }
        })
    });
    let rejected = bounded(submission).await.unwrap();
    assert!(matches!(rejected, Err(SpawnError::Closed)));
    assert_eq!(invoked.load(Ordering::SeqCst), 0);
    assert_eq!(released.load(Ordering::SeqCst), 1);
    release_root.send(()).unwrap();
    let exit = bounded(observer.wait()).await.unwrap();
    assert_eq!(exit.services[0].work.completed, 0);
    assert!(exit.services[0].work.active.is_empty());
    assert!(exit.services[0].work.recent.is_empty());
    assert!(exit.is_success());
}
