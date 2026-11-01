//! Real calendar backend via the product worker bridge, including finite EOF.
#![cfg(feature = "miju")]
use miju_core::worker::{builder::WorkerBuilder, context::WorkerContext};
use miju_cron::{CronScheduler, Schedule, Tick, timezone::Utc};
use miju_services::{Application, Lifetime, ServiceSpec, miju::worker};
use std::{
    collections::VecDeque,
    str::FromStr,
    sync::{Arc, Mutex},
    time::Duration,
};

struct Finite(VecDeque<u64>);

impl Schedule<Utc> for Finite {
    fn next_tick(&mut self, _: &Utc) -> Option<Tick> {
        self.0.pop_front().map(Tick::new)
    }
}

#[tokio::test]
async fn finite_calendar_source_finishes_without_stop_or_intermediate_queue() {
    let values = Arc::new(Mutex::new(Vec::new()));
    let record = values.clone();
    let context = WorkerContext::new("finite");
    let jobs = WorkerBuilder::new(context.clone())
        .backend(CronScheduler::new(Finite(VecDeque::from([1, 2]))))
        .build(move |tick: Tick| {
            let record = record.clone();
            async move {
                record.lock().unwrap().push(tick.get_timestamp());
            }
        });
    let control = Application::builder()
        .service(
            ServiceSpec::new("calendar").lifetime(Lifetime::Finite),
            worker(context, jobs.stream()),
        )
        .build()
        .unwrap()
        .start()
        .unwrap();
    let exit = tokio::time::timeout(Duration::from_secs(2), control.observer().wait())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(*values.lock().unwrap(), [1, 2]);
    assert!(exit.is_success());
    assert!(!exit.services[0].backend_cleanup_unconfirmed);
}

#[tokio::test]
async fn real_calendar_expression_ticks_and_stops() {
    let schedule = cron::Schedule::from_str("*/1 * * * * *").unwrap();
    let context = WorkerContext::new("calendar");
    let (send, mut receive) = tokio::sync::mpsc::unbounded_channel();
    let jobs = WorkerBuilder::new(context.clone())
        .backend(CronScheduler::new(schedule))
        .build(move |tick: Tick| {
            let send = send.clone();
            async move {
                send.send(tick.get_timestamp()).unwrap();
            }
        });
    let control = Application::builder()
        .service(ServiceSpec::new("calendar"), worker(context, jobs.stream()))
        .build()
        .unwrap()
        .start()
        .unwrap();
    control
        .observer()
        .ready(Duration::from_secs(2))
        .await
        .unwrap();
    let first = tokio::time::timeout(Duration::from_secs(3), receive.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(first > 0);
    control.stop();
    let exit = tokio::time::timeout(Duration::from_secs(2), control.observer().wait())
        .await
        .unwrap()
        .unwrap();
    assert!(exit.is_success());
}
