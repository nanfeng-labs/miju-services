//! A JVM-independent owner for one application generation at a time.
#![forbid(unsafe_code)]

use super::{App, BuildError, BuildErrorKind};
use crate::{AppHandle, AppObserver, Application, ExitReport, Phase, ReadyError};
use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{Arc, Condvar, Mutex},
    thread,
    time::{Duration, Instant},
};
use tokio::runtime::Runtime;
use tokio_util::sync::CancellationToken;

const IDLE: i32 = 0;
const STARTING: i32 = 1;
const RUNNING: i32 = 2;
const STOPPING: i32 = 3;
const FAILED: i32 = 5;
const ALREADY_RUNNING: i32 = 1;
const NOT_RUNNING: i32 = 2;
const BUSY: i32 = 3;
const CANCELLED: i32 = 4;
const USAGE: i32 = -64;
const STARTUP_FAILED: i32 = -69;
const FAULT: i32 = -70;
const OS_ERROR: i32 = -71;
const CONFIG: i32 = -78;
const PANICKED: i32 = -99;
const STOP_TIMED_OUT: i32 = -124;

type Factory = fn(Vec<String>) -> Result<Application, BuildError>;

pub(super) struct Bridge {
    slot: Mutex<Option<Arc<Generation>>>,
    factory: Factory,
    timeout: Duration,
    version: String,
}

// This metadata deliberately cannot own a core handle, report, error or closure.
struct Generation {
    state: Mutex<State>,
    changed: Condvar,
    stop: CancellationToken,
    deadline: Instant,
}

struct State {
    phase: i32,
    first: Option<i32>,
    terminal: Option<i32>,
    failure: Option<i32>,
    isolated: Option<i32>,
    reusable: bool,
    stopping: bool,
    error: String,
}

impl Bridge {
    pub(super) fn new<A: App>() -> Self {
        Self {
            slot: Mutex::new(None),
            factory: A::build,
            timeout: A::START_TIMEOUT,
            version: format!("{} {}", A::NAME, A::VERSION),
        }
    }

    pub(super) fn start(&self, args: Vec<String>) -> i32 {
        if self.timeout.is_zero() || Instant::now().checked_add(self.timeout).is_none() {
            return USAGE;
        }
        let generation = match self.reserve() {
            Ok(generation) => generation,
            Err(code) => return code,
        };
        let owner = generation.clone();
        let factory = self.factory;
        match catch_unwind(AssertUnwindSafe(|| {
            thread::Builder::new()
                .name("miju-jni-owner".into())
                .spawn(move || own_generation(owner, factory, args))
        })) {
            Ok(Ok(_thread)) => generation.wait_start(),
            Ok(Err(_error)) => {
                generation.record_failure(OS_ERROR, "owner thread creation failed");
                generation.finish();
                generation.wait_start()
            }
            Err(payload) => {
                // A panic payload may itself have a user-defined, panicking Drop.
                std::mem::forget(payload);
                generation.quarantine(PANICKED);
                PANICKED
            }
        }
    }

    fn reserve(&self) -> Result<Arc<Generation>, i32> {
        loop {
            let previous = self.slot.lock().map_err(|_| FAULT)?.clone();
            if let Some(old) = &previous {
                let state = old.state.lock().map_err(|_| FAULT)?;
                if !state.reusable {
                    return Err(
                        if !state.stopping && matches!(state.phase, STARTING | RUNNING) {
                            ALREADY_RUNNING
                        } else {
                            BUSY
                        },
                    );
                }
            }
            let mut slot = self.slot.lock().map_err(|_| FAULT)?;
            let unchanged = match (&*slot, &previous) {
                (None, None) => true,
                (Some(current), Some(old)) => Arc::ptr_eq(current, old),
                _ => false,
            };
            if !unchanged {
                continue;
            }
            let deadline = Instant::now().checked_add(self.timeout).ok_or(USAGE)?;
            let generation = Arc::new(Generation::new(deadline));
            let displaced = slot.replace(generation.clone());
            drop(slot);
            drop(displaced);
            return Ok(generation);
        }
    }

    pub(super) fn stop(&self, timeout_millis: i64) -> i32 {
        let within = Duration::from_millis(timeout_millis.max(0) as u64);
        let Some(deadline) = Instant::now().checked_add(within) else {
            return USAGE;
        };
        let generation = match self.slot.lock() {
            Ok(slot) => slot.clone(),
            Err(_) => return FAULT,
        };
        let Some(generation) = generation else {
            return NOT_RUNNING;
        };
        generation.stop_and_wait(deadline)
    }

    pub(super) fn status(&self) -> i32 {
        let generation = match self.slot.lock() {
            Ok(slot) => slot.clone(),
            Err(_) => return FAILED,
        };
        generation.map_or(IDLE, |generation| {
            generation.state.lock().map_or(FAILED, |state| state.phase)
        })
    }

    pub(super) fn last_error(&self) -> String {
        let generation = match self.slot.lock() {
            Ok(slot) => slot.clone(),
            Err(_) => return "JNI host state unavailable".into(),
        };
        generation.map_or_else(String::new, |generation| {
            generation.state.lock().map_or_else(
                |_| "JNI host state unavailable".into(),
                |state| state.error.clone(),
            )
        })
    }

    pub(super) fn version(&self) -> String {
        self.version.clone()
    }
}

impl Generation {
    fn new(deadline: Instant) -> Self {
        Self {
            state: Mutex::new(State {
                phase: STARTING,
                first: None,
                terminal: None,
                failure: None,
                isolated: None,
                reusable: false,
                stopping: false,
                error: String::new(),
            }),
            changed: Condvar::new(),
            stop: CancellationToken::new(),
            deadline,
        }
    }

    fn wait_start(&self) -> i32 {
        let mut state = match self.state.lock() {
            Ok(state) => state,
            Err(_) => return FAULT,
        };
        loop {
            if let Some(code) = state.first.or(state.isolated) {
                return code;
            }
            let now = Instant::now();
            if now >= self.deadline {
                drop(state);
                self.expire_start();
                state = match self.state.lock() {
                    Ok(state) => state,
                    Err(_) => return FAULT,
                };
                continue;
            }
            state = match self.changed.wait_timeout(state, self.deadline - now) {
                Ok((state, _)) => state,
                Err(_) => return FAULT,
            };
        }
    }

    fn stop_and_wait(&self, deadline: Instant) -> i32 {
        let mut state = match self.state.lock() {
            Ok(state) => state,
            Err(_) => return FAULT,
        };
        if state.reusable {
            return NOT_RUNNING;
        }
        if let Some(code) = state.isolated {
            return code;
        }
        let mut displaced = None;
        state.stopping = true;
        state.phase = STOPPING;
        if state.first.is_none() {
            if Instant::now() >= self.deadline {
                state.first = Some(STARTUP_FAILED);
                if state.failure.is_none() {
                    state.failure = Some(STARTUP_FAILED);
                    displaced = Some(std::mem::replace(
                        &mut state.error,
                        "startup observation timed out".into(),
                    ));
                }
            } else {
                state.first = Some(CANCELLED);
            }
        }
        drop(state);
        drop(displaced);
        self.stop.cancel();
        self.changed.notify_all();
        let mut state = match self.state.lock() {
            Ok(state) => state,
            Err(_) => return FAULT,
        };
        loop {
            if let Some(code) = state.terminal.or(state.isolated) {
                return code;
            }
            let now = Instant::now();
            if now >= deadline {
                let displaced = if state.failure.is_none() {
                    Some(std::mem::replace(
                        &mut state.error,
                        "stop observation timed out; cleanup continues".into(),
                    ))
                } else {
                    None
                };
                drop(state);
                drop(displaced);
                return STOP_TIMED_OUT;
            }
            state = match self.changed.wait_timeout(state, deadline - now) {
                Ok((state, _)) => state,
                Err(_) => return FAULT,
            };
        }
    }

    fn expire_start(&self) {
        let displaced = {
            let Ok(mut state) = self.state.lock() else {
                return;
            };
            if state.first.is_some() || Instant::now() < self.deadline {
                return;
            }
            state.first = Some(STARTUP_FAILED);
            state.stopping = true;
            state.phase = STOPPING;
            if state.failure.is_none() {
                state.failure = Some(STARTUP_FAILED);
                Some(std::mem::replace(
                    &mut state.error,
                    "startup observation timed out".into(),
                ))
            } else {
                None
            }
        };
        drop(displaced);
        self.stop.cancel();
        self.changed.notify_all();
    }

    fn available(&self) -> Result<(), ()> {
        let mut state = self.state.lock().map_err(|_| ())?;
        if state.first.is_some() || state.stopping || state.phase != STARTING {
            return Ok(());
        }
        if Instant::now() >= self.deadline {
            drop(state);
            self.expire_start();
        } else {
            state.first = Some(0);
            state.phase = RUNNING;
            drop(state);
            self.changed.notify_all();
        }
        Ok(())
    }

    fn record_failure(&self, code: i32, safe_message: &str) {
        let message = bounded_message(safe_message);
        let displaced = {
            let Ok(mut state) = self.state.lock() else {
                return;
            };
            if state.failure.is_some() {
                return;
            }
            state.failure = Some(code);
            state.first.get_or_insert(code);
            state.phase = STOPPING;
            state.stopping = true;
            std::mem::replace(&mut state.error, message)
        };
        drop(displaced);
        self.stop.cancel();
        self.changed.notify_all();
    }

    fn stopping(&self) -> Result<bool, ()> {
        self.state
            .lock()
            .map(|state| state.stopping)
            .map_err(|_| ())
    }

    fn observe_stopping(&self) -> Result<(), ()> {
        let mut state = self.state.lock().map_err(|_| ())?;
        if !state.reusable && state.isolated.is_none() {
            state.phase = STOPPING;
        }
        Ok(())
    }

    fn finish(&self) {
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        if state.isolated.is_some() {
            return;
        }
        if state.first.is_none() && Instant::now() >= self.deadline {
            drop(state);
            self.expire_start();
            state = match self.state.lock() {
                Ok(state) => state,
                Err(_) => return,
            };
        }
        let code = state.failure.unwrap_or(0);
        state.first.get_or_insert(code);
        state.terminal = Some(code);
        state.phase = if code == 0 { IDLE } else { FAILED };
        state.reusable = true;
        drop(state);
        self.changed.notify_all();
    }

    fn quarantine(&self, code: i32) {
        let displaced = {
            let Ok(mut state) = self.state.lock() else {
                self.changed.notify_all();
                return;
            };
            state.isolated = Some(code);
            state.first.get_or_insert(code);
            state.phase = FAILED;
            state.stopping = true;
            state.reusable = false;
            std::mem::replace(&mut state.error, "JNI host completion unconfirmed".into())
        };
        drop(displaced);
        self.changed.notify_all();
    }
}

fn bounded_message(message: &str) -> String {
    let mut end = message.len().min(4096);
    while !message.is_char_boundary(end) {
        end -= 1;
    }
    message[..end].to_owned()
}

#[derive(Default)]
struct Owned {
    runtime: Option<Runtime>,
    application: Option<Application>,
    control: Option<AppHandle>,
    observer: Option<AppObserver>,
    report: Option<Arc<ExitReport>>,
    possibly_started: bool,
}

fn own_generation(generation: Arc<Generation>, factory: Factory, args: Vec<String>) {
    // These slots outlive the unwind guard; losing orchestration must not drop a live runtime.
    let mut owned = Owned::default();
    let result = catch_unwind(AssertUnwindSafe(|| {
        owner_body(&generation, factory, args, &mut owned)
    }));
    let isolated = match result {
        Ok(Ok(())) => None,
        Ok(Err(())) => Some(FAULT),
        Err(payload) => {
            std::mem::forget(payload);
            if owned.possibly_started {
                Some(PANICKED)
            } else {
                generation.record_failure(PANICKED, "application factory panicked");
                None
            }
        }
    };
    if let Some(code) = isolated {
        generation.quarantine(code);
        retain_quarantined(owned);
    }
    let released = catch_unwind(AssertUnwindSafe(|| {
        drop(owned.application.take());
        drop(owned.control.take());
        drop(owned.observer.take());
        drop(owned.report.take());
    }));
    if let Err(payload) = released {
        std::mem::forget(payload);
        generation.quarantine(PANICKED);
        retain_quarantined(owned);
    }
    let runtime = owned.runtime.take();
    if let Err(payload) = catch_unwind(AssertUnwindSafe(|| drop(runtime))) {
        std::mem::forget(payload);
        generation.quarantine(PANICKED);
        retain_quarantined(owned);
    }
    // No user object, core observer, report or runtime survives this publication.
    generation.finish();
}

fn retain_quarantined(owned: Owned) -> ! {
    loop {
        std::hint::black_box(&owned);
        thread::park();
    }
}

fn owner_body(
    generation: &Generation,
    factory: Factory,
    args: Vec<String>,
    owned: &mut Owned,
) -> Result<(), ()> {
    owned.runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => Some(runtime),
        Err(_) => {
            generation.record_failure(OS_ERROR, "Tokio runtime creation failed");
            return Ok(());
        }
    };
    generation.expire_start();
    if generation.stopping()? {
        return Ok(());
    }
    let runtime = owned
        .runtime
        .as_ref()
        .expect("runtime assigned before factory");
    let built = {
        let _entered = runtime.enter();
        factory(args)
    };
    match built {
        Ok(application) => owned.application = Some(application),
        Err(error) => {
            let code = match error.kind() {
                BuildErrorKind::Usage => USAGE,
                BuildErrorKind::Configuration => CONFIG,
                BuildErrorKind::Startup => STARTUP_FAILED,
            };
            generation.record_failure(code, error.safe_message());
            return Ok(());
        }
    }
    generation.expire_start();
    if generation.stopping()? {
        return Ok(());
    }
    {
        let _entered = runtime.enter();
        owned.possibly_started = true;
        match owned
            .application
            .take()
            .expect("factory supplied application")
            .start()
        {
            Ok(control) => {
                owned.observer = Some(control.observer());
                owned.control = Some(control);
            }
            Err(error) => {
                owned.application = Some(error.into_application());
                owned.possibly_started = false;
                generation.record_failure(STARTUP_FAILED, "application could not start");
                return Ok(());
            }
        }
    }
    let observer = owned
        .observer
        .as_ref()
        .expect("started application has observer");
    let control = owned
        .control
        .as_ref()
        .expect("started application has control");
    let report = runtime.block_on(async {
        let ready = observer.ready(
            generation
                .deadline
                .saturating_duration_since(Instant::now()),
        );
        let wait = observer.wait();
        let deadline =
            tokio::time::sleep_until(tokio::time::Instant::from_std(generation.deadline));
        tokio::pin!(ready, wait, deadline);
        let mut snapshots = observer.subscribe();
        let mut observe_ready = true;
        let mut observe_deadline = true;
        let mut observe_snapshots = true;
        let mut delivered_stop = false;
        loop {
            tokio::select! {
                ready = &mut ready, if observe_ready => {
                    observe_ready = false;
                    match ready {
                        Ok(snapshot) => {
                            drop(snapshot);
                            generation.available()?;
                            observe_deadline = false;
                        }
                        Err(ReadyError::StoppedBeforeReady) => {}
                        Err(ReadyError::DriverLost(_)) => return Err(()),
                        Err(ReadyError::ServiceFailed(_)) => {
                            generation.record_failure(
                                STARTUP_FAILED,
                                "required service failed before availability"
                            );
                        }
                        Err(ReadyError::Timeout) => generation.expire_start(),
                    }
                }
                report = &mut wait => {
                    generation.observe_stopping()?;
                    return report.map_err(|_| ());
                }
                _ = generation.stop.cancelled(), if !delivered_stop => {
                    delivered_stop = true;
                    observe_deadline = false;
                    control.stop();
                    generation.observe_stopping()?;
                }
                _ = &mut deadline, if observe_deadline => {
                    observe_deadline = false;
                    generation.expire_start();
                }
                snapshot = snapshots.changed(), if observe_snapshots => {
                    match snapshot {
                        Some(snapshot)
                            if matches!(
                                snapshot.phase,
                                Phase::Stopping | Phase::Cleaning | Phase::Terminated
                            ) => generation.observe_stopping()?,
                        Some(_) => {}
                        None => observe_snapshots = false,
                    }
                }
            }
        }
    })?;
    // Keep the complete report in the outer slot until its guarded destruction.
    owned.report = Some(report);
    if !owned.report.as_ref().expect("report assigned").is_success() {
        let was_available = generation.state.lock().map_err(|_| ())?.first == Some(0);
        generation.record_failure(
            if was_available { FAULT } else { STARTUP_FAILED },
            "application lifecycle failed; inspect application diagnostics",
        );
    }
    Ok(())
}

#[cfg(test)]
#[path = "host_tests.rs"]
mod tests;
