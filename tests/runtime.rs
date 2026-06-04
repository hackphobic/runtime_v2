// Copyright 2026 runtime contributors
// SPDX-License-Identifier: Apache-2.0

use std::{
    convert::Infallible,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use runtime::{
    DynServiceError, RuntimeBuilder, RuntimeError, Service, ServiceContext, ShutdownReason,
};

// ============ helpers ============

/// Spin-wait for a counter to reach `target` or `deadline`, whichever first.
async fn wait_for(counter: &AtomicUsize, target: usize, deadline: Duration) -> bool {
    let end = tokio::time::Instant::now() + deadline;
    while counter.load(Ordering::SeqCst) < target {
        if tokio::time::Instant::now() >= end {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    true
}

/// State threaded into each test service so we can observe start/stop.
struct Marker {
    starts: Arc<AtomicUsize>,
    stops: Arc<AtomicUsize>,
}

impl Marker {
    fn new(starts: Arc<AtomicUsize>, stops: Arc<AtomicUsize>) -> Self {
        Self { starts, stops }
    }
}

macro_rules! marker_service {
    ($ty:ident, $name:literal) => {
        struct $ty(Marker);
        impl Service for $ty {
            const NAME: &'static str = $name;
            type Error = Infallible;
            fn run(
                self: Arc<Self>,
                ctx: ServiceContext,
            ) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send {
                async move {
                    self.0.starts.fetch_add(1, Ordering::SeqCst);
                    ctx.mark_ready();
                    ctx.cancelled().await;
                    self.0.stops.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
            }
        }
    };
}

marker_service!(Producer, "producer");
marker_service!(Consumer, "consumer");

// ============ startup + clean shutdown ============

#[tokio::test]
async fn two_services_start_and_stop_cleanly() {
    let starts = Arc::new(AtomicUsize::new(0));
    let stops = Arc::new(AtomicUsize::new(0));

    let producer = Arc::new(Producer(Marker::new(starts.clone(), stops.clone())));
    let consumer = Arc::new(Consumer(Marker::new(starts.clone(), stops.clone())));

    let rt = RuntimeBuilder::new()
        .service(producer)
        .unwrap()
        .service_with_deps(consumer, &["producer"])
        .unwrap()
        .shutdown_timeout(Duration::from_secs(2))
        .build();

    let handle = rt.handle();
    let runner = tokio::spawn(rt.run());

    assert!(
        wait_for(&starts, 2, Duration::from_secs(1)).await,
        "both services must start",
    );
    assert!(!handle.is_shutting_down());

    handle.request_shutdown(ShutdownReason::Requested);
    runner.await.unwrap().unwrap();

    assert_eq!(stops.load(Ordering::SeqCst), 2, "both services must stop");
    assert_eq!(handle.shutdown_reason(), ShutdownReason::Requested);
    assert!(handle.is_shutting_down());
}

#[tokio::test]
async fn run_with_ctrl_c_obeys_request_shutdown() {
    // We can't easily synthesize a Ctrl-C in a unit test, but we can verify the
    // run_with_ctrl_c path still obeys explicit request_shutdown calls.
    let starts = Arc::new(AtomicUsize::new(0));
    let stops = Arc::new(AtomicUsize::new(0));

    let producer = Arc::new(Producer(Marker::new(starts.clone(), stops.clone())));

    let rt = RuntimeBuilder::new()
        .service(producer)
        .unwrap()
        .shutdown_timeout(Duration::from_secs(1))
        .build();

    let handle = rt.handle();
    let runner = tokio::spawn(rt.run_with_ctrl_c());

    assert!(wait_for(&starts, 1, Duration::from_secs(1)).await);
    handle.request_shutdown(ShutdownReason::Requested);
    runner.await.unwrap().unwrap();
    assert_eq!(stops.load(Ordering::SeqCst), 1);
}

// ============ failure propagation ============

#[derive(thiserror::Error, Debug)]
#[error("boom")]
struct Boom;

struct Faulty;
impl Service for Faulty {
    const NAME: &'static str = "faulty";
    type Error = Boom;
    fn run(
        self: Arc<Self>,
        _ctx: ServiceContext,
    ) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send {
        async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            Err(Boom)
        }
    }
}

#[tokio::test]
async fn service_failure_triggers_runtime_shutdown_with_typed_error() {
    let starts = Arc::new(AtomicUsize::new(0));
    let stops = Arc::new(AtomicUsize::new(0));

    let bystander = Arc::new(Producer(Marker::new(starts.clone(), stops.clone())));

    let rt = RuntimeBuilder::new()
        .service(bystander)
        .unwrap()
        .service(Arc::new(Faulty))
        .unwrap()
        .shutdown_timeout(Duration::from_secs(2))
        .build();

    let handle = rt.handle();
    let err = rt.run().await.unwrap_err();

    match err {
        RuntimeError::ServiceFailed(dyn_err) => {
            let DynServiceError { service, .. } = &dyn_err;
            assert_eq!(*service, "faulty");
            assert!(
                dyn_err.downcast_ref::<Boom>().is_some(),
                "typed error must be recoverable via downcast",
            );
        }
        other => panic!("expected ServiceFailed, got {other:?}"),
    }

    assert_eq!(handle.shutdown_reason(), ShutdownReason::ServiceFailed);
    // The bystander started and was then cancelled cleanly.
    assert_eq!(stops.load(Ordering::SeqCst), 1);
}

// ============ shutdown timeout ============

#[derive(thiserror::Error, Debug)]
#[error("ignored cancel")]
struct IgnoredCancel;

/// A service that ignores cancellation entirely.
struct Stubborn;
impl Service for Stubborn {
    const NAME: &'static str = "stubborn";
    type Error = IgnoredCancel;
    fn run(
        self: Arc<Self>,
        _ctx: ServiceContext,
    ) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send {
        async move {
            tokio::time::sleep(Duration::from_secs(300)).await;
            Ok(())
        }
    }
}

#[tokio::test]
async fn drain_timeout_is_enforced() {
    let rt = RuntimeBuilder::new()
        .service(Arc::new(Stubborn))
        .unwrap()
        .shutdown_timeout(Duration::from_millis(100))
        .build();

    let handle = rt.handle();
    let runner = tokio::spawn(rt.run());
    tokio::time::sleep(Duration::from_millis(20)).await;
    handle.request_shutdown(ShutdownReason::Requested);

    let err = runner.await.unwrap().unwrap_err();
    assert!(
        matches!(err, RuntimeError::ShutdownTimeout(_)),
        "expected ShutdownTimeout, got {err:?}",
    );
}

// ============ empty runtime ============

#[tokio::test]
async fn empty_runtime_runs_to_completion() {
    // Zero services → supervision loop sees joinset.join_next() == None → Ok(()).
    let rt = RuntimeBuilder::new()
        .shutdown_timeout(Duration::from_secs(1))
        .build();
    rt.run().await.unwrap();
}
