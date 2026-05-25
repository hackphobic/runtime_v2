// Copyright 2026 runtime contributors
// SPDX-License-Identifier: Apache-2.0

use std::{
    convert::Infallible,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use runtime::{
    RuntimeBuilder, Service, ServiceContext, ShutdownReason,
};

// ============ helpers ============

async fn wait_for<F>(mut cond: F, deadline: Duration) -> bool
where
    F: FnMut() -> bool,
{
    let end = tokio::time::Instant::now() + deadline;
    while !cond() {
        if tokio::time::Instant::now() >= end {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    true
}

// ============ direct handle tests ============

#[tokio::test]
async fn handle_is_ready_false_before_mark() {
    struct S;
    impl Service for S {
        const NAME: &'static str = "s";
        type Error = Infallible;
        fn run(
            self: Arc<Self>,
            ctx: ServiceContext,
        ) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send {
            async move {
                ctx.cancelled().await;
                Ok(())
            }
        }
    }

    let rt = RuntimeBuilder::new().service(Arc::new(S)).unwrap().build();
    let handle = rt.handle();
    // Before run() starts, readiness slot exists but is false.
    assert!(!handle.is_ready("s"));
    // Explicit mark, even outside a service, works.
    handle.mark_ready("s");
    assert!(handle.is_ready("s"));
    drop(rt); // never ran; that's fine, we only tested the registry
}

#[tokio::test]
async fn await_ready_for_unknown_service_returns_immediately() {
    let rt = RuntimeBuilder::new()
        .service(Arc::new(NoOp))
        .unwrap()
        .build();
    let handle = rt.handle();
    // Should warn and return, not hang.
    tokio::time::timeout(Duration::from_millis(100), handle.await_ready("nonexistent"))
        .await
        .expect("await_ready must not hang on unknown name");
}

// ============ supervisor-driven readiness tests ============

/// A service whose `auto_ready` is overridden to false; it waits N ms before
/// marking itself ready, recording when ready was marked.
struct DelayedReady {
    delay: Duration,
    marked_ready: Arc<AtomicBool>,
}

impl Service for DelayedReady {
    const NAME: &'static str = "delayed";
    type Error = Infallible;

    fn auto_ready(&self) -> bool {
        false
    }

    fn run(
        self: Arc<Self>,
        ctx: ServiceContext,
    ) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send {
        async move {
            tokio::time::sleep(self.delay).await;
            self.marked_ready.store(true, Ordering::SeqCst);
            ctx.mark_ready();
            ctx.cancelled().await;
            Ok(())
        }
    }
}

/// A dependent service that records when it actually starts running.
struct Dependent {
    start_order: Arc<AtomicUsize>,
    start_index: Arc<AtomicUsize>,
}

impl Service for Dependent {
    const NAME: &'static str = "dependent";
    type Error = Infallible;

    fn run(
        self: Arc<Self>,
        ctx: ServiceContext,
    ) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send {
        async move {
            let n = self.start_order.fetch_add(1, Ordering::SeqCst);
            self.start_index.store(n, Ordering::SeqCst);
            ctx.cancelled().await;
            Ok(())
        }
    }
}

#[tokio::test]
async fn dependent_waits_until_dep_marks_ready() {
    let order = Arc::new(AtomicUsize::new(0));
    let dep_marked = Arc::new(AtomicBool::new(false));
    let dep_idx = Arc::new(AtomicUsize::new(usize::MAX));
    let dependent_idx = Arc::new(AtomicUsize::new(usize::MAX));

    // The dep takes ~150ms to "hydrate" before marking ready.
    let dep = Arc::new(DelayedReady {
        delay: Duration::from_millis(150),
        marked_ready: dep_marked.clone(),
    });
    // Wrap dep in an auto-ready outer to record start order too.
    // Actually we want to know when the dep itself started, vs when it marked
    // ready. So we just measure: at the time the dependent starts running,
    // dep_marked must already be true.
    let _ = dep_idx; // not used; we measure via dep_marked

    let dependent = Arc::new(Dependent {
        start_order: order.clone(),
        start_index: dependent_idx.clone(),
    });

    let rt = RuntimeBuilder::new()
        .service(dep)
        .unwrap()
        .service_with_deps(dependent, &["delayed"])
        .unwrap()
        .shutdown_timeout(Duration::from_secs(2))
        .build();

    let handle = rt.handle();
    let runner = tokio::spawn(rt.run());

    // Wait until the dependent has actually started running.
    assert!(
        wait_for(
            || dependent_idx.load(Ordering::SeqCst) != usize::MAX,
            Duration::from_secs(2),
        )
        .await,
        "dependent never started",
    );

    // Crucial assertion: at the moment dependent started, dep had already
    // marked itself ready.
    assert!(
        dep_marked.load(Ordering::SeqCst),
        "dependent started before its dep marked ready",
    );

    handle.request_shutdown(ShutdownReason::Requested);
    runner.await.unwrap().unwrap();
}

// ============ auto_ready=true behaves like v3 (no gating) ============

struct AutoReadyDep {
    /// We do a small simulated init that does NOT involve mark_ready.
    started: Arc<AtomicBool>,
}

impl Service for AutoReadyDep {
    const NAME: &'static str = "auto-dep";
    type Error = Infallible;
    fn run(
        self: Arc<Self>,
        ctx: ServiceContext,
    ) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send {
        async move {
            self.started.store(true, Ordering::SeqCst);
            ctx.cancelled().await;
            Ok(())
        }
    }
}

#[tokio::test]
async fn auto_ready_dep_is_ready_immediately_so_dependents_dont_block() {
    let started = Arc::new(AtomicBool::new(false));

    let dep = Arc::new(AutoReadyDep {
        started: started.clone(),
    });
    let dependent = Arc::new(Dependent {
        start_order: Arc::new(AtomicUsize::new(0)),
        start_index: Arc::new(AtomicUsize::new(usize::MAX)),
    });

    let rt = RuntimeBuilder::new()
        .service(dep)
        .unwrap()
        .service_with_deps(dependent, &["auto-dep"])
        .unwrap()
        .shutdown_timeout(Duration::from_secs(2))
        .build();

    let handle = rt.handle();
    let runner = tokio::spawn(rt.run());

    // Wait briefly; both should be running quickly.
    assert!(
        wait_for(
            || handle.is_ready("auto-dep") && handle.is_ready("dependent"),
            Duration::from_secs(1),
        )
        .await,
        "both services should reach ready quickly with auto_ready",
    );
    assert!(started.load(Ordering::SeqCst));

    handle.request_shutdown(ShutdownReason::Requested);
    runner.await.unwrap().unwrap();
}

// ============ NoOp helper ============

struct NoOp;
impl Service for NoOp {
    const NAME: &'static str = "noop";
    type Error = Infallible;
    fn run(
        self: Arc<Self>,
        ctx: ServiceContext,
    ) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send {
        async move {
            ctx.cancelled().await;
            Ok(())
        }
    }
}
