// Copyright 2026 runtime contributors
// SPDX-License-Identifier: Apache-2.0

use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use runtime::{
    ExhaustedAction, OnError, RestartPolicy, RuntimeBuilder, RuntimeError, Service,
    ServiceContext, ShutdownReason,
};

// ============ test errors ============

#[derive(thiserror::Error, Debug)]
#[error("flaky #{attempt}")]
struct FlakyError {
    attempt: usize,
}

// ============ Flaky service: errors `fail_until` times, then succeeds ============

struct Flaky {
    fail_until: usize,
    attempts: Arc<AtomicUsize>,
    policy: OnError,
}

impl Service for Flaky {
    const NAME: &'static str = "flaky";
    type Error = FlakyError;

    fn on_error(&self) -> OnError {
        self.policy
    }

    fn run(
        self: Arc<Self>,
        ctx: ServiceContext,
    ) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send {
        async move {
            let attempt = self.attempts.fetch_add(1, Ordering::SeqCst) + 1;
            if attempt <= self.fail_until {
                return Err(FlakyError { attempt });
            }
            // Now stable; wait for cancellation.
            ctx.cancelled().await;
            Ok(())
        }
    }
}

// ============ Always-failing service (for exhaustion tests) ============

#[derive(thiserror::Error, Debug)]
#[error("doomed")]
struct DoomedError;

struct Doomed {
    attempts: Arc<AtomicUsize>,
    policy: OnError,
}

impl Service for Doomed {
    const NAME: &'static str = "doomed";
    type Error = DoomedError;

    fn on_error(&self) -> OnError {
        self.policy
    }

    fn run(
        self: Arc<Self>,
        _ctx: ServiceContext,
    ) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send {
        async move {
            self.attempts.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(5)).await;
            Err(DoomedError)
        }
    }
}

// ============ tests ============

#[tokio::test]
async fn restart_recovers_after_transient_failures() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let service = Arc::new(Flaky {
        fail_until: 3,
        attempts: attempts.clone(),
        policy: OnError::Restart(RestartPolicy {
            min_backoff: Duration::from_millis(10),
            max_backoff: Duration::from_millis(50),
            jitter: 0.0,
            max_retries: None,
            on_exhausted: ExhaustedAction::ShutdownRuntime,
        }),
    });

    let rt = RuntimeBuilder::new()
        .service(service)
        .unwrap()
        .shutdown_timeout(Duration::from_secs(2))
        .build();

    let handle = rt.handle();
    let runner = tokio::spawn(rt.run());

    // Wait until the service has succeeded (attempt #4 starts running).
    let deadline = Instant::now() + Duration::from_secs(2);
    while attempts.load(Ordering::SeqCst) < 4 && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(
        attempts.load(Ordering::SeqCst) >= 4,
        "expected ≥4 attempts (3 fails + 1 success), got {}",
        attempts.load(Ordering::SeqCst)
    );
    assert!(!handle.is_shutting_down(), "runtime should still be running");

    handle.request_shutdown(ShutdownReason::Requested);
    runner.await.unwrap().unwrap();
}

#[tokio::test]
async fn restart_max_retries_exhausted_shuts_runtime_down() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let service = Arc::new(Doomed {
        attempts: attempts.clone(),
        policy: OnError::Restart(RestartPolicy {
            min_backoff: Duration::from_millis(5),
            max_backoff: Duration::from_millis(20),
            jitter: 0.0,
            max_retries: Some(3),
            on_exhausted: ExhaustedAction::ShutdownRuntime,
        }),
    });

    let rt = RuntimeBuilder::new()
        .service(service)
        .unwrap()
        .shutdown_timeout(Duration::from_secs(2))
        .build();

    let err = rt.run().await.unwrap_err();
    assert!(
        matches!(err, RuntimeError::ServiceFailed(_)),
        "expected ServiceFailed, got {err:?}",
    );
    // 1 initial run + 3 restarts = 4 attempts total
    let total = attempts.load(Ordering::SeqCst);
    assert_eq!(total, 4, "expected exactly 4 attempts, got {total}");
}

#[tokio::test]
async fn restart_max_retries_exhausted_with_ignore_keeps_runtime_alive() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let doomed = Arc::new(Doomed {
        attempts: attempts.clone(),
        policy: OnError::Restart(RestartPolicy {
            min_backoff: Duration::from_millis(5),
            max_backoff: Duration::from_millis(20),
            jitter: 0.0,
            max_retries: Some(2),
            on_exhausted: ExhaustedAction::Ignore,
        }),
    });

    // Pair with a bystander that just sits.
    let bystander = Arc::new(Bystander);

    let rt = RuntimeBuilder::new()
        .service(doomed)
        .unwrap()
        .service(bystander)
        .unwrap()
        .shutdown_timeout(Duration::from_secs(2))
        .build();

    let handle = rt.handle();
    let runner = tokio::spawn(rt.run());

    // Wait for exhaustion (1 + 2 = 3 attempts) to complete.
    let deadline = Instant::now() + Duration::from_secs(2);
    while attempts.load(Ordering::SeqCst) < 3 && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(attempts.load(Ordering::SeqCst), 3);

    // Give the supervisor a beat to process the exhaustion.
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Runtime should still be running; only the bystander is left.
    assert!(!handle.is_shutting_down());

    handle.request_shutdown(ShutdownReason::Requested);
    runner.await.unwrap().unwrap();
}

#[tokio::test]
async fn ignore_policy_lets_service_exit_without_affecting_runtime() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let doomed = Arc::new(Doomed {
        attempts: attempts.clone(),
        policy: OnError::Ignore,
    });
    let bystander = Arc::new(Bystander);

    let rt = RuntimeBuilder::new()
        .service(doomed)
        .unwrap()
        .service(bystander)
        .unwrap()
        .shutdown_timeout(Duration::from_secs(2))
        .build();

    let handle = rt.handle();
    let runner = tokio::spawn(rt.run());

    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(attempts.load(Ordering::SeqCst), 1, "Ignore = no restart");
    assert!(!handle.is_shutting_down());

    handle.request_shutdown(ShutdownReason::Requested);
    runner.await.unwrap().unwrap();
}

#[tokio::test]
async fn default_policy_is_shutdown_runtime() {
    // Default on_error() is ShutdownRuntime; one failure kills the runtime.
    let attempts = Arc::new(AtomicUsize::new(0));
    let doomed = Arc::new(Doomed {
        attempts: attempts.clone(),
        policy: OnError::ShutdownRuntime, // explicit; this IS the default
    });

    let rt = RuntimeBuilder::new()
        .service(doomed)
        .unwrap()
        .shutdown_timeout(Duration::from_secs(2))
        .build();

    let err = rt.run().await.unwrap_err();
    assert!(matches!(err, RuntimeError::ServiceFailed(_)));
    assert_eq!(attempts.load(Ordering::SeqCst), 1, "no restart under default");
}

#[tokio::test]
async fn shutdown_during_restart_backoff_doesnt_hang() {
    // A service that fails immediately and is set up for long-backoff restarts.
    // If we request_shutdown while it's sleeping between restarts, the runtime
    // must still finish in bounded time.
    let attempts = Arc::new(AtomicUsize::new(0));
    let doomed = Arc::new(Doomed {
        attempts: attempts.clone(),
        policy: OnError::Restart(RestartPolicy {
            min_backoff: Duration::from_secs(60), // intentionally long
            max_backoff: Duration::from_secs(60),
            jitter: 0.0,
            max_retries: None,
            on_exhausted: ExhaustedAction::ShutdownRuntime,
        }),
    });

    let rt = RuntimeBuilder::new()
        .service(doomed)
        .unwrap()
        .shutdown_timeout(Duration::from_secs(2))
        .build();

    let handle = rt.handle();
    let started = Instant::now();
    let runner = tokio::spawn(rt.run());

    // Wait for one failure to occur and the backoff sleep to start.
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(attempts.load(Ordering::SeqCst) >= 1);

    handle.request_shutdown(ShutdownReason::Requested);
    runner.await.unwrap().unwrap();
    // Should not have waited anywhere near 60s.
    assert!(
        started.elapsed() < Duration::from_secs(3),
        "shutdown during restart backoff took too long: {:?}",
        started.elapsed(),
    );
}

// ============ helpers ============

struct Bystander;
impl Service for Bystander {
    const NAME: &'static str = "bystander";
    type Error = std::convert::Infallible;
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
