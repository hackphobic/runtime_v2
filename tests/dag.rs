// Copyright 2026 runtime contributors
// SPDX-License-Identifier: Apache-2.0
//
// We test DAG behavior end-to-end through RuntimeBuilder → Runtime::run, since
// the topological sort is invoked inside `run` and `DagError` is exposed via
// `RuntimeError::Dag`. This avoids reaching into `pub(crate)` internals.

use std::{convert::Infallible, sync::Arc, time::Duration};

use runtime::{
    DagError, RuntimeBuilder, RuntimeError, Service, ServiceContext, ServiceStartError,
};

// ============ test services ============

macro_rules! make_service {
    ($name:ident, $service_name:literal) => {
        struct $name;
        impl Service for $name {
            const NAME: &'static str = $service_name;
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
    };
}

make_service!(A, "a");
make_service!(B, "b");
make_service!(C, "c");
make_service!(D, "d");

// ============ helpers ============

async fn run_and_immediately_cancel(rt: runtime::Runtime) -> Result<(), RuntimeError> {
    let handle = rt.handle();
    let runner = tokio::spawn(rt.run());
    // Give the supervisor a moment to validate the DAG and either reject or start.
    tokio::time::sleep(Duration::from_millis(20)).await;
    handle.request_shutdown(runtime::ShutdownReason::Requested);
    runner.await.unwrap()
}

// ============ tests ============

#[tokio::test]
async fn linear_chain_runs() {
    // a → b → c → d
    let rt = RuntimeBuilder::new()
        .service(Arc::new(A))
        .unwrap()
        .service_with_deps(Arc::new(B), &["a"])
        .unwrap()
        .service_with_deps(Arc::new(C), &["b"])
        .unwrap()
        .service_with_deps(Arc::new(D), &["c"])
        .unwrap()
        .shutdown_timeout(Duration::from_secs(2))
        .build();

    run_and_immediately_cancel(rt).await.unwrap();
}

#[tokio::test]
async fn diamond_runs() {
    // a → b
    // a → c
    // b,c → d
    let rt = RuntimeBuilder::new()
        .service(Arc::new(A))
        .unwrap()
        .service_with_deps(Arc::new(B), &["a"])
        .unwrap()
        .service_with_deps(Arc::new(C), &["a"])
        .unwrap()
        .service_with_deps(Arc::new(D), &["b", "c"])
        .unwrap()
        .shutdown_timeout(Duration::from_secs(2))
        .build();

    run_and_immediately_cancel(rt).await.unwrap();
}

#[tokio::test]
async fn duplicate_deps_are_tolerated() {
    // dedup_preserve_order must collapse the duplicate "a" so the DAG indegree
    // stays correct.
    let rt = RuntimeBuilder::new()
        .service(Arc::new(A))
        .unwrap()
        .service_with_deps(Arc::new(B), &["a", "a", "a"])
        .unwrap()
        .shutdown_timeout(Duration::from_secs(2))
        .build();

    run_and_immediately_cancel(rt).await.unwrap();
}

#[tokio::test]
async fn unknown_dep_rejected() {
    let rt = RuntimeBuilder::new()
        .service_with_deps(Arc::new(A), &["nonexistent"])
        .unwrap()
        .build();

    let err = rt.run().await.unwrap_err();
    match err {
        RuntimeError::Dag(DagError::UnknownDependency { service, dep }) => {
            assert_eq!(service, "a");
            assert_eq!(dep, "nonexistent");
        }
        other => panic!("expected UnknownDependency, got {other:?}"),
    }
}

#[tokio::test]
async fn self_dependency_rejected() {
    let rt = RuntimeBuilder::new()
        .service_with_deps(Arc::new(A), &["a"])
        .unwrap()
        .build();

    let err = rt.run().await.unwrap_err();
    match err {
        RuntimeError::Dag(DagError::SelfDependency(name)) => assert_eq!(name, "a"),
        other => panic!("expected SelfDependency, got {other:?}"),
    }
}

#[tokio::test]
async fn cycle_rejected() {
    // a → b → c → a (cycle)
    // (We can't construct this with explicit deps on a single builder pass
    // because B is declared before C exists, so we declare deps lazily by
    // referencing names that get registered later.)
    let rt = RuntimeBuilder::new()
        .service_with_deps(Arc::new(A), &["c"])
        .unwrap()
        .service_with_deps(Arc::new(B), &["a"])
        .unwrap()
        .service_with_deps(Arc::new(C), &["b"])
        .unwrap()
        .build();

    let err = rt.run().await.unwrap_err();
    match err {
        RuntimeError::Dag(DagError::Cycle(names)) => {
            // The exact membership depends on Kahn's algorithm; all three
            // services should remain because they all have indegree > 0.
            assert!(names.contains(&"a"));
            assert!(names.contains(&"b"));
            assert!(names.contains(&"c"));
        }
        other => panic!("expected Cycle, got {other:?}"),
    }
}

#[tokio::test]
async fn duplicate_service_name_rejected_at_build_time() {
    let result = RuntimeBuilder::new()
        .service(Arc::new(A))
        .unwrap()
        .service(Arc::new(A));

    match result {
        Err(ServiceStartError::Duplicate(name)) => assert_eq!(name, "a"),
        Ok(_) => panic!("expected Duplicate"),
    }
}
