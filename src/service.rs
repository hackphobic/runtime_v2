// Copyright 2026 runtime contributors
// SPDX-License-Identifier: Apache-2.0

//! The [`Service`] trait, its execution [`ServiceContext`], and supporting
//! error / policy types.
//!
//! A service is a long-running unit of work registered with the runtime. The
//! runtime starts services in topological dependency order, gates each one
//! behind its declared dependencies' readiness, hands it a
//! [`ServiceContext`] with a per-service [`CancellationToken`], and drives
//! the returned future. Returning `Ok(())` exits the service gracefully;
//! returning `Err(_)` is handled according to the service's [`OnError`] policy.

use std::{
    error::Error as StdError,
    fmt,
    future::Future,
    pin::Pin,
    sync::Arc,
    time::Duration,
};

use tokio_util::sync::CancellationToken;

use crate::runtime::RuntimeHandle;

/// Static identifier for a service. Used to declare dependencies and label logs.
pub type ServiceName = &'static str;

// ============ Failure policy ============

/// What the supervisor should do when a service's `run()` returns `Err(_)`.
///
/// Defaults to [`OnError::ShutdownRuntime`]: a failed service is treated as
/// fatal unless it opts into restart or explicitly asks to be ignored.
#[derive(Clone, Copy, Debug, Default)]
pub enum OnError {
    /// Trigger a runtime-wide shutdown with [`ShutdownReason::ServiceFailed`].
    /// This is the default.
    ///
    /// [`ShutdownReason::ServiceFailed`]: crate::ShutdownReason::ServiceFailed
    #[default]
    ShutdownRuntime,
    /// Restart the service after a backoff. Other services keep running.
    Restart(RestartPolicy),
    /// Log the error and let the service stay exited. Other services keep
    /// running. The service's readiness state (if set) is preserved.
    Ignore,
}

/// Restart policy: exponential backoff with optional jitter and max retries.
#[derive(Clone, Copy, Debug)]
pub struct RestartPolicy {
    /// Minimum backoff (the delay before the first restart attempt).
    pub min_backoff: Duration,
    /// Maximum backoff cap. Backoff grows as `min_backoff * 2^(attempt-1)`,
    /// capped at this value.
    pub max_backoff: Duration,
    /// Jitter ratio in `[0.0, 1.0]`. `0.2` means each backoff is multiplied
    /// by a random factor in `[0.8, 1.2]`. Defaults to `0.2`.
    pub jitter: f64,
    /// Maximum number of restart attempts. `None` means unlimited.
    pub max_retries: Option<u32>,
    /// What to do once `max_retries` is reached. Defaults to
    /// [`ExhaustedAction::ShutdownRuntime`].
    pub on_exhausted: ExhaustedAction,
}

impl Default for RestartPolicy {
    fn default() -> Self {
        Self {
            min_backoff: Duration::from_millis(250),
            max_backoff: Duration::from_secs(30),
            jitter: 0.2,
            max_retries: None,
            on_exhausted: ExhaustedAction::ShutdownRuntime,
        }
    }
}

impl RestartPolicy {
    /// Construct a restart policy with the given bounds and default jitter
    /// (`0.2`) and no retry cap.
    #[must_use]
    pub const fn with_bounds(min_backoff: Duration, max_backoff: Duration) -> Self {
        Self {
            min_backoff,
            max_backoff,
            jitter: 0.2,
            max_retries: None,
            on_exhausted: ExhaustedAction::ShutdownRuntime,
        }
    }
}

/// What to do when a restart policy's `max_retries` is exceeded.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExhaustedAction {
    /// Treat the exhausted service as a fatal runtime failure.
    ShutdownRuntime,
    /// Let the service stay exited; runtime keeps going.
    Ignore,
}

// ============ Service trait ============

/// Context handed to every service when it's started.
///
/// Cloning is cheap; clones share the same cancellation token tree, runtime
/// handle, and name.
#[derive(Clone, Debug)]
pub struct ServiceContext {
    /// Cancellation token for this service.
    ///
    /// This is a child of the runtime's root token: cancelling the root
    /// cancels every service token, but cancelling one service's token
    /// doesn't affect siblings. Services should treat this firing as a
    /// request to wind down.
    pub cancel: CancellationToken,

    /// Handle to the runtime. Use to query state or request shutdown.
    pub runtime: RuntimeHandle,

    /// The service's declared name (matches its [`Service::NAME`]).
    pub name: ServiceName,
}

impl ServiceContext {
    /// Await cancellation of this service's token.
    pub async fn cancelled(&self) {
        self.cancel.cancelled().await;
    }

    /// Returns `true` if this service's cancellation has been triggered.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.cancel.is_cancelled()
    }

    /// Create a child cancellation token for a sub-task.
    #[must_use]
    pub fn child_cancel(&self) -> CancellationToken {
        self.cancel.child_token()
    }

    /// Mark this service as ready, allowing dependents to proceed.
    ///
    /// Any service that others depend on must call this once its initialization
    /// (DB hydration, listener bind, initial sync) is complete — dependents
    /// block until then. A service that nothing depends on need not call it.
    pub fn mark_ready(&self) {
        self.runtime.mark_ready(self.name);
    }
}

/// A long-running unit of work.
///
/// Implementors typically loop on inputs, using [`tokio::select!`] to bail on
/// cancellation:
///
/// ```ignore
/// loop {
///     tokio::select! {
///         _ = ctx.cancelled() => return Ok(()),
///         msg = receiver.recv() => { /* handle */ }
///     }
/// }
/// ```
///
/// If other services declare a dependency on this one, call
/// [`ServiceContext::mark_ready`] once initialization is complete — dependents
/// stay gated until then.
///
/// The returned future is required to be `Send` so the supervisor can spawn
/// it onto a multi-threaded runtime.
pub trait Service: Send + Sync + 'static {
    /// Stable, human-readable name. Used for dependency declaration and tracing.
    const NAME: ServiceName;

    /// The service's typed error.
    type Error: StdError + Send + Sync + 'static;

    /// Main execution function. Called once per spawn by the runtime.
    ///
    /// - `Ok(())` ends the service gracefully. If the service was restartable,
    ///   it will *not* be restarted on a graceful exit.
    /// - `Err(_)` is handled according to [`on_error`](Self::on_error).
    fn run(
        self: Arc<Self>,
        ctx: ServiceContext,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Policy for handling errors returned by `run()`. Defaults to
    /// [`OnError::ShutdownRuntime`].
    fn on_error(&self) -> OnError {
        OnError::ShutdownRuntime
    }
}

// ============ Errors ============

/// Error returned when a service can't be registered with the builder.
#[derive(Debug, thiserror::Error)]
pub enum ServiceStartError {
    /// Another service with the same [`Service::NAME`] was already registered.
    #[error("duplicate service name: {0}")]
    Duplicate(ServiceName),
}

/// Boundary error: wraps a service's typed error along with its name.
///
/// The [`Runtime`](crate::Runtime) returns this when a service fails fatally
/// (either because its policy is [`OnError::ShutdownRuntime`] or because its
/// restart attempts were exhausted with
/// [`ExhaustedAction::ShutdownRuntime`]). Use [`downcast_ref`](Self::downcast_ref)
/// to recover the concrete error type.
#[derive(Debug)]
pub struct DynServiceError {
    /// The name of the service that failed.
    pub service: ServiceName,
    /// The boxed typed error.
    pub source: Box<dyn StdError + Send + Sync + 'static>,
}

impl DynServiceError {
    /// Attempt to downcast the inner error to a concrete type.
    #[must_use]
    pub fn downcast_ref<E: StdError + 'static>(&self) -> Option<&E> {
        (self.source.as_ref() as &(dyn StdError + 'static)).downcast_ref::<E>()
    }
}

impl fmt::Display for DynServiceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "service `{}` failed: {}", self.service, self.source)
    }
}

impl StdError for DynServiceError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        Some(&*self.source)
    }
}

// ============ internal type erasure ============

type BoxedFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Internal trait that erases the concrete `Service` type so the supervisor
/// can store heterogeneous services uniformly.
pub(crate) trait DynService: Send + Sync + 'static {
    fn on_error(&self) -> OnError;
    fn run_boxed(
        self: Arc<Self>,
        ctx: ServiceContext,
    ) -> BoxedFuture<'static, Result<(), DynServiceError>>;
}

pub(crate) struct ServiceAdapter<S: Service>(pub(crate) Arc<S>);

impl<S: Service> DynService for ServiceAdapter<S> {
    fn on_error(&self) -> OnError {
        self.0.on_error()
    }

    fn run_boxed(
        self: Arc<Self>,
        ctx: ServiceContext,
    ) -> BoxedFuture<'static, Result<(), DynServiceError>> {
        let inner = Arc::clone(&self.0);
        Box::pin(async move {
            inner.run(ctx).await.map_err(|e| DynServiceError {
                service: S::NAME,
                source: Box::new(e),
            })
        })
    }
}

/// Internal: a registered service plus its declared dependencies. Built by
/// [`RuntimeBuilder`](crate::RuntimeBuilder), consumed by the supervisor.
pub(crate) struct ServiceSpec {
    pub(crate) name: ServiceName,
    pub(crate) deps: Vec<ServiceName>,
    pub(crate) adapter: Arc<dyn DynService>,
}

impl fmt::Debug for ServiceSpec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ServiceSpec")
            .field("name", &self.name)
            .field("deps", &self.deps)
            .finish_non_exhaustive()
    }
}
