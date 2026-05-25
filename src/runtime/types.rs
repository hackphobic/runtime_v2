// Copyright 2026 runtime contributors
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::HashMap,
    fmt,
    sync::{
        Arc,
        atomic::{AtomicU8, Ordering},
    },
    time::Duration,
};

use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use crate::{
    runtime::{dag::DagError, supervisor},
    service::{DynServiceError, ServiceName, ServiceSpec},
    util::DEFAULT_SHUTDOWN_TIMEOUT,
};

// ============ ShutdownReason ============

/// Why the runtime began shutting down.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShutdownReason {
    /// Explicit [`RuntimeHandle::request_shutdown`] call.
    Requested = 1,
    /// A service returned `Err(_)` and its policy escalated to a runtime shutdown.
    ServiceFailed = 2,
    /// An OS signal (e.g. Ctrl-C, when using [`Runtime::run_with_ctrl_c`]) was received.
    Signal = 3,
}

impl ShutdownReason {
    fn from_u8(v: u8) -> Self {
        match v {
            2 => Self::ServiceFailed,
            3 => Self::Signal,
            _ => Self::Requested,
        }
    }
}

pub(crate) struct AtomicShutdownReason(AtomicU8);

impl AtomicShutdownReason {
    fn new(r: ShutdownReason) -> Self {
        Self(AtomicU8::new(r as u8))
    }
    fn load(&self) -> ShutdownReason {
        ShutdownReason::from_u8(self.0.load(Ordering::Acquire))
    }
    fn store(&self, r: ShutdownReason) {
        self.0.store(r as u8, Ordering::Release);
    }
}

// ============ Readiness registry ============

/// Per-service readiness slot. A `watch::Sender<bool>` so awaiters can `.await`
/// the transition from `false` → `true` and new awaiters created after the
/// transition see `true` immediately.
pub(crate) struct ReadinessRegistry {
    slots: HashMap<ServiceName, watch::Sender<bool>>,
}

impl ReadinessRegistry {
    pub(crate) fn new(names: &[ServiceName]) -> Self {
        let mut slots = HashMap::with_capacity(names.len());
        for name in names {
            let (tx, _) = watch::channel(false);
            slots.insert(*name, tx);
        }
        Self { slots }
    }

    /// Mark a service ready. Idempotent; unknown names are logged and ignored.
    pub(crate) fn mark_ready(&self, name: ServiceName) {
        if let Some(tx) = self.slots.get(&name) {
            // send_replace is infallible; ignore the previous value
            let _ = tx.send_replace(true);
        } else {
            tracing::warn!(
                service = name,
                "mark_ready called for unregistered service; ignored",
            );
        }
    }

    pub(crate) fn is_ready(&self, name: ServiceName) -> bool {
        self.slots
            .get(&name)
            .map(|tx| *tx.borrow())
            .unwrap_or(false)
    }

    /// Resolves when the named service is ready. Returns immediately for
    /// unknown names (matching `is_ready`'s `false` behavior would block
    /// forever, which is the worse default).
    pub(crate) async fn await_ready(&self, name: ServiceName) {
        let Some(tx) = self.slots.get(&name) else {
            tracing::warn!(
                service = name,
                "await_ready called for unregistered service; returning immediately",
            );
            return;
        };
        let mut rx = tx.subscribe();
        // Cheap fast path: already ready
        if *rx.borrow() {
            return;
        }
        // Slow path: wait for transition
        while rx.changed().await.is_ok() {
            if *rx.borrow() {
                return;
            }
        }
        // If the sender was dropped the watch returns Err. We treat that as
        // "no longer trackable" — return so callers don't hang forever.
    }
}

// ============ RuntimeError ============

/// Errors produced by [`Runtime::run`].
#[derive(Debug, thiserror::Error)]
pub enum RuntimeError {
    /// The dependency graph could not be ordered (cycle, missing dep, etc.).
    #[error("dependency graph error: {0}")]
    Dag(#[from] DagError),

    /// A service returned an error from its `run` method and its
    /// [`OnError`](crate::OnError) policy escalated to a runtime shutdown.
    #[error("a service failed: {0}")]
    ServiceFailed(#[from] DynServiceError),

    /// Services did not finish draining within the configured shutdown timeout.
    #[error("shutdown timed out after {0:?}")]
    ShutdownTimeout(Duration),
}

// ============ RuntimeHandle ============

/// A cloneable handle to the runtime. Use to query state, request shutdown,
/// and read or update per-service readiness.
#[derive(Clone)]
pub struct RuntimeHandle {
    pub(crate) inner: Arc<RuntimeInner>,
}

impl RuntimeHandle {
    /// Returns `true` once shutdown has been requested (explicitly, via signal,
    /// or implicitly via a service failure escalation).
    #[must_use]
    pub fn is_shutting_down(&self) -> bool {
        self.inner.cancel.is_cancelled()
    }

    /// Request shutdown of the runtime. Idempotent.
    pub fn request_shutdown(&self, reason: ShutdownReason) {
        self.inner.reason.store(reason);
        self.inner.cancel.cancel();
    }

    /// Returns the recorded shutdown reason. Meaningful only after
    /// [`is_shutting_down`](Self::is_shutting_down) is true.
    #[must_use]
    pub fn shutdown_reason(&self) -> ShutdownReason {
        self.inner.reason.load()
    }

    /// The runtime's root cancellation token. Cancelled when shutdown is requested.
    #[must_use]
    pub fn cancellation_token(&self) -> CancellationToken {
        self.inner.cancel.clone()
    }

    /// Resolves when shutdown is requested. Safe to call before, during, or after.
    pub async fn wait_shutdown_requested(&self) {
        self.inner.cancel.cancelled().await;
    }

    /// Mark a service as ready, allowing dependents to proceed.
    ///
    /// Usually called via [`ServiceContext::mark_ready`](crate::ServiceContext::mark_ready),
    /// but exposed here for use from tests or out-of-band tooling.
    pub fn mark_ready(&self, name: ServiceName) {
        self.inner.readiness.mark_ready(name);
    }

    /// Returns whether a service has been marked ready.
    ///
    /// Returns `false` for unknown service names.
    #[must_use]
    pub fn is_ready(&self, name: ServiceName) -> bool {
        self.inner.readiness.is_ready(name)
    }

    /// Resolve when the named service is marked ready.
    ///
    /// Returns immediately if the service is already ready, or if the name is
    /// unknown to the runtime (with a warning logged in the latter case).
    pub async fn await_ready(&self, name: ServiceName) {
        self.inner.readiness.await_ready(name).await;
    }
}

impl fmt::Debug for RuntimeHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RuntimeHandle")
            .field("is_shutting_down", &self.is_shutting_down())
            .field("shutdown_reason", &self.shutdown_reason())
            .finish_non_exhaustive()
    }
}

// ============ Runtime ============

/// A configured runtime, ready to run.
///
/// Build with [`RuntimeBuilder`](crate::RuntimeBuilder). Run with [`Runtime::run`]
/// or [`Runtime::run_with_ctrl_c`].
pub struct Runtime {
    pub(crate) inner: Arc<RuntimeInner>,
    pub(crate) specs: Vec<ServiceSpec>,
}

impl Runtime {
    /// Get a cloneable handle to this runtime.
    #[must_use]
    pub fn handle(&self) -> RuntimeHandle {
        RuntimeHandle {
            inner: Arc::clone(&self.inner),
        }
    }

    /// Run the runtime until shutdown is requested or a service fails fatally.
    pub async fn run(self) -> Result<(), RuntimeError> {
        supervisor::run_supervised(self).await
    }

    /// Like [`Runtime::run`] but spawns a Ctrl-C listener that requests shutdown
    /// with [`ShutdownReason::Signal`] when the first signal is received.
    pub async fn run_with_ctrl_c(self) -> Result<(), RuntimeError> {
        let handle = self.handle();
        let token = handle.cancellation_token();
        tokio::spawn(async move {
            tokio::select! {
                _ = token.cancelled() => {}
                res = tokio::signal::ctrl_c() => {
                    if res.is_ok() {
                        handle.request_shutdown(ShutdownReason::Signal);
                    }
                }
            }
        });
        self.run().await
    }
}

impl fmt::Debug for Runtime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Runtime")
            .field("services", &self.specs.len())
            .finish_non_exhaustive()
    }
}

pub(crate) struct RuntimeInner {
    pub(crate) cancel: CancellationToken,
    pub(crate) reason: AtomicShutdownReason,
    pub(crate) shutdown_timeout: Duration,
    pub(crate) readiness: ReadinessRegistry,
}

impl RuntimeInner {
    pub(crate) fn new(
        shutdown_timeout: Option<Duration>,
        service_names: &[ServiceName],
    ) -> Self {
        Self {
            cancel: CancellationToken::new(),
            reason: AtomicShutdownReason::new(ShutdownReason::Requested),
            shutdown_timeout: shutdown_timeout.unwrap_or(DEFAULT_SHUTDOWN_TIMEOUT),
            readiness: ReadinessRegistry::new(service_names),
        }
    }
}
