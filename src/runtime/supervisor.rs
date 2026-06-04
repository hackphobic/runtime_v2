// Copyright 2026 runtime contributors
// SPDX-License-Identifier: Apache-2.0

use std::{sync::Arc, time::Duration};

use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, info, warn};

use crate::{
    RuntimeHandle,
    service::{DynService, DynServiceError, ExhaustedAction, OnError, RestartPolicy, ServiceContext, ServiceName},
};

use super::types::{Runtime, RuntimeError, ShutdownReason};

/// Per-service supervision state, indexed so a JoinSet task can correlate back.
struct ServiceState {
    name: ServiceName,
    deps: Vec<ServiceName>,
    adapter: Arc<dyn DynService>,
    on_error: OnError,
    /// How many restart attempts have been made.
    attempts: u32,
}

/// JoinSet task output: `(service_index, run_result)`.
type TaskResult = (usize, Result<(), DynServiceError>);

pub(crate) async fn run_supervised(rt: Runtime) -> Result<(), RuntimeError> {
    let Runtime { inner, specs } = rt;

    let order = super::dag::topo_sort(&specs)?;
    let handle = RuntimeHandle {
        inner: Arc::clone(&inner),
    };
    let root_cancel = inner.cancel.clone();
    let shutdown_timeout = inner.shutdown_timeout;

    // Build the indexed state vector in topological order so logs make sense.
    let mut states: Vec<ServiceState> = order
        .into_iter()
        .map(|i| {
            let spec = &specs[i];
            let adapter = Arc::clone(&spec.adapter);
            let on_error = adapter.on_error();
            ServiceState {
                name: spec.name,
                deps: spec.deps.clone(),
                adapter,
                on_error,
                attempts: 0,
            }
        })
        .collect();

    // Spec vec is no longer needed.
    drop(specs);

    let mut joinset: JoinSet<TaskResult> = JoinSet::new();

    // Initial spawn of every service.
    for (idx, state) in states.iter().enumerate() {
        spawn_service(&mut joinset, idx, state, &handle, &root_cancel, None);
    }

    // Run the supervision loop.
    let loop_outcome = supervision_loop(
        &mut joinset,
        &mut states,
        &handle,
        &root_cancel,
    )
    .await;

    if root_cancel.is_cancelled() {
        info!(reason = ?handle.shutdown_reason(), "draining services");
    }

    // Always drain remaining tasks within the timeout.
    let drain_outcome = drain_with_timeout(&mut joinset, shutdown_timeout).await;

    match (loop_outcome, drain_outcome) {
        (Ok(()), Ok(())) => Ok(()),
        (Ok(()), Err(drain_err)) => Err(drain_err),
        (Err(loop_err), Ok(())) => Err(loop_err),
        (Err(loop_err), Err(drain_err)) => {
            warn!(error = %drain_err, "drain also failed");
            Err(loop_err)
        }
    }
}

/// Spawn a service into the joinset, for either a fresh start (`delay = None`)
/// or a restart (`delay = Some(backoff)`).
///
/// The spawned task, in order: waits out the restart backoff if any (cancellable);
/// awaits readiness of each declared dependency (cancellable); then runs the
/// service. Readiness is the service's own responsibility — it calls
/// [`ServiceContext::mark_ready`] when its init completes.
fn spawn_service(
    joinset: &mut JoinSet<TaskResult>,
    idx: usize,
    state: &ServiceState,
    handle: &RuntimeHandle,
    root_cancel: &CancellationToken,
    delay: Option<Duration>,
) {
    let name = state.name;
    let deps = state.deps.clone();
    let adapter = Arc::clone(&state.adapter);
    let handle = handle.clone();
    let root_cancel = root_cancel.clone();

    let span = if delay.is_some() {
        tracing::info_span!("service-restart", name)
    } else {
        let span = tracing::info_span!("service", name);
        info!(parent: &span, deps = ?deps, "starting");
        span
    };

    joinset.spawn(
        async move {
            // Restart backoff, if this is a restart.
            if let Some(backoff) = delay {
                tokio::select! {
                    _ = root_cancel.cancelled() => return (idx, Ok(())),
                    _ = tokio::time::sleep(backoff) => {}
                }
            }

            let cancel = root_cancel.child_token();
            let ctx = ServiceContext {
                cancel: cancel.clone(),
                runtime: handle.clone(),
                name,
            };

            // Wait for declared dependencies to be ready (cooperatively cancellable).
            let wait_deps = async {
                for &dep in &deps {
                    handle.await_ready(dep).await;
                }
            };

            tokio::select! {
                _ = cancel.cancelled() => {
                    // Shutdown before we even got to run; treat as graceful exit.
                    return (idx, Ok(()));
                }
                _ = wait_deps => {}
            }

            let result = adapter.run_boxed(ctx).await;
            (idx, result)
        }
        .instrument(span),
    );
}

/// Main supervision loop. Returns when the runtime is shutting down or all
/// tasks have exited; on a fatal service failure, returns `Err`.
async fn supervision_loop(
    joinset: &mut JoinSet<TaskResult>,
    states: &mut [ServiceState],
    handle: &RuntimeHandle,
    root_cancel: &CancellationToken,
) -> Result<(), RuntimeError> {
    loop {
        tokio::select! {
            _ = root_cancel.cancelled() => {
                info!(reason = ?handle.shutdown_reason(), "shutdown requested");
                return Ok(());
            }

            res = joinset.join_next() => match res {
                None => {
                    info!("all services exited");
                    return Ok(());
                }
                Some(Ok((idx, Ok(())))) => {
                    info!(service = states[idx].name, "service exited cleanly");
                    // Graceful exits are never restarted, even under Restart policy.
                }
                Some(Ok((idx, Err(e)))) => {
                    if let Some(err) = handle_failure(joinset, states, handle, root_cancel, idx, e) {
                        return Err(err);
                    }
                }
                Some(Err(join_err)) => {
                    warn!(
                        error = %join_err,
                        "service task panicked or was cancelled unexpectedly",
                    );
                    handle.request_shutdown(ShutdownReason::ServiceFailed);
                    return Err(RuntimeError::ServiceFailed(DynServiceError {
                        service: "<unknown>",
                        source: Box::new(join_err),
                    }));
                }
            }
        }
    }
}

/// Dispatch on the failed service's `OnError` policy. Returns `Some(err)` if
/// the failure escalates to a runtime shutdown, or `None` if it's absorbed
/// (ignored or scheduled for restart).
fn handle_failure(
    joinset: &mut JoinSet<TaskResult>,
    states: &mut [ServiceState],
    handle: &RuntimeHandle,
    root_cancel: &CancellationToken,
    idx: usize,
    err: DynServiceError,
) -> Option<RuntimeError> {
    let state = &mut states[idx];
    match state.on_error {
        OnError::ShutdownRuntime => {
            warn!(
                service = state.name,
                error = %err,
                "service failed; shutting down runtime",
            );
            handle.request_shutdown(ShutdownReason::ServiceFailed);
            Some(RuntimeError::ServiceFailed(err))
        }
        OnError::Ignore => {
            warn!(
                service = state.name,
                error = %err,
                "service failed; ignored per policy",
            );
            None
        }
        OnError::Restart(policy) => {
            state.attempts = state.attempts.saturating_add(1);

            if let Some(max) = policy.max_retries {
                if state.attempts > max {
                    warn!(
                        service = state.name,
                        attempts = state.attempts,
                        "restart attempts exhausted",
                    );
                    return match policy.on_exhausted {
                        ExhaustedAction::ShutdownRuntime => {
                            handle.request_shutdown(ShutdownReason::ServiceFailed);
                            Some(RuntimeError::ServiceFailed(err))
                        }
                        ExhaustedAction::Ignore => None,
                    };
                }
            }

            let backoff = compute_backoff(&policy, state.attempts);
            warn!(
                service = state.name,
                error = %err,
                ?backoff,
                attempt = state.attempts,
                "service failed; scheduling restart",
            );

            // Re-spawn the service after the backoff (same task body as the
            // initial spawn, with the restart delay applied up front).
            spawn_service(joinset, idx, &states[idx], handle, root_cancel, Some(backoff));

            None
        }
    }
}

/// Compute the next backoff using exponential growth + jitter.
///
/// `attempt` is 1-indexed (1 for the first restart attempt).
fn compute_backoff(policy: &RestartPolicy, attempt: u32) -> Duration {
    // Clamp the exponent to avoid overflow even with extreme attempt counts.
    let exp_pow = attempt.saturating_sub(1).min(20);
    let exp = 1u64.checked_shl(exp_pow).unwrap_or(u64::MAX);

    let base_ms = policy.min_backoff.as_millis() as u64;
    let mut ms = base_ms.saturating_mul(exp);
    let max_ms = policy.max_backoff.as_millis() as u64;
    if ms > max_ms {
        ms = max_ms;
    }

    // Symmetric jitter in [-jitter, +jitter] proportional to the current ms.
    if policy.jitter > 0.0 {
        let factor = policy.jitter.clamp(0.0, 1.0);
        let jitter_ms = ((ms as f64) * factor) as u64;
        if jitter_ms > 0 {
            // fastrand::u64(low..=high) inclusive
            let delta = fastrand::u64(0..=jitter_ms.saturating_mul(2));
            ms = ms.saturating_sub(jitter_ms).saturating_add(delta);
        }
    }

    Duration::from_millis(ms)
}

/// Drain all remaining tasks in the joinset, bounded by `timeout`.
async fn drain_with_timeout(
    joinset: &mut JoinSet<TaskResult>,
    timeout: Duration,
) -> Result<(), RuntimeError> {
    let drain = async {
        while joinset.join_next().await.is_some() {}
    };

    tokio::time::timeout(timeout, drain)
        .await
        .map_err(|_| RuntimeError::ShutdownTimeout(timeout))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_grows_then_caps() {
        let policy = RestartPolicy {
            min_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(1),
            jitter: 0.0, // deterministic
            max_retries: None,
            on_exhausted: ExhaustedAction::ShutdownRuntime,
        };

        assert_eq!(compute_backoff(&policy, 1), Duration::from_millis(100));
        assert_eq!(compute_backoff(&policy, 2), Duration::from_millis(200));
        assert_eq!(compute_backoff(&policy, 3), Duration::from_millis(400));
        assert_eq!(compute_backoff(&policy, 4), Duration::from_millis(800));
        assert_eq!(compute_backoff(&policy, 5), Duration::from_millis(1000)); // capped
        assert_eq!(compute_backoff(&policy, 100), Duration::from_millis(1000));
    }

    #[test]
    fn backoff_jitter_stays_in_bounds() {
        let policy = RestartPolicy {
            min_backoff: Duration::from_millis(1000),
            max_backoff: Duration::from_secs(60),
            jitter: 0.2,
            max_retries: None,
            on_exhausted: ExhaustedAction::ShutdownRuntime,
        };

        for _ in 0..100 {
            let b = compute_backoff(&policy, 3); // base = 4000ms, jitter ±800ms
            let ms = b.as_millis() as u64;
            assert!(ms >= 3200 && ms <= 4800, "got {ms}ms, want 3200..=4800");
        }
    }

    #[test]
    fn backoff_with_huge_attempt_doesnt_overflow() {
        let policy = RestartPolicy {
            min_backoff: Duration::from_secs(1),
            max_backoff: Duration::from_secs(30),
            jitter: 0.0,
            max_retries: None,
            on_exhausted: ExhaustedAction::ShutdownRuntime,
        };
        // Saturation; just confirm it doesn't panic and is bounded.
        let b = compute_backoff(&policy, u32::MAX);
        assert!(b <= Duration::from_secs(30));
    }
}
