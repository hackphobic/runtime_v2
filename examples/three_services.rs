// Copyright 2026 runtime contributors
// SPDX-License-Identifier: Apache-2.0
//
// Run with:
//   cargo run --example three_services
//
// Then Ctrl-C to shut down cleanly.
//
// Demonstrates:
// 1. `Topic<E>`     — live delta stream from `chain-source`.
// 2. `State<T>`     — current snapshot, served by `state-store`.
// 3. `OnError::Restart` — `chain-source` restarts on transient errors.
// 4. Readiness gating — `state-store` does fake "hydration", then calls
//    `mark_ready`; `ws-api` (which depends on it) waits for that.

use std::{
    sync::{
        Arc,
        atomic::{AtomicU32, Ordering},
    },
    time::Duration,
};

use runtime::{
    ExhaustedAction, OnError, RestartPolicy, RuntimeBuilder, RuntimeError, Service,
    ServiceContext, State, Topic, TopicConfig,
};
use tokio::sync::broadcast::error::RecvError;
use tracing::{info, warn};

// ============ domain types ============

#[derive(Debug, Clone)]
struct Tick {
    seq: u64,
}

#[derive(Debug, Clone, Default)]
struct AppSnapshot {
    last_seq: u64,
    total_seen: u64,
}

// ============ chain-source: produces ticks, fails every N for restart demo ============

#[derive(thiserror::Error, Debug)]
enum ChainSourceError {
    #[error("simulated upstream disconnect after {0} ticks")]
    SimulatedDisconnect(u64),
}

struct ChainSource {
    ticks: Topic<Tick>,
    fail_every: u64,
    counter: AtomicU32,
}

impl Service for ChainSource {
    const NAME: &'static str = "chain-source";
    type Error = ChainSourceError;

    fn on_error(&self) -> OnError {
        OnError::Restart(RestartPolicy {
            min_backoff: Duration::from_millis(200),
            max_backoff: Duration::from_secs(2),
            jitter: 0.2,
            max_retries: None,
            on_exhausted: ExhaustedAction::ShutdownRuntime,
        })
    }

    fn run(
        self: Arc<Self>,
        ctx: ServiceContext,
    ) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send {
        async move {
            let mut session_ticks = 0u64;
            let session = self.counter.fetch_add(1, Ordering::SeqCst) + 1;
            info!(session, "chain-source session begins");

            loop {
                tokio::select! {
                    _ = ctx.cancelled() => return Ok(()),
                    _ = tokio::time::sleep(Duration::from_millis(300)) => {
                        session_ticks += 1;

                        if session_ticks >= self.fail_every {
                            warn!(session, session_ticks, "simulated disconnect");
                            return Err(ChainSourceError::SimulatedDisconnect(session_ticks));
                        }

                        // The publish may fail if no subscriber is up yet — fine.
                        let seq = (session as u64 - 1) * self.fail_every + session_ticks;
                        let _ = self.ticks.publish(Tick { seq });
                    }
                }
            }
        }
    }
}

// ============ state-store: hydrates, then consumes ticks, holds snapshot ============

#[derive(thiserror::Error, Debug)]
enum StateStoreError {
    #[error("upstream lagged so far we lost messages")]
    Lagged,
}

struct StateStore {
    ticks: Topic<Tick>,
    snapshot: State<Arc<AppSnapshot>>,
}

impl Service for StateStore {
    const NAME: &'static str = "state-store";
    type Error = StateStoreError;

    // Default OnError = ShutdownRuntime — a state-store error IS fatal.

    fn run(
        self: Arc<Self>,
        ctx: ServiceContext,
    ) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send {
        async move {
            info!("state-store: simulating DB hydration (500ms)");
            tokio::time::sleep(Duration::from_millis(500)).await;
            info!("state-store: hydration complete; marking ready");
            ctx.mark_ready();

            let mut rx = self.ticks.subscribe();

            loop {
                tokio::select! {
                    _ = ctx.cancelled() => {
                        info!("state-store: shutting down");
                        return Ok(());
                    }
                    res = rx.recv() => match res {
                        Ok(tick) => {
                            self.snapshot.modify(|snap| {
                                let next = AppSnapshot {
                                    last_seq: tick.seq,
                                    total_seen: snap.total_seen + 1,
                                };
                                *snap = Arc::new(next);
                            });
                        }
                        Err(RecvError::Lagged(n)) => {
                            warn!(missed = n, "state-store: lagged");
                            // Could refill from DB here. For the demo, fatal.
                            return Err(StateStoreError::Lagged);
                        }
                        Err(RecvError::Closed) => {
                            info!("state-store: topic closed");
                            return Ok(());
                        }
                    }
                }
            }
        }
    }
}

// ============ ws-api: reads snapshots, prints them; depends on state-store ============

#[derive(thiserror::Error, Debug)]
enum WsApiError {} // never errors in this demo

struct WsApi {
    snapshot: State<Arc<AppSnapshot>>,
}

impl Service for WsApi {
    const NAME: &'static str = "ws-api";
    type Error = WsApiError;

    fn run(
        self: Arc<Self>,
        ctx: ServiceContext,
    ) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send {
        async move {
            info!("ws-api: starting (deps were gated; state-store IS hydrated by now)");

            let mut rx = self.snapshot.subscribe();
            // Print initial snapshot.
            let snap = self.snapshot.snapshot();
            info!(last_seq = snap.last_seq, total_seen = snap.total_seen, "initial snapshot");

            loop {
                tokio::select! {
                    _ = ctx.cancelled() => {
                        info!("ws-api: shutting down");
                        return Ok(());
                    }
                    res = rx.changed() => {
                        if res.is_err() {
                            // Sender dropped; treat as graceful exit.
                            return Ok(());
                        }
                        let snap = self.snapshot.snapshot();
                        info!(
                            last_seq = snap.last_seq,
                            total_seen = snap.total_seen,
                            "snapshot update",
                        );
                    }
                }
            }
        }
    }
}

// ============ main ============

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,runtime=info,three_services=info".into()),
        )
        .init();

    let ticks = Topic::<Tick>::new(TopicConfig { capacity: 256 });
    let snapshot = State::<Arc<AppSnapshot>>::new(Arc::new(AppSnapshot::default()));

    let chain_source = Arc::new(ChainSource {
        ticks: ticks.clone(),
        fail_every: 5,
        counter: AtomicU32::new(0),
    });

    let state_store = Arc::new(StateStore {
        ticks: ticks.clone(),
        snapshot: snapshot.clone(),
    });

    let ws_api = Arc::new(WsApi {
        snapshot: snapshot.clone(),
    });

    let rt: Result<_, RuntimeError> = Ok(RuntimeBuilder::new()
        .service(chain_source)?
        .service(state_store)?
        .service_with_deps(ws_api, &["state-store"])?
        .shutdown_timeout(Duration::from_secs(3))
        .build());

    rt?.run_with_ctrl_c().await?;
    Ok(())
}
