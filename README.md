# runtime

A modern, structured-concurrency runtime for service-oriented async Rust applications.

`runtime` is a ground-up rewrite of the original 2020 crate. It keeps the
spirit of the original — supervised services, a shared event bus, deterministic
shutdown — but reimplements every primitive on modern Rust async idioms:

- Native `async fn` in traits (no `async_trait` macro, no boxed futures in the
  public API)
- [`tokio_util::sync::CancellationToken`] for tree-structured cancellation
- [`tokio::task::JoinSet`] for structured task supervision
- [`tokio::sync::broadcast`] for typed pub/sub
- [`tracing`] (with per-service spans) instead of `log`
- [`thiserror`] for typed errors at every layer
- DAG-based startup ordering with cycle detection

The crate is `edition = "2024"`, `rust-version = "1.85"`, and
`#![forbid(unsafe_code)]`.

## At a glance

| Concern         | Primitive                                                       |
| --------------- | --------------------------------------------------------------- |
| Service unit    | [`Service`] trait with `const NAME` and typed `Error`           |
| Service context | [`ServiceContext`] (cancel token + runtime handle + name)       |
| Supervision     | [`Runtime`] + [`RuntimeBuilder`]                                |
| Cancellation    | [`CancellationToken`] tree (root → per-service children)        |
| Shutdown        | [`RuntimeHandle::request_shutdown`] + drain timeout             |
| Delta pub/sub   | [`Topic<E>`] (typed broadcast, `Arc<E>` payloads)               |
| Snapshot state  | [`State<T>`] (typed watch, always has current value)            |
| Failure policy  | [`OnError`] per service (`ShutdownRuntime` \| `Restart` \| `Ignore`) |
| Readiness       | `Service::auto_ready` + `ServiceContext::mark_ready` + DAG gating |
| Resources       | [`ResourceHandle`] (`Arc<T>` + clone-site tracking for leaks)   |

### Topic vs State

These two are complements, not alternatives:

| Need                                              | Use         |
|---------------------------------------------------|-------------|
| "Stream of newly-confirmed events"                | `Topic<E>`  |
| "What's the current value? Plus future changes."  | `State<T>`  |

Typical real-time API pattern: serve a `State::snapshot()` to a new client,
*then* hook them up to a `Topic` for live deltas.

### Readiness

`RuntimeBuilder` topologically sorts service dependencies, but spawn order
alone doesn't guarantee a dependent service sees its dep in a usable state.
By default, services are auto-marked ready as soon as `run()` is invoked
(preserving v3.0 semantics). Services with real init work — DB hydration,
listener bind, initial chain sync — should override `Service::auto_ready` to
return `false` and call `ServiceContext::mark_ready` when init completes.
Dependents wait until then before their own `run()` is invoked.

### Failure policy

Each `Service` declares an `OnError` policy via `fn on_error(&self) -> OnError`:

- `OnError::ShutdownRuntime` — default; any error in `run()` tears down the
  runtime with `ShutdownReason::ServiceFailed`.
- `OnError::Restart(RestartPolicy)` — exponential backoff + jitter, optional
  retry cap. On exhaustion: `ShutdownRuntime` or `Ignore`.
- `OnError::Ignore` — log and exit this service; runtime keeps going.

## Example

```rust
use std::{sync::Arc, time::Duration};

use runtime::{
    RuntimeBuilder, Service, ServiceContext, ShutdownReason, Topic, TopicConfig,
};
use tokio::sync::broadcast;
use tracing::{info, warn};

#[derive(Debug, Clone)]
struct Tick(u64);

#[derive(thiserror::Error, Debug)]
enum TickerError {
    #[error("no subscribers left")]
    NoSubscribers,
}

struct Ticker {
    ticks: Topic<Tick>,
}

impl Service for Ticker {
    const NAME: &'static str = "ticker";
    type Error = TickerError;

    fn run(
        self: Arc<Self>,
        ctx: ServiceContext,
    ) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send {
        async move {
            let mut n = 0u64;
            loop {
                tokio::select! {
                    _ = ctx.cancelled() => {
                        info!("ticker: cancelled");
                        return Ok(());
                    }
                    _ = tokio::time::sleep(Duration::from_millis(200)) => {
                        n += 1;
                        if self.ticks.publish(Tick(n)).is_err() {
                            warn!("no subscribers; stopping");
                            return Err(TickerError::NoSubscribers);
                        }
                    }
                }
            }
        }
    }
}

#[derive(thiserror::Error, Debug)]
enum PrinterError {
    #[error("recv: {0}")]
    Recv(#[from] broadcast::error::RecvError),
}

struct Printer {
    rx: broadcast::Receiver<Arc<Tick>>,
}

impl Service for Printer {
    const NAME: &'static str = "printer";
    type Error = PrinterError;

    fn run(
        self: Arc<Self>,
        ctx: ServiceContext,
    ) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send {
        async move {
            // The receiver is in self; resubscribe inside the task so we own a
            // fresh receiver and clones of Self don't share consumption state.
            let mut rx = self.rx.resubscribe();
            loop {
                tokio::select! {
                    _ = ctx.cancelled() => {
                        info!("printer: cancelled");
                        return Ok(());
                    }
                    msg = rx.recv() => {
                        let tick = msg?;
                        info!(tick = tick.0, "tick received");
                        if tick.0 >= 10 {
                            warn!("printer: requesting shutdown");
                            ctx.runtime.request_shutdown(ShutdownReason::Requested);
                        }
                    }
                }
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt().with_env_filter("info").init();

    let ticks = Topic::<Tick>::new(TopicConfig { capacity: 64 });
    let ticker = Arc::new(Ticker { ticks: ticks.clone() });
    let printer = Arc::new(Printer { rx: ticks.subscribe() });

    let rt = RuntimeBuilder::new()
        .service(ticker)?
        .service_with_deps(printer, &["ticker"])?
        .shutdown_timeout(Duration::from_secs(3))
        .build();

    rt.run_with_ctrl_c().await?;
    Ok(())
}
```

Note that `RuntimeBuilder::service*` returns `ServiceStartError` (duplicate
service names) while `Runtime::run` returns `RuntimeError` — they're separate
error types because they correspond to separate phases. Use `Box<dyn Error>`
or your own error type to unify them with `?`.

## Lifecycle

1. [`RuntimeBuilder`] collects services and their declared dependencies (by
   `Service::NAME`). Duplicate names are rejected immediately.
2. On [`Runtime::run`], the supervisor:
   - validates the DAG (unknown deps, self-loops, cycles → [`RuntimeError::Dag`]);
   - topologically sorts services and spawns each into a [`JoinSet`], handing it
     a [`ServiceContext`] whose `cancel` is a child of the runtime's root token;
   - runs a `select!` loop on either the root cancellation or any task finishing.
3. On shutdown (explicit, signal, or a service error), the root token is
   cancelled — propagating to every per-service child token — and the supervisor
   drains the `JoinSet` within the configured `shutdown_timeout`. Failure to
   drain yields [`RuntimeError::ShutdownTimeout`].
4. A service returning `Ok(())` exits gracefully and the runtime keeps going.
   A service returning `Err(_)` triggers a runtime-wide shutdown with reason
   [`ShutdownReason::ServiceFailed`]; its typed error is preserved inside
   [`DynServiceError`] and is recoverable via `downcast_ref::<E>()`.

## What this isn't

- An actor framework. Services are independent units; communication between them
  is via your own channels ([`Topic`], `tokio::sync::mpsc`, `tokio::sync::watch`, etc.).
- A service locator. Resources are injected explicitly when you construct each
  `Arc<S>` for the builder. [`ResourceHandle`] is available if you want clone-
  site tracking, but it is optional.
- A multi-runtime abstraction. Built squarely on `tokio`.

## Migration from 2.x and earlier

This is a major version. The `Bus`, `Worker`, `Node`, and global `Resources`
types are gone. See `CHANGELOG.md` for the mapping.

