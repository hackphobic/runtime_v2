# Changelog

All notable changes to `runtime` will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [3.1.0] — 2026-05-25

Backwards-compatible release adding three primitives for service-oriented apps
with real-time state. v3.0 services compile unchanged: every new trait method
has a default that preserves v3.0 semantics.

### Added

- **`State<T>`** — snapshot primitive backed by `tokio::sync::watch`,
  counterpart to `Topic<E>`. Where `Topic<E>` is "live deltas only, late
  subscribers miss the past," `State<T>` is "always has a current value, new
  subscribers immediately see it." Methods: `borrow`/`snapshot`/`set`/`modify`/
  `modify_if`/`subscribe`/`receiver_count`. The `borrow`/`snapshot` split
  intentionally guides users away from holding a read guard across `.await`
  (which would deadlock writers).

- **Per-service `OnError` policy** with three variants:
  - `OnError::ShutdownRuntime` — current v3.0 behavior; remains the default.
  - `OnError::Restart(RestartPolicy)` — restart the service after a backoff.
    Policy: `min_backoff`, `max_backoff`, `jitter` (proportional, default
    `0.2`), `max_retries` (`None` = unlimited), `on_exhausted`
    (`ShutdownRuntime` | `Ignore`).
  - `OnError::Ignore` — log and let the service stay exited; runtime
    continues.

  Configured per service via `fn on_error(&self) -> OnError { ... }` on the
  `Service` trait, with a default that returns `ShutdownRuntime`.

- **Readiness signaling**. The supervisor now gates each service on its
  declared dependencies' readiness, not just topological spawn order.
  - `Service::auto_ready(&self) -> bool` (default `true`): if `true`, the
    supervisor marks the service ready as soon as `run()` is invoked
    (preserving v3.0 behavior). Override to `false` for services with real
    init work (DB hydration, listener bind), and call
    `ServiceContext::mark_ready()` when init completes.
  - `RuntimeHandle::mark_ready` / `is_ready` / `await_ready` for out-of-band
    inspection. Readiness slots are pre-created for all registered services
    at `build()` time so `await_ready` for an unknown name returns
    immediately with a warning rather than hanging forever.

- **`examples/three_services.rs`** — runnable demo wiring `chain-source`,
  `state-store`, `ws-api` using `Topic<Tick>` + `State<Arc<AppSnapshot>>` +
  readiness gating + `OnError::Restart`. Compiled by CI.

### Changed

- The supervisor now tracks each running task with an index so it can route
  per-task results back to the correct service's policy. No user-visible
  change.

- `Cargo.toml`: added `fastrand = "2"` (zero transitive deps) for restart
  jitter.

### Migration from 3.0

None required. Every existing service compiles and behaves identically:
- `auto_ready()` defaults to `true` → supervisor marks ready on spawn, same as
  v3.0 (which had no readiness concept; dependents started in parallel).
- `on_error()` defaults to `ShutdownRuntime` → any service error tears down
  the runtime, matching v3.0.

To opt into new behavior, override either or both methods on individual
services.

## [3.0.0] — 2026-05-25

This is a major architectural rewrite. The crate has been redesigned around
modern Rust async primitives — `CancellationToken`, `JoinSet`, broadcast topics,
and a real supervised runtime — rather than the 2020-era patterns the original
crate was built on. Most of the public API has been replaced.

### Added

- **`Runtime` + `RuntimeBuilder`** — a supervised, dependency-ordered runtime
  for services. Builders accept services and explicit dependency lists; the
  runtime topologically sorts at startup time and runs all services to
  completion or shutdown.
- **`Service` trait** with `const NAME: &'static str`, a typed `Error`, and
  `fn run(self: Arc<Self>, ctx: ServiceContext) -> impl Future + Send`. Uses
  native RPITIT — no `async_trait`, no boxed futures in the public API.
- **`ServiceContext`** — handed to every service: per-service `CancellationToken`
  (child of the runtime's root), a cloneable `RuntimeHandle`, and the service name.
- **`Topic<E>`** — typed publish/subscribe over `tokio::sync::broadcast`,
  carrying `Arc<E>` for zero-copy fan-out. Replaces the old untyped `Bus`.
- **`DynServiceError`** — boundary error type preserving the offending service's
  name; supports `downcast_ref::<E>()` to recover concrete error types.
- **`DagError`** — surfaces `UnknownDependency`, `Cycle`, `SelfDependency`
  failures from startup ordering.
- **`Runtime::run_with_ctrl_c`** — supervised run that installs an OS signal
  handler and shuts down with `ShutdownReason::Signal` on Ctrl-C.
- **`ShutdownStream::from_cancellation_token`** — convenience constructor for
  the common case of binding cancellation to a `CancellationToken`.

### Changed

- **Edition** bumped to `2024`; MSRV declared as `1.85`.
- **`ShutdownStream`** is now generic over any `Future<Output = ()> + Unpin`
  (not specialized to `oneshot::Receiver<()>` as before) and any `Stream + Unpin`
  (no longer requiring `FusedStream`). The `Fuse` wrapping is gone; an internal
  `Option<S>` tracks termination.
- **`ResourceHandle`** modernized: uses `tracing::warn!` (instead of `log::warn!`),
  uses a named `Inner` struct (instead of a tuple), and is poison-resilient on
  its internal `Mutex`.
- **Logging** moved from `log` to `tracing` throughout, with per-service
  `tracing::Span`s applied via `Instrument`.
- **Errors** use `thiserror = "2"` throughout.

### Removed

- **`Bus`** and the `event::Bus`/`event::EventListener` types. Replaced by the
  typed `Topic<E>`. No `Any` downcasting, no string topic names.
- **`Worker` trait** and the `node` module (`Node`, `NodeBuilder`). Replaced by
  `Service` + `Runtime` + `RuntimeBuilder`.
- **`bee-storage` integration** in the `Node` trait. The runtime is now storage-
  agnostic; services inject their own dependencies.
- **`dashmap`**, **`async-trait`**, **`log`**, and the full `futures` dependency
  are no longer required. The crate now depends only on `futures-core` (for
  `Stream`), `thiserror`, `tokio` (with a minimal feature set:
  `rt`/`sync`/`macros`/`time`/`signal`), `tokio-util` (default features only;
  `CancellationToken` is unconditional), and `tracing`.
- **TypeId-based dependency declaration** on `Worker`. Services now declare
  dependencies by `&'static str` name, matching `Service::NAME` — debuggable in
  logs, with cycle/missing-dep validation surfaced as a typed error.

### Migration notes

Mapping from old types to new:

| Old (≤ 2.x)             | New (3.0)                                             |
| ----------------------- | ----------------------------------------------------- |
| `Bus`                   | `Topic<E>` (one topic per event type)                 |
| `Worker`                | `Service`                                             |
| `Node` / `NodeBuilder`  | `Runtime` / `RuntimeBuilder`                          |
| `Resources` (typemap)   | explicit DI: pass `Arc<T>` to service constructors    |
| `shutdown_stream` arg   | `ServiceContext::cancel` (use `ctx.cancelled().await`) |
| `log::*`                | `tracing::*`                                          |
| `oneshot::Receiver<()>` shutdown | `CancellationToken` (with `from_cancellation_token` for `ShutdownStream`) |

A typical worker → service translation:

```rust
// Old (2.x):
//   #[async_trait]
//   impl Worker<N> for MyWorker {
//       type Config = MyConfig;
//       type Error = MyError;
//       fn dependencies() -> &'static [TypeId] { &[TypeId::of::<Dep>()] }
//       async fn start(node: &mut N, config: Self::Config) -> Result<Self, Self::Error> { ... }
//   }

// New (3.0):
impl Service for MyService {
    const NAME: &'static str = "my-service";
    type Error = MyError;
    fn run(self: Arc<Self>, ctx: ServiceContext)
        -> impl std::future::Future<Output = Result<(), Self::Error>> + Send
    {
        async move {
            loop {
                tokio::select! {
                    _ = ctx.cancelled() => return Ok(()),
                    // ...
                }
            }
        }
    }
}

// And declare dependency by name at build time:
//   RuntimeBuilder::new()
//       .service(dep)?
//       .service_with_deps(my_service, &["dep"])?
//       .build();
```

## [1.0.0-alpha.1] — 2021-04-19

Initial release of the original `runtime` crate. (See git history for prior changes.)
