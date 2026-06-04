# Changelog

All notable changes to `runtime` will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0]

Initial release. A structured-concurrency runtime for service-oriented async
applications, built on modern Rust async primitives.

### Added

- **`Runtime` + `RuntimeBuilder`** — a supervised, dependency-ordered runtime.
  Services are registered with explicit dependency lists (by `Service::NAME`).
  The builder rejects duplicate names eagerly; the dependency graph is
  topologically sorted at `run()` time, surfacing `UnknownDependency`, `Cycle`,
  and `SelfDependency` as a typed `RuntimeError::Dag`.
- **`Service` trait** — `const NAME: &'static str`, a typed `Error`, and
  `fn run(self: Arc<Self>, ctx: ServiceContext) -> impl Future + Send`. Native
  RPITIT — no `async_trait`, no boxed futures in the public API.
- **`ServiceContext`** — handed to every service: a per-service
  `CancellationToken` (child of the runtime's root token), a cloneable
  `RuntimeHandle`, and the service name.
- **Explicit readiness** — a service is gated behind its declared dependencies,
  and each dependency stays un-ready until it calls
  `ServiceContext::mark_ready`. Dependents block until then.
  `RuntimeHandle::mark_ready` / `is_ready` / `await_ready` are exposed for
  out-of-band inspection.
- **Per-service `OnError` policy** — `ShutdownRuntime` (default),
  `Restart(RestartPolicy)` with exponential backoff + jitter and an optional
  retry cap (`on_exhausted`: `ShutdownRuntime` | `Ignore`), or `Ignore`.
- **`DynServiceError`** — boundary error preserving the offending service's
  name; `downcast_ref::<E>()` recovers the concrete error type.
- **`Topic<E>`** — typed publish/subscribe over `tokio::sync::broadcast`,
  carrying `Arc<E>` for zero-copy fan-out.
- **`State<T>`** — snapshot primitive over `tokio::sync::watch`; always has a
  current value that new subscribers see immediately.
- **`Runtime::run_with_ctrl_c`** — supervised run that shuts down with
  `ShutdownReason::Signal` on Ctrl-C.
- **`tracing`** instrumentation with per-service spans.

### Notes

- `edition = "2024"`, `rust-version = "1.85"`, `#![forbid(unsafe_code)]`.
- Dependencies: `fastrand` (restart jitter), `thiserror`, `tokio` (minimal
  feature set), `tokio-util` (`CancellationToken`), and `tracing`.
