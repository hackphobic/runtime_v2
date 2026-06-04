// Copyright 2026 runtime contributors
// SPDX-License-Identifier: Apache-2.0

//! A modern, structured-concurrency runtime for service-oriented async applications.
//!
//! It provides:
//!
//! - [`Service`] + [`ServiceContext`] — a trait for long-running units of work
//!   driven by an async `run` method with typed errors and per-service
//!   [`OnError`] policy (`ShutdownRuntime` | `Restart { backoff, .. }` | `Ignore`).
//! - [`Runtime`] + [`RuntimeBuilder`] — a supervisor that starts services in
//!   topological order, **gates each on its declared dependencies' readiness**,
//!   propagates cancellation through a tree of
//!   [`CancellationToken`](tokio_util::sync::CancellationToken)s, drains
//!   shutdown within a configurable timeout, and surfaces typed failures.
//! - [`Topic`] — typed publish-subscribe on top of [`tokio::sync::broadcast`].
//!   For *delta* streams where late subscribers see only new events.
//! - [`State`] — snapshot primitive on top of [`tokio::sync::watch`].
//!   For *current value* where new subscribers immediately see the latest.
//!
//! # Topic vs State
//!
//! These two primitives are complements, not alternatives:
//!
//! | Need                                              | Use         |
//! |---------------------------------------------------|-------------|
//! | "Stream of newly-confirmed events"                | [`Topic`]   |
//! | "What's the current value? Plus future changes."  | [`State`]   |
//!
//! A typical pattern for a real-time API: serve a [`State::snapshot`] to a new
//! client, *then* hook them up to a [`Topic`] for live deltas.
//!
//! # Readiness
//!
//! `RuntimeBuilder` topologically sorts service dependencies, but spawn order
//! alone doesn't guarantee a dependent service sees its dep in a *usable* state.
//! Readiness is therefore **explicit**: a service is gated behind its declared
//! dependencies, and each dependency stays un-ready until it calls
//! [`ServiceContext::mark_ready`]. Any service that others depend on must call
//! `mark_ready()` once its initialization (DB hydration, listener bind, initial
//! sync) is complete; dependents block until then. A service that nothing
//! depends on need not call it.
//!
//! # Quick example
//!
//! ```no_run
//! use std::{sync::Arc, time::Duration};
//! use runtime::{RuntimeBuilder, Service, ServiceContext, Topic, TopicConfig};
//!
//! #[derive(Debug, Clone)]
//! struct Tick(u64);
//!
//! #[derive(thiserror::Error, Debug)]
//! enum TickerError {
//!     #[error("no subscribers")]
//!     NoSubscribers,
//! }
//!
//! struct Ticker { ticks: Topic<Tick> }
//!
//! impl Service for Ticker {
//!     const NAME: &'static str = "ticker";
//!     type Error = TickerError;
//!
//!     fn run(
//!         self: Arc<Self>,
//!         ctx: ServiceContext,
//!     ) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send {
//!         async move {
//!             ctx.mark_ready();
//!             let mut n = 0u64;
//!             loop {
//!                 tokio::select! {
//!                     _ = ctx.cancelled() => return Ok(()),
//!                     _ = tokio::time::sleep(Duration::from_millis(100)) => {
//!                         n += 1;
//!                         let _ = self.ticks.publish(Tick(n));
//!                     }
//!                 }
//!             }
//!         }
//!     }
//! }
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let ticks = Topic::<Tick>::new(TopicConfig { capacity: 64 });
//! let ticker = Arc::new(Ticker { ticks });
//!
//! let rt = RuntimeBuilder::new()
//!     .service(ticker)?
//!     .shutdown_timeout(Duration::from_secs(3))
//!     .build();
//!
//! rt.run_with_ctrl_c().await?;
//! # Ok(())
//! # }
//! ```

#![deny(missing_docs)]
#![forbid(unsafe_code)]

pub mod event;
pub mod runtime;
pub mod service;
pub mod state;

mod util;

#[doc(inline)]
pub use event::{Topic, TopicClosed, TopicConfig};
#[doc(inline)]
pub use runtime::{
    DagError, Runtime, RuntimeBuilder, RuntimeError, RuntimeHandle, ShutdownReason,
};
#[doc(inline)]
pub use service::{
    DynServiceError, ExhaustedAction, OnError, RestartPolicy, Service, ServiceContext,
    ServiceName, ServiceStartError,
};
#[doc(inline)]
pub use state::State;
