// Copyright 2026 runtime contributors
// SPDX-License-Identifier: Apache-2.0

//! Supervised, dependency-ordered runtime with readiness gating and per-service
//! restart policies.
//!
//! See [`Runtime`] and [`RuntimeBuilder`] for the entry points.

mod builder;
mod dag;
mod supervisor;
mod types;

pub use builder::RuntimeBuilder;
pub use dag::DagError;
pub use types::{Runtime, RuntimeError, RuntimeHandle, ShutdownReason};
