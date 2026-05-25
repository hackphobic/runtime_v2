// Copyright 2026 runtime contributors
// SPDX-License-Identifier: Apache-2.0

use std::{sync::Arc, time::Duration};

use crate::{
    service::{DynService, Service, ServiceAdapter, ServiceName, ServiceSpec, ServiceStartError},
    util::dedup_preserve_order,
};

use super::types::{Runtime, RuntimeInner};

/// Builder for [`Runtime`].
///
/// Services are registered with explicit dependencies (by name). Validation —
/// duplicate names, unknown deps, cycles — happens partly here ([`Duplicate`])
/// and partly at run time when the supervisor topologically sorts the graph.
///
/// [`Duplicate`]: ServiceStartError::Duplicate
pub struct RuntimeBuilder {
    specs: Vec<ServiceSpec>,
    shutdown_timeout: Option<Duration>,
}

impl RuntimeBuilder {
    /// Start a new builder with no services and the default shutdown drain timeout.
    #[must_use]
    pub fn new() -> Self {
        Self {
            specs: Vec::new(),
            shutdown_timeout: None,
        }
    }

    /// Override the default shutdown drain timeout.
    ///
    /// This is how long the supervisor waits for services to finish after their
    /// cancellation tokens have been triggered, before giving up with
    /// [`RuntimeError::ShutdownTimeout`](crate::RuntimeError::ShutdownTimeout).
    #[must_use]
    pub fn shutdown_timeout(mut self, d: Duration) -> Self {
        self.shutdown_timeout = Some(d);
        self
    }

    /// Register a service with no dependencies.
    pub fn service<S: Service>(self, service: Arc<S>) -> Result<Self, ServiceStartError> {
        self.service_with_deps(service, &[])
    }

    /// Register a service that depends on the named services.
    ///
    /// Dependencies are referenced by [`Service::NAME`]. Duplicate dep names are
    /// silently deduped. Unknown or cyclic dependencies are reported at
    /// [`Runtime::run`](crate::Runtime::run) time as a
    /// [`RuntimeError::Dag`](crate::RuntimeError::Dag).
    pub fn service_with_deps<S: Service>(
        mut self,
        service: Arc<S>,
        deps: &[ServiceName],
    ) -> Result<Self, ServiceStartError> {
        let name = S::NAME;
        if self.specs.iter().any(|s| s.name == name) {
            return Err(ServiceStartError::Duplicate(name));
        }

        let deps = dedup_preserve_order(deps);
        let adapter: Arc<dyn DynService> = Arc::new(ServiceAdapter(service));

        self.specs.push(ServiceSpec {
            name,
            deps,
            adapter,
        });
        Ok(self)
    }

    /// Build the runtime.
    #[must_use]
    pub fn build(self) -> Runtime {
        let names: Vec<ServiceName> = self.specs.iter().map(|s| s.name).collect();
        Runtime {
            inner: Arc::new(RuntimeInner::new(self.shutdown_timeout, &names)),
            specs: self.specs,
        }
    }
}

impl Default for RuntimeBuilder {
    fn default() -> Self {
        Self::new()
    }
}
