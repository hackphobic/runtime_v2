// SPDX-License-Identifier: Apache-2.0

//! Owning, usage-tracked handles for sharing resources between services.
//!
//! [`ResourceHandle<R>`] is essentially `Arc<R>` with one extra capability: each
//! clone records the source location of where it was made, and [`try_unwrap`]
//! emits a diagnostic listing those locations if there are still outstanding
//! clones — useful for finding leaked references during shutdown.
//!
//! This module is independent of the runtime — services typically inject their
//! dependencies as plain [`Arc`]s. Use `ResourceHandle` when you want the leak
//! diagnostic.
//!
//! [`try_unwrap`]: ResourceHandle::try_unwrap

use std::{
    any::type_name,
    collections::HashMap,
    fmt::Write as _,
    ops::Deref,
    panic::Location,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicUsize, Ordering},
    },
};

use tracing::warn;

static RESOURCE_ID: AtomicUsize = AtomicUsize::new(0);

type UsageMap = HashMap<usize, &'static Location<'static>>;

struct Inner<R> {
    res: R,
    usages: Mutex<UsageMap>,
}

/// An owning handle to a shared resource.
///
/// Clones share the same underlying resource (like `Arc<R>`) but each clone is
/// tracked individually so that [`try_unwrap`](Self::try_unwrap) can report
/// where outstanding clones were created if the resource can't be reclaimed.
pub struct ResourceHandle<R> {
    id: Option<usize>,
    inner: Arc<Inner<R>>,
}

impl<R> ResourceHandle<R> {
    /// Wrap a resource for shared immutable access.
    #[must_use]
    pub fn new(res: R) -> Self {
        Self {
            id: None,
            inner: Arc::new(Inner {
                res,
                usages: Mutex::new(HashMap::new()),
            }),
        }
    }

    /// Convert this owned handle into a [`WeakHandle`].
    #[must_use]
    pub fn into_weak(self) -> WeakHandle<R> {
        let inner = Arc::downgrade(&self.inner);
        drop(self);
        WeakHandle { inner }
    }

    /// Attempt to reclaim ownership of the underlying resource.
    ///
    /// Returns `Some(res)` if this was the only outstanding handle, or `None`
    /// otherwise. On failure, a `WARN`-level tracing event is emitted listing
    /// the source locations of all outstanding clones — usually enough to find
    /// the task or listener that didn't get stopped during shutdown.
    pub fn try_unwrap(self) -> Option<R> {
        let inner = Arc::clone(&self.inner);
        drop(self);
        match Arc::try_unwrap(inner) {
            Ok(Inner { res, .. }) => Some(res),
            Err(inner) => {
                let guard = inner
                    .usages
                    .lock()
                    .unwrap_or_else(|p| p.into_inner());
                let mut buf = String::new();
                for loc in guard.values() {
                    let _ = write!(buf, "\n  - {loc}");
                }
                warn!(
                    resource = type_name::<R>(),
                    "could not reclaim resource — {} outstanding clone(s):{}",
                    guard.len(),
                    buf,
                );
                None
            }
        }
    }
}

impl<R> Clone for ResourceHandle<R> {
    #[track_caller]
    fn clone(&self) -> Self {
        let new_id = RESOURCE_ID.fetch_add(1, Ordering::Relaxed);
        self.inner
            .usages
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(new_id, Location::caller());
        Self {
            id: Some(new_id),
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<R> Deref for ResourceHandle<R> {
    type Target = R;

    fn deref(&self) -> &Self::Target {
        &self.inner.res
    }
}

impl<R> Drop for ResourceHandle<R> {
    fn drop(&mut self) {
        if let Some(id) = self.id {
            if let Ok(mut guard) = self.inner.usages.lock() {
                guard.remove(&id);
            }
        }
    }
}

/// A non-owning handle to a shared resource. Upgrade to [`ResourceHandle`] to use.
pub struct WeakHandle<R> {
    inner: Weak<Inner<R>>,
}

impl<R> WeakHandle<R> {
    /// Try to upgrade to a [`ResourceHandle`] if the resource is still alive.
    #[track_caller]
    #[must_use]
    pub fn upgrade(&self) -> Option<ResourceHandle<R>> {
        let inner = self.inner.upgrade()?;
        let new_id = RESOURCE_ID.fetch_add(1, Ordering::Relaxed);
        inner
            .usages
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(new_id, Location::caller());
        Some(ResourceHandle {
            id: Some(new_id),
            inner,
        })
    }
}

impl<R> Clone for WeakHandle<R> {
    fn clone(&self) -> Self {
        Self {
            inner: Weak::clone(&self.inner),
        }
    }
}
