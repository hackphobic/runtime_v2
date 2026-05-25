// Copyright 2026 runtime contributors
// SPDX-License-Identifier: Apache-2.0

//! [`State<T>`] — a snapshot primitive backed by [`tokio::sync::watch`].
//!
//! Counterpart to [`Topic<E>`](crate::Topic): where `Topic<E>` is "live deltas
//! only, late subscribers miss the past," `State<T>` is "always has a current
//! value, new subscribers immediately see it." Use it for app state that a
//! newly-connected client needs to read before subscribing to live updates.
//!
//! For large state, wrap the inner type in an [`Arc`](std::sync::Arc) so
//! [`snapshot`](State::snapshot) is a refcount clone rather than a deep copy.
//!
//! # Example
//!
//! ```no_run
//! use std::sync::Arc;
//! use runtime::State;
//!
//! # async fn example() {
//! // For "large" snapshot state, wrap in Arc so snapshot() is cheap.
//! let state: State<Arc<Vec<u64>>> = State::new(Arc::new(Vec::new()));
//!
//! // Update in place — efficient and notifies subscribers.
//! state.modify(|v| Arc::make_mut(v).push(42));
//!
//! // Cheap snapshot for a fresh read.
//! let snap: Arc<Vec<u64>> = state.snapshot();
//! assert_eq!(snap.as_slice(), &[42]);
//!
//! // Subscribe to get notified of subsequent changes.
//! let mut rx = state.subscribe();
//! state.modify(|v| Arc::make_mut(v).push(43));
//! rx.changed().await.unwrap();
//! # }
//! ```

use std::fmt;

use tokio::sync::watch;

/// Snapshot state with change notifications.
///
/// Cheap to clone; all clones share the same underlying channel.
pub struct State<T> {
    tx: watch::Sender<T>,
}

impl<T> State<T>
where
    T: Send + Sync + 'static,
{
    /// Create a new state cell with the given initial value.
    #[must_use]
    pub fn new(initial: T) -> Self {
        let (tx, _) = watch::channel(initial);
        Self { tx }
    }

    /// Borrow the current value without cloning.
    ///
    /// The returned guard holds an internal read lock — **do not hold it across
    /// an `.await` point**, or [`set`](Self::set) / [`modify`](Self::modify)
    /// calls from other tasks will block. Use [`snapshot`](Self::snapshot)
    /// instead for the "load and release" pattern.
    pub fn borrow(&self) -> watch::Ref<'_, T> {
        self.tx.borrow()
    }

    /// Take a snapshot of the current value.
    ///
    /// Safe to hold across `.await` points. For large `T`, prefer
    /// `State<Arc<U>>` so this is just a refcount bump.
    pub fn snapshot(&self) -> T
    where
        T: Clone,
    {
        self.tx.borrow().clone()
    }

    /// Replace the current value. Notifies all active subscribers.
    pub fn set(&self, value: T) {
        // send_replace never errors; it discards the old value (and would also
        // succeed with no receivers, unlike send()).
        let _ = self.tx.send_replace(value);
    }

    /// Update the current value in place. Notifies all active subscribers.
    pub fn modify<F>(&self, f: F)
    where
        F: FnOnce(&mut T),
    {
        self.tx.send_modify(f);
    }

    /// Update the current value in place; only notify subscribers if `f`
    /// returns `true`.
    pub fn modify_if<F>(&self, f: F) -> bool
    where
        F: FnOnce(&mut T) -> bool,
    {
        self.tx.send_if_modified(f)
    }

    /// Subscribe to changes. The returned receiver also exposes the current
    /// value (call `borrow()` / `borrow_and_update()` on it).
    pub fn subscribe(&self) -> watch::Receiver<T> {
        self.tx.subscribe()
    }

    /// Current number of active subscribers.
    #[must_use]
    pub fn receiver_count(&self) -> usize {
        self.tx.receiver_count()
    }
}

impl<T> Clone for State<T> {
    fn clone(&self) -> Self {
        Self { tx: self.tx.clone() }
    }
}

impl<T> fmt::Debug for State<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("State")
            .field("receiver_count", &self.tx.receiver_count())
            .finish_non_exhaustive()
    }
}
