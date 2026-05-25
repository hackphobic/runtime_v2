// Copyright 2026 runtime contributors
// SPDX-License-Identifier: Apache-2.0

//! Typed publish/subscribe over [`tokio::sync::broadcast`].
//!
//! [`Topic<E>`] replaces the original crate's untyped `Bus`. Each topic carries one
//! event type, subscribers receive `Arc<E>` (zero-copy fan-out), and laggy
//! subscribers receive a [`broadcast::error::RecvError::Lagged`] so back-pressure
//! is observable rather than silent.
//!
//! # Example
//!
//! ```no_run
//! use std::sync::Arc;
//! use runtime::{Topic, TopicConfig};
//!
//! # async fn example() {
//! let topic: Topic<&'static str> = Topic::new(TopicConfig { capacity: 8 });
//! let mut rx = topic.subscribe();
//!
//! topic.publish("hello").expect("at least one subscriber");
//!
//! let msg: Arc<&str> = rx.recv().await.unwrap();
//! assert_eq!(*msg, "hello");
//! # }
//! ```

use std::{fmt, sync::Arc};

use tokio::sync::broadcast;

/// Configuration for a [`Topic`].
#[derive(Clone, Copy, Debug)]
pub struct TopicConfig {
    /// Ring buffer capacity. Lagging subscribers start dropping messages when
    /// more than `capacity` events are in flight for them. Minimum effective
    /// value is 1.
    pub capacity: usize,
}

impl Default for TopicConfig {
    fn default() -> Self {
        Self { capacity: 1024 }
    }
}

/// Typed publish/subscribe channel.
///
/// Cheap to clone; all clones share one underlying ring buffer. Events are
/// wrapped in [`Arc`] so fan-out to many subscribers is allocation-free.
pub struct Topic<E> {
    tx: broadcast::Sender<Arc<E>>,
}

impl<E> Clone for Topic<E> {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
        }
    }
}

impl<E: Send + Sync + 'static> Topic<E> {
    /// Create a new topic with the given configuration.
    #[must_use]
    pub fn new(cfg: TopicConfig) -> Self {
        let (tx, _) = broadcast::channel(cfg.capacity.max(1));
        Self { tx }
    }

    /// Create a new topic with the default capacity.
    #[must_use]
    pub fn with_default_capacity() -> Self {
        Self::new(TopicConfig::default())
    }

    /// Publish an event to all current subscribers.
    ///
    /// Returns the number of receivers the event was delivered to, or
    /// [`TopicClosed`] if there are no active subscribers.
    pub fn publish(&self, event: E) -> Result<usize, TopicClosed> {
        self.tx.send(Arc::new(event)).map_err(|_| TopicClosed)
    }

    /// Subscribe to this topic, returning a fresh broadcast receiver.
    pub fn subscribe(&self) -> broadcast::Receiver<Arc<E>> {
        self.tx.subscribe()
    }

    /// Current number of active subscribers.
    #[must_use]
    pub fn receiver_count(&self) -> usize {
        self.tx.receiver_count()
    }
}

impl<E> fmt::Debug for Topic<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Topic")
            .field("receiver_count", &self.tx.receiver_count())
            .finish_non_exhaustive()
    }
}

/// Returned by [`Topic::publish`] when no subscribers are active.
#[derive(Debug, Clone, Copy, thiserror::Error)]
#[error("topic is closed (no active subscribers)")]
pub struct TopicClosed;
