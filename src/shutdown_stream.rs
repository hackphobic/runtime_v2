// Copyright 2026 runtime contributors
// SPDX-License-Identifier: Apache-2.0

//! A [`Stream`] adapter that ends when an associated future fires.
//!
//! Use [`ShutdownStream`] to replace this idiom:
//!
//! ```ignore
//! loop {
//!     tokio::select! {
//!         _ = shutdown => break,
//!         item = stream.next() => { /* logic */ }
//!     }
//! }
//! ```
//!
//! with this:
//!
//! ```ignore
//! let mut shutdown_stream = ShutdownStream::new(shutdown, stream);
//! while let Some(item) = shutdown_stream.next().await {
//!     /* logic */
//! }
//! ```
//!
//! The shutdown future can be anything `Future<Output = ()> + Unpin`. For the
//! common case of a [`CancellationToken`], use
//! [`ShutdownStream::from_cancellation_token`].
//!
//! For most service code, prefer using `tokio::select!` with
//! [`ServiceContext::cancelled`](crate::ServiceContext::cancelled) directly —
//! `ShutdownStream` is most useful when adapting an existing stream-consuming
//! function to be cancellable.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use futures_core::{FusedStream, Stream};
use tokio_util::sync::CancellationToken;

/// A [`Stream`] that ends when an associated shutdown future fires.
///
/// Polls the shutdown future before the inner stream on each `poll_next`, so a
/// cancellation cannot be starved by a stream that's always ready.
pub struct ShutdownStream<Sh, S> {
    shutdown: Option<Sh>,
    stream: Option<S>,
}

impl<Sh, S> ShutdownStream<Sh, S> {
    /// Construct from a shutdown future and a stream.
    pub fn new(shutdown: Sh, stream: S) -> Self {
        Self {
            shutdown: Some(shutdown),
            stream: Some(stream),
        }
    }

    /// Construct from already-`Option`-wrapped parts (useful when rejoining a
    /// previously-split [`ShutdownStream`]).
    pub fn from_parts(shutdown: Option<Sh>, stream: Option<S>) -> Self {
        Self { shutdown, stream }
    }

    /// Decompose into `(shutdown, stream)`. Either may be `None` if the
    /// `ShutdownStream` has terminated.
    pub fn into_parts(self) -> (Option<Sh>, Option<S>) {
        (self.shutdown, self.stream)
    }
}

/// Convenience: bind a [`CancellationToken`] as the shutdown future.
///
/// The returned `ShutdownStream` uses a boxed future internally because
/// `CancellationToken::cancelled` borrows the token; we move a clone into an
/// owned async block to avoid the lifetime.
pub type ShutdownFut = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

impl<S> ShutdownStream<ShutdownFut, S> {
    /// Create a `ShutdownStream` that terminates when the given cancellation
    /// token fires.
    pub fn from_cancellation_token(token: CancellationToken, stream: S) -> Self {
        let fut: ShutdownFut = Box::pin(async move { token.cancelled().await });
        Self::new(fut, stream)
    }
}

impl<Sh, S, T> Stream for ShutdownStream<Sh, S>
where
    Sh: Future<Output = ()> + Unpin,
    S: Stream<Item = T> + Unpin,
{
    type Item = T;

    /// Poll the shutdown future first; if it's ready, terminate. Otherwise poll
    /// the inner stream.
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = &mut *self;

        if let Some(sh) = this.shutdown.as_mut() {
            if Pin::new(sh).poll(cx).is_ready() {
                this.shutdown = None;
                this.stream = None;
                return Poll::Ready(None);
            }
        }

        if let Some(st) = this.stream.as_mut() {
            match Pin::new(st).poll_next(cx) {
                Poll::Ready(Some(item)) => Poll::Ready(Some(item)),
                Poll::Ready(None) => {
                    this.shutdown = None;
                    this.stream = None;
                    Poll::Ready(None)
                }
                Poll::Pending => Poll::Pending,
            }
        } else {
            Poll::Ready(None)
        }
    }
}

impl<Sh, S, T> FusedStream for ShutdownStream<Sh, S>
where
    Sh: Future<Output = ()> + Unpin,
    S: Stream<Item = T> + Unpin,
{
    fn is_terminated(&self) -> bool {
        self.stream.is_none()
    }
}
