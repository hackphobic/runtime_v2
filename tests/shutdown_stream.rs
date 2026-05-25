// Copyright 2020-2021 IOTA Stiftung
// Copyright 2026 runtime contributors
// SPDX-License-Identifier: Apache-2.0

use std::time::Duration;

use runtime::ShutdownStream;
use futures_core::Stream;
use tokio::{sync::mpsc, task::spawn, time::sleep};
use tokio_stream::{StreamExt, wrappers::UnboundedReceiverStream};
use tokio_util::sync::CancellationToken;

fn unbounded_stream<T: Send + 'static>() -> (mpsc::UnboundedSender<T>, impl Stream<Item = T> + Unpin) {
    let (tx, rx) = mpsc::unbounded_channel::<T>();
    (tx, UnboundedReceiverStream::new(rx))
}

#[tokio::test]
async fn stream_runs_to_completion_when_shutdown_pending() {
    let (sender, stream) = unbounded_stream::<usize>();
    let (_shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    // Map the oneshot result to () so it matches the `Future<Output = ()>` bound.
    let shutdown = async move {
        let _ = shutdown_rx.await;
    };
    let shutdown = Box::pin(shutdown);

    let handle = spawn(async move {
        let mut shutdown_stream = ShutdownStream::new(shutdown, stream);
        let mut acc = 0usize;
        while let Some(item) = shutdown_stream.next().await {
            acc += item;
            sleep(Duration::from_millis(1)).await;
        }
        acc
    });

    for i in 0..=100usize {
        sender.send(i).unwrap();
    }
    drop(sender); // closes the stream → ShutdownStream returns None

    assert_eq!(handle.await.unwrap(), 5050);
}

#[tokio::test]
async fn early_shutdown_terminates_stream() {
    let (sender, stream) = unbounded_stream::<usize>();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let shutdown = Box::pin(async move {
        let _ = shutdown_rx.await;
    });

    let handle = spawn(async move {
        let mut shutdown_stream = ShutdownStream::new(shutdown, stream);
        let mut acc = 0usize;
        while let Some(item) = shutdown_stream.next().await {
            acc += item;
            sleep(Duration::from_millis(2)).await;
        }
        acc
    });

    for i in 0..=100usize {
        sender.send(i).unwrap();
    }
    // Trigger shutdown before all items can be drained at 2ms/item.
    shutdown_tx.send(()).unwrap();

    let acc = handle.await.unwrap();
    assert!(acc < 5050, "got {acc}, expected partial sum");
}

#[tokio::test]
async fn from_cancellation_token_terminates_stream() {
    let (sender, stream) = unbounded_stream::<usize>();
    let token = CancellationToken::new();

    let handle = {
        let token = token.clone();
        spawn(async move {
            let mut shutdown_stream = ShutdownStream::from_cancellation_token(token, stream);
            let mut acc = 0usize;
            while let Some(item) = shutdown_stream.next().await {
                acc += item;
                sleep(Duration::from_millis(2)).await;
            }
            acc
        })
    };

    for i in 0..=100usize {
        sender.send(i).unwrap();
    }
    token.cancel();

    let acc = handle.await.unwrap();
    assert!(acc < 5050, "got {acc}");
}

#[tokio::test]
async fn into_parts_round_trip() {
    let (sender, stream) = unbounded_stream::<u32>();
    let token = CancellationToken::new();
    let ss = ShutdownStream::from_cancellation_token(token.clone(), stream);

    let (shutdown, stream) = ss.into_parts();
    assert!(shutdown.is_some() && stream.is_some());

    let mut ss = ShutdownStream::from_parts(shutdown, stream);
    sender.send(7).unwrap();
    let got = ss.next().await.unwrap();
    assert_eq!(got, 7);
}

#[tokio::test]
async fn fused_after_termination() {
    use futures_core::FusedStream;

    let (sender, stream) = unbounded_stream::<u32>();
    let token = CancellationToken::new();
    let mut ss = ShutdownStream::from_cancellation_token(token.clone(), stream);

    assert!(!ss.is_terminated());

    drop(sender); // close the inner stream
    assert!(ss.next().await.is_none());
    assert!(ss.is_terminated());
    // Polling again returns None and stays terminated.
    assert!(ss.next().await.is_none());
}
