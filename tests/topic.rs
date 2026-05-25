// Copyright 2026 runtime contributors
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use runtime::{Topic, TopicClosed, TopicConfig};
use tokio::sync::broadcast::error::RecvError;

#[tokio::test]
async fn round_trip() {
    let t: Topic<u32> = Topic::with_default_capacity();
    let mut rx = t.subscribe();
    assert_eq!(t.receiver_count(), 1);

    let n = t.publish(42).expect("had a subscriber");
    assert_eq!(n, 1);

    let got: Arc<u32> = rx.recv().await.unwrap();
    assert_eq!(*got, 42);
}

#[tokio::test]
async fn multiple_subscribers_see_event() {
    let t: Topic<&'static str> = Topic::new(TopicConfig { capacity: 8 });
    let mut a = t.subscribe();
    let mut b = t.subscribe();

    t.publish("hello").unwrap();

    let ga = a.recv().await.unwrap();
    let gb = b.recv().await.unwrap();
    assert_eq!(*ga, "hello");
    assert_eq!(*gb, "hello");
    // Arc-wrapping → same underlying allocation
    assert!(Arc::ptr_eq(&ga, &gb));
}

#[tokio::test]
async fn publish_without_subscribers_errors() {
    let t: Topic<i32> = Topic::with_default_capacity();
    assert_eq!(t.receiver_count(), 0);
    let err = t.publish(1).unwrap_err();
    let _: TopicClosed = err;
}

#[tokio::test]
async fn publish_after_subscriber_dropped() {
    let t: Topic<i32> = Topic::with_default_capacity();
    let rx = t.subscribe();
    drop(rx);
    let err = t.publish(1).unwrap_err();
    let _: TopicClosed = err;
}

#[tokio::test]
async fn lag_is_signalled() {
    // Tiny buffer + lots of messages → lag.
    let t: Topic<u32> = Topic::new(TopicConfig { capacity: 2 });
    let mut rx = t.subscribe();
    for i in 0..10 {
        // publish returns Ok(_) even when some subscribers will lag — the
        // lag is reported to the subscriber on recv.
        t.publish(i).unwrap();
    }
    let err = rx.recv().await.unwrap_err();
    assert!(matches!(err, RecvError::Lagged(_)), "got {err:?}");
}

#[tokio::test]
async fn cloning_topic_shares_buffer() {
    let t: Topic<u32> = Topic::with_default_capacity();
    let mut rx = t.subscribe();
    let t2 = t.clone();
    t2.publish(7).unwrap();
    let got = rx.recv().await.unwrap();
    assert_eq!(*got, 7);
}
