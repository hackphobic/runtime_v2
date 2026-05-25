// Copyright 2026 runtime contributors
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use runtime::State;

#[tokio::test]
async fn new_holds_initial_value() {
    let s = State::new(42u32);
    assert_eq!(*s.borrow(), 42);
    assert_eq!(s.snapshot(), 42);
}

#[tokio::test]
async fn set_replaces_value() {
    let s = State::new(0u32);
    s.set(99);
    assert_eq!(s.snapshot(), 99);
}

#[tokio::test]
async fn modify_updates_in_place() {
    let s = State::new(vec![1, 2, 3]);
    s.modify(|v| v.push(4));
    assert_eq!(s.snapshot(), vec![1, 2, 3, 4]);
}

#[tokio::test]
async fn modify_if_skips_notification_when_false() {
    let s = State::new(0u32);
    let rx = s.subscribe();
    let changed = s.modify_if(|v| {
        *v = 1;
        false // explicitly say "no change"
    });
    assert!(!changed);
    // Value was still mutated even though receivers weren't notified
    // (this matches tokio::sync::watch::send_if_modified semantics).
    assert_eq!(s.snapshot(), 1);
    // No notification fired, so rx.has_changed() should be false.
    assert!(!rx.has_changed().unwrap());
}

#[tokio::test]
async fn subscribe_then_set_wakes_receiver() {
    let s = State::new(0u32);
    let mut rx = s.subscribe();
    assert_eq!(*rx.borrow_and_update(), 0);

    s.set(7);
    rx.changed().await.unwrap();
    assert_eq!(*rx.borrow_and_update(), 7);
}

#[tokio::test]
async fn multiple_subscribers_all_see_updates() {
    let s = State::new(0u32);
    let mut a = s.subscribe();
    let mut b = s.subscribe();
    s.set(11);

    a.changed().await.unwrap();
    b.changed().await.unwrap();
    assert_eq!(*a.borrow_and_update(), 11);
    assert_eq!(*b.borrow_and_update(), 11);
}

#[tokio::test]
async fn clone_shares_channel() {
    let s1 = State::new(0u32);
    let s2 = s1.clone();
    let mut rx = s1.subscribe();

    s2.set(5);
    rx.changed().await.unwrap();
    assert_eq!(*rx.borrow_and_update(), 5);
    assert_eq!(s1.snapshot(), 5);
}

#[tokio::test]
async fn arc_inner_is_cheap_to_snapshot() {
    // The recommended pattern for large state: State<Arc<T>>.
    // snapshot() is a refcount clone, not a deep clone.
    let s = State::new(Arc::new(vec![0u8; 1024 * 1024])); // 1 MB
    let snap_a = s.snapshot();
    let snap_b = s.snapshot();
    assert!(Arc::ptr_eq(&snap_a, &snap_b));
    assert_eq!(snap_a.len(), 1024 * 1024);
}

#[tokio::test]
async fn arc_make_mut_pattern() {
    let s: State<Arc<Vec<u32>>> = State::new(Arc::new(Vec::new()));
    s.modify(|v| Arc::make_mut(v).push(1));
    s.modify(|v| Arc::make_mut(v).push(2));
    assert_eq!(*s.snapshot(), vec![1, 2]);
}

#[tokio::test]
async fn receiver_count_tracks_subscribers() {
    let s = State::new(0u32);
    assert_eq!(s.receiver_count(), 0);
    let _a = s.subscribe();
    assert_eq!(s.receiver_count(), 1);
    let b = s.subscribe();
    assert_eq!(s.receiver_count(), 2);
    drop(b);
    assert_eq!(s.receiver_count(), 1);
}
