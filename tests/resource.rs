// Copyright 2026 runtime contributors
// SPDX-License-Identifier: Apache-2.0

use runtime::ResourceHandle;

#[test]
fn deref_yields_inner() {
    let h = ResourceHandle::new(42u32);
    assert_eq!(*h, 42);
}

#[test]
fn try_unwrap_succeeds_when_unique() {
    let h = ResourceHandle::new(String::from("alone"));
    let got = h.try_unwrap().expect("only handle");
    assert_eq!(got, "alone");
}

#[test]
fn try_unwrap_fails_when_cloned() {
    let h = ResourceHandle::new(7u32);
    let clone = h.clone();
    assert!(h.try_unwrap().is_none(), "clone still alive");
    drop(clone);
}

#[test]
fn try_unwrap_succeeds_after_clones_dropped() {
    let h = ResourceHandle::new(7u32);
    let c1 = h.clone();
    let c2 = h.clone();
    drop(c1);
    drop(c2);
    assert_eq!(h.try_unwrap(), Some(7));
}

#[test]
fn weak_handle_upgrade_works_while_owned() {
    let h = ResourceHandle::new(123_u64);
    let weak = h.clone().into_weak();
    let upgraded = weak.upgrade().expect("original still alive");
    assert_eq!(*upgraded, 123);
}

#[test]
fn weak_handle_upgrade_fails_after_drop() {
    let h = ResourceHandle::new(0u8);
    let weak = h.clone().into_weak();
    drop(h);
    assert!(weak.upgrade().is_none());
}

#[test]
fn shared_reads_observe_same_value() {
    let h1 = ResourceHandle::new(vec![1, 2, 3]);
    let h2 = h1.clone();
    assert_eq!(&*h1, &*h2);
}
