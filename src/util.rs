// Copyright 2026 runtime contributors
// SPDX-License-Identifier: Apache-2.0

use std::time::Duration;

/// Default drain timeout used by the supervisor when none is set on the builder.
pub(crate) const DEFAULT_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);

/// Dedup a slice in-order, preserving the first occurrence of each item.
///
/// Used by the runtime builder so that accidentally listing a dependency twice
/// (`&["storage", "storage"]`) doesn't corrupt the topological sort's indegrees.
pub(crate) fn dedup_preserve_order<T>(items: &[T]) -> Vec<T>
where
    T: Clone + Eq + std::hash::Hash,
{
    use std::collections::HashSet;
    let mut seen: HashSet<T> = HashSet::with_capacity(items.len());
    items
        .iter()
        .filter(|&x| seen.insert(x.clone()))
        .cloned()
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dedup_preserves_order() {
        let v = vec!["a", "b", "a", "c", "b", "d"];
        assert_eq!(dedup_preserve_order(&v), vec!["a", "b", "c", "d"]);
    }

    #[test]
    fn dedup_empty() {
        let v: Vec<&str> = vec![];
        assert!(dedup_preserve_order(&v).is_empty());
    }

    #[test]
    fn dedup_no_duplicates() {
        let v = vec!["a", "b", "c"];
        assert_eq!(dedup_preserve_order(&v), v);
    }
}
