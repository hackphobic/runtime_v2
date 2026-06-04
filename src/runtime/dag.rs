// Copyright 2026 runtime contributors
// SPDX-License-Identifier: Apache-2.0

use std::collections::{HashMap, HashSet, VecDeque};

use crate::service::{ServiceName, ServiceSpec};

/// Errors from validating and ordering the service dependency graph.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum DagError {
    /// A service declared a dependency on a name not registered with the builder.
    #[error("service `{service}` depends on unknown service `{dep}`")]
    UnknownDependency {
        /// The service with the bad dependency.
        service: ServiceName,
        /// The dependency name that was not found.
        dep: ServiceName,
    },

    /// A cycle was detected in the dependency graph. The included names are the
    /// services still on the cycle after Kahn's algorithm has drained the rest.
    #[error("cycle detected in dependency graph; services involved: {0:?}")]
    Cycle(Vec<ServiceName>),

    /// A service declared itself as a dependency.
    #[error("service `{0}` cannot depend on itself")]
    SelfDependency(ServiceName),
}

/// Topologically sort services. Returns indices into `specs` in start order.
///
/// Uses Kahn's algorithm; duplicate dep names within a single service's dep list
/// should already be deduped by the builder (see [`util::dedup_preserve_order`]),
/// but the algorithm tolerates duplicates by counting indegree per occurrence.
///
/// [`util::dedup_preserve_order`]: crate::util::dedup_preserve_order
pub(crate) fn topo_sort(specs: &[ServiceSpec]) -> Result<Vec<usize>, DagError> {
    let n = specs.len();

    // Build name → index.
    let mut name_to_idx: HashMap<ServiceName, usize> = HashMap::with_capacity(n);
    for (i, s) in specs.iter().enumerate() {
        name_to_idx.insert(s.name, i);
    }

    // Validate deps: no self-loops, no unknown names.
    for s in specs {
        for &d in &s.deps {
            if d == s.name {
                return Err(DagError::SelfDependency(s.name));
            }
            if !name_to_idx.contains_key(&d) {
                return Err(DagError::UnknownDependency {
                    service: s.name,
                    dep: d,
                });
            }
        }
    }

    // Build indegree and adjacency.
    let mut indegree = vec![0usize; n];
    let mut outgoing: Vec<Vec<usize>> = vec![Vec::new(); n];
    for (i, s) in specs.iter().enumerate() {
        for &dep_name in &s.deps {
            let dep_idx = name_to_idx[&dep_name];
            indegree[i] += 1;
            outgoing[dep_idx].push(i);
        }
    }

    // Kahn's algorithm.
    let mut queue: VecDeque<usize> = indegree
        .iter()
        .enumerate()
        .filter_map(|(i, &d)| (d == 0).then_some(i))
        .collect();

    let mut out = Vec::with_capacity(n);
    while let Some(i) = queue.pop_front() {
        out.push(i);
        for &next in &outgoing[i] {
            indegree[next] -= 1;
            if indegree[next] == 0 {
                queue.push_back(next);
            }
        }
    }

    if out.len() != n {
        let mut remaining: Vec<ServiceName> = indegree
            .iter()
            .enumerate()
            .filter(|&(_, &d)| d > 0)
            .map(|(i, _)| specs[i].name)
            .collect();
        let mut seen = HashSet::new();
        remaining.retain(|x| seen.insert(*x));
        return Err(DagError::Cycle(remaining));
    }

    Ok(out)
}
