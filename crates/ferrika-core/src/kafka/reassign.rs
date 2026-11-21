use std::fs;
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::error::{KafkaError, KafkaResult};

/// An individual partition replica placement specification
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionAssignment {
    pub topic: String,
    pub partition: i32,
    pub replicas: Vec<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub log_dirs: Option<Vec<String>>,
}

impl PartitionAssignment {
    pub fn new(topic: impl Into<String>, partition: i32, replicas: Vec<i32>) -> Self {
        Self {
            topic: topic.into(),
            partition,
            replicas,
            log_dirs: None,
        }
    }
}

/// Official Apache Kafka partition reassignment plan format (version 1)
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReassignmentPlan {
    pub version: i32,
    pub partitions: Vec<PartitionAssignment>,
}

impl Default for ReassignmentPlan {
    fn default() -> Self {
        Self {
            version: 1,
            partitions: Vec::new(),
        }
    }
}

impl ReassignmentPlan {
    pub fn new(partitions: Vec<PartitionAssignment>) -> Self {
        Self {
            version: 1,
            partitions,
        }
    }

    /// Rejects plans the cluster could not carry out or that make no sense:
    /// no partitions, a partition listed twice, or a replica list that is
    /// empty, repeats a broker, or holds a negative id.
    pub fn validate(&self) -> KafkaResult<()> {
        if self.partitions.is_empty() {
            return Err(KafkaError::config("Reassignment plan has no partitions"));
        }
        let mut seen = std::collections::HashSet::new();
        for p in &self.partitions {
            let name = format!("{}-{}", p.topic, p.partition);
            if p.partition < 0 {
                return Err(KafkaError::config(format!(
                    "Plan has a negative partition number for {name}"
                )));
            }
            if !seen.insert((p.topic.as_str(), p.partition)) {
                return Err(KafkaError::config(format!(
                    "Plan lists partition {name} more than once"
                )));
            }
            if p.replicas.is_empty() {
                return Err(KafkaError::config(format!(
                    "Plan gives {name} an empty replica list"
                )));
            }
            if p.replicas.iter().any(|r| *r < 0) {
                return Err(KafkaError::config(format!(
                    "Plan gives {name} a negative broker id: {:?}",
                    p.replicas
                )));
            }
            let mut ids = std::collections::HashSet::new();
            if !p.replicas.iter().all(|r| ids.insert(*r)) {
                return Err(KafkaError::config(format!(
                    "Plan gives {name} a duplicate replica: {:?}",
                    p.replicas
                )));
            }
        }
        Ok(())
    }

    /// Serializes plan to formatted JSON string matching Kafka specifications
    pub fn to_json_pretty(&self) -> KafkaResult<String> {
        serde_json::to_string_pretty(self).map_err(|e| {
            KafkaError::Serialization(format!("Failed to serialize reassignment plan: {e}"))
        })
    }

    /// Parses plan from JSON string
    pub fn from_json(json: &str) -> KafkaResult<Self> {
        serde_json::from_str(json).map_err(|e| {
            KafkaError::Serialization(format!("Failed to parse reassignment plan JSON: {e}"))
        })
    }

    /// Reads plan from a file on disk
    pub fn load_from_file(path: &Path) -> KafkaResult<Self> {
        let content = fs::read_to_string(path).map_err(|e| {
            KafkaError::Io(format!(
                "Failed to read reassignment plan from {}: {e}",
                path.display()
            ))
        })?;
        Self::from_json(&content)
    }

    /// Writes plan to a file on disk
    pub fn save_to_file(&self, path: &Path) -> KafkaResult<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|e| {
                KafkaError::Io(format!(
                    "Failed to create directory {}: {e}",
                    parent.display()
                ))
            })?;
        }
        let json = self.to_json_pretty()?;
        fs::write(path, json).map_err(|e| {
            KafkaError::Io(format!(
                "Failed to write reassignment plan to {}: {e}",
                path.display()
            ))
        })?;
        Ok(())
    }
}

/// Delta comparison between current replica layout and planned target replicas
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionMoveDelta {
    pub topic: String,
    pub partition: i32,
    pub current_replicas: Vec<i32>,
    pub target_replicas: Vec<i32>,
    pub added_replicas: Vec<i32>,
    pub removed_replicas: Vec<i32>,
}

impl PartitionMoveDelta {
    pub fn calculate(topic: &str, partition: i32, current: &[i32], target: &[i32]) -> Self {
        let added_replicas: Vec<i32> = target
            .iter()
            .copied()
            .filter(|r| !current.contains(r))
            .collect();
        let removed_replicas: Vec<i32> = current
            .iter()
            .copied()
            .filter(|r| !target.contains(r))
            .collect();

        Self {
            topic: topic.to_string(),
            partition,
            current_replicas: current.to_vec(),
            target_replicas: target.to_vec(),
            added_replicas,
            removed_replicas,
        }
    }

    pub fn has_movement(&self) -> bool {
        !self.added_replicas.is_empty() || !self.removed_replicas.is_empty()
    }
}

/// Most partitions per topic `generate_balanced_plan` will lay out; a typo such
/// as `-p 100000000` would otherwise try to allocate gigabytes.
pub const MAX_PLAN_PARTITIONS: usize = 100_000;

/// `items` without repeats, keeping the first occurrence and the order.
pub fn dedup_preserving_order<T: Clone + Eq + std::hash::Hash>(items: &[T]) -> Vec<T> {
    let mut seen = std::collections::HashSet::new();
    items
        .iter()
        .filter(|i| seen.insert((*i).clone()))
        .cloned()
        .collect()
}

/// Generates a round-robin balanced partition replica plan across the target brokers.
///
/// Partition `p` gets its preferred leader on `target_brokers[(start_offset + p) % n]`
/// and its followers on the next brokers. When planning several topics, pass
/// the number of partitions already placed as `start_offset` so each topic
/// continues the rotation; starting every topic at the first broker piles the
/// leaders of many small topics onto it.
pub fn generate_balanced_plan(
    topic: &str,
    partition_count: usize,
    target_brokers: &[i32],
    replication_factor: usize,
    start_offset: usize,
) -> KafkaResult<ReassignmentPlan> {
    if target_brokers.is_empty() {
        return Err(KafkaError::admin("Target broker list cannot be empty"));
    }
    // A repeated broker id would put two replicas of one partition on a broker.
    let target_brokers = &dedup_preserving_order(target_brokers);
    if partition_count > MAX_PLAN_PARTITIONS {
        return Err(KafkaError::admin(format!(
            "Partition count {partition_count} exceeds the planning limit of {MAX_PLAN_PARTITIONS}"
        )));
    }
    if replication_factor == 0 {
        return Err(KafkaError::admin(
            "Replication factor must be greater than 0",
        ));
    }
    if replication_factor > target_brokers.len() {
        return Err(KafkaError::admin(format!(
            "Replication factor ({replication_factor}) cannot exceed the number of distinct target brokers ({})",
            target_brokers.len()
        )));
    }

    let num_brokers = target_brokers.len();
    let mut partitions = Vec::with_capacity(partition_count);

    for p in 0..partition_count {
        let mut replicas = Vec::with_capacity(replication_factor);
        let leader_idx = (start_offset + p) % num_brokers;
        replicas.push(target_brokers[leader_idx]);

        for r in 1..replication_factor {
            let follower_idx = (leader_idx + r) % num_brokers;
            replicas.push(target_brokers[follower_idx]);
        }

        partitions.push(PartitionAssignment {
            topic: topic.to_string(),
            partition: p as i32,
            replicas,
            log_dirs: None,
        });
    }

    Ok(ReassignmentPlan::new(partitions))
}

/// Plans `current` partitions onto `target_brokers` moving as few replicas
/// as possible, unlike [`generate_balanced_plan`], which lays every
/// partition out afresh.
///
/// A replica already on a target broker stays unless that broker holds more
/// than its share (`floor` or `ceil` of replicas / brokers); new replicas go
/// to the least-loaded brokers. The current preferred leader stays first
/// unless it leads more than its share. `replication_factor` `None` keeps
/// each partition's current replica count.
pub fn generate_minimal_move_plan(
    current: &[PartitionAssignment],
    target_brokers: &[i32],
    replication_factor: Option<usize>,
) -> KafkaResult<ReassignmentPlan> {
    if target_brokers.is_empty() {
        return Err(KafkaError::admin("Target broker list cannot be empty"));
    }
    let brokers = dedup_preserving_order(target_brokers);
    if current.len() > MAX_PLAN_PARTITIONS {
        return Err(KafkaError::admin(format!(
            "Partition count {} exceeds the planning limit of {MAX_PLAN_PARTITIONS}",
            current.len()
        )));
    }
    let wanted: Vec<usize> = current
        .iter()
        .map(|p| replication_factor.unwrap_or(dedup_preserving_order(&p.replicas).len()))
        .collect();
    if let Some((i, &rf)) = wanted
        .iter()
        .enumerate()
        .find(|(_, &rf)| rf == 0 || rf > brokers.len())
    {
        return Err(KafkaError::admin(
            if rf == 0 && replication_factor.is_none() {
                format!(
                    "Partition {}-{} has no replicas; pass the replication factor",
                    current[i].topic, current[i].partition
                )
            } else if rf == 0 {
                "Replication factor must be greater than 0".to_string()
            } else {
                format!(
                "Replication factor ({rf}) cannot exceed the number of distinct target brokers ({})",
                brokers.len()
            )
            },
        ));
    }

    let mut assigned: Vec<Vec<i32>> = current
        .iter()
        .zip(&wanted)
        .map(|(p, &rf)| {
            let mut kept = dedup_preserving_order(&p.replicas);
            kept.retain(|b| brokers.contains(b));
            kept.truncate(rf);
            kept
        })
        .collect();

    // Each broker's share: `base`, and `base + 1` for `extra` of them.
    let total: usize = wanted.iter().sum();
    let (base, extra) = (total / brokers.len(), total % brokers.len());
    let load = |assigned: &[Vec<i32>], b: i32| assigned.iter().filter(|r| r.contains(&b)).count();
    // Brokers that already hold the most get the `base + 1` shares, so the
    // fewest replicas have to leave them.
    let mut by_load: Vec<i32> = brokers.clone();
    by_load.sort_by_key(|&b| std::cmp::Reverse(load(&assigned, b)));
    let share = |b: i32| base + usize::from(by_load.iter().take(extra).any(|&x| x == b));
    for &b in &brokers {
        let mut over = load(&assigned, b).saturating_sub(share(b));
        // Drop followers before leaders, from the last partitions first.
        for followers_only in [true, false] {
            for replicas in assigned.iter_mut().rev() {
                if over == 0 {
                    break;
                }
                if let Some(i) = replicas.iter().position(|&x| x == b) {
                    if !followers_only || i > 0 {
                        replicas.remove(i);
                        over -= 1;
                    }
                }
            }
        }
    }

    // Fill up from the least-loaded brokers; ties go round the broker list
    // from a different start per partition so they spread.
    let mut loads: std::collections::HashMap<i32, usize> =
        brokers.iter().map(|&b| (b, load(&assigned, b))).collect();
    for (i, (replicas, &rf)) in assigned.iter_mut().zip(&wanted).enumerate() {
        while replicas.len() < rf {
            let pick = (0..brokers.len())
                .map(|k| brokers[(i + k) % brokers.len()])
                .filter(|b| !replicas.contains(b))
                .min_by_key(|b| loads[b])
                .ok_or_else(|| {
                    KafkaError::admin("Not enough brokers for the replication factor")
                })?;
            replicas.push(pick);
            *loads.entry(pick).or_default() += 1;
        }
    }

    // A partition that needs several replicas can run out of brokers below
    // their share (the others are in it already) and take one at its share.
    // Swap replicas from brokers over their share to brokers under it,
    // preferring a replica the plan just added (no extra copy) and a follower.
    loop {
        let over = |b: &&i32| loads[*b] > share(**b);
        let under = |b: &&i32| loads[*b] < share(**b);
        let mut swap = None;
        'search: for &from in brokers.iter().filter(over) {
            for &to in brokers.iter().filter(under) {
                let best = assigned
                    .iter()
                    .enumerate()
                    .filter(|(_, r)| r.contains(&from) && !r.contains(&to))
                    .min_by_key(|(i, r)| {
                        (
                            current[*i].replicas.contains(&from),
                            r.first() == Some(&from),
                        )
                    })
                    .map(|(i, _)| i);
                if let Some(i) = best {
                    swap = Some((i, from, to));
                    break 'search;
                }
            }
        }
        let Some((i, from, to)) = swap else {
            break;
        };
        if let Some(slot) = assigned[i].iter_mut().find(|b| **b == from) {
            *slot = to;
            *loads.entry(from).or_default() -= 1;
            *loads.entry(to).or_default() += 1;
        }
    }

    // Preferred leaders: keep the current one while it is within its share.
    let leader_share = current.len().div_ceil(brokers.len());
    let mut leads: std::collections::HashMap<i32, usize> = std::collections::HashMap::new();
    let mut leader: Vec<Option<i32>> = vec![None; current.len()];
    for (i, p) in current.iter().enumerate() {
        if let Some(&b) = p.replicas.first() {
            if assigned[i].contains(&b) && leads.get(&b).copied().unwrap_or(0) < leader_share {
                leader[i] = Some(b);
                *leads.entry(b).or_default() += 1;
            }
        }
    }
    for (i, replicas) in assigned.iter_mut().enumerate() {
        let lead = match leader[i] {
            Some(b) => b,
            None => {
                let b = replicas
                    .iter()
                    .copied()
                    .min_by_key(|b| leads.get(b).copied().unwrap_or(0))
                    .ok_or_else(|| KafkaError::admin("Partition without replicas"))?;
                *leads.entry(b).or_default() += 1;
                b
            }
        };
        if let Some(pos) = replicas.iter().position(|&b| b == lead) {
            let b = replicas.remove(pos);
            replicas.insert(0, b);
        }
    }

    Ok(ReassignmentPlan::new(
        current
            .iter()
            .zip(assigned)
            .map(|(p, replicas)| PartitionAssignment {
                topic: p.topic.clone(),
                partition: p.partition,
                replicas,
                log_dirs: None,
            })
            .collect(),
    ))
}

/// Replicas a move from `current` to `planned` copies onto new brokers.
pub fn replicas_to_move(current: &[PartitionAssignment], planned: &ReassignmentPlan) -> usize {
    let before: std::collections::HashMap<(&str, i32), &[i32]> = current
        .iter()
        .map(|c| ((c.topic.as_str(), c.partition), c.replicas.as_slice()))
        .collect();
    planned
        .partitions
        .iter()
        .map(|p| {
            let before = before
                .get(&(p.topic.as_str(), p.partition))
                .copied()
                .unwrap_or_default();
            p.replicas.iter().filter(|b| !before.contains(b)).count()
        })
        .sum()
}

/// Generates a rollback plan restoring current assignments
pub fn generate_rollback_plan(current_assignments: &[PartitionAssignment]) -> ReassignmentPlan {
    ReassignmentPlan::new(current_assignments.to_vec())
}

/// The replicas a partition had before an ongoing move: its current replica
/// set minus the replicas being added (metadata lists old and new together).
/// Without an ongoing move the replicas are returned unchanged.
pub fn pre_move_replicas(
    replicas: &[i32],
    ongoing: Option<&crate::model::cluster::OngoingReassignment>,
) -> Vec<i32> {
    match ongoing {
        Some(m) => replicas
            .iter()
            .copied()
            .filter(|r| !m.adding.contains(r))
            .collect(),
        None => replicas.to_vec(),
    }
}

/// Status of partition reassignment execution
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReassignmentStatus {
    Completed,
    InProgress,
    Failed,
}

impl std::fmt::Display for ReassignmentStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Completed => write!(f, "COMPLETED"),
            Self::InProgress => write!(f, "IN_PROGRESS"),
            Self::Failed => write!(f, "FAILED"),
        }
    }
}

/// Evaluates migration status of a partition given its live replicas & ISR vs target plan
pub fn evaluate_partition_status(
    live_replicas: &[i32],
    live_isr: &[i32],
    target_replicas: &[i32],
) -> ReassignmentStatus {
    if live_replicas == target_replicas && target_replicas.iter().all(|r| live_isr.contains(r)) {
        ReassignmentStatus::Completed
    } else if target_replicas.iter().any(|r| live_replicas.contains(r)) {
        ReassignmentStatus::InProgress
    } else {
        ReassignmentStatus::Failed
    }
}

/// New replica lists for `partitions` (`(partition, current replicas)`) of
/// `topic` with `target_rf` replicas each, moving as little data as possible:
/// growing keeps every current replica and adds the least-loaded brokers;
/// shrinking keeps the first (preferred leader) replica and drops the
/// followers on the most-loaded brokers. `load` is the replica count per
/// broker across the cluster and is updated as replicas are placed.
/// Partitions already at `target_rf` are left out.
pub fn change_replication(
    topic: &str,
    partitions: &[(i32, Vec<i32>)],
    brokers: &[i32],
    load: &mut std::collections::HashMap<i32, usize>,
    target_rf: usize,
) -> KafkaResult<Vec<PartitionAssignment>> {
    if target_rf == 0 || target_rf > brokers.len() {
        return Err(KafkaError::config(format!(
            "Replication factor must be between 1 and {} (online brokers)",
            brokers.len()
        )));
    }
    let mut plan = Vec::new();
    for (partition, current) in partitions {
        let mut replicas = current.clone();
        if replicas.len() == target_rf {
            continue;
        }
        while replicas.len() < target_rf {
            let next = brokers
                .iter()
                .filter(|b| !replicas.contains(b))
                .min_by_key(|b| (load.get(b).copied().unwrap_or(0), **b))
                .copied()
                .ok_or_else(|| {
                    KafkaError::config("Not enough brokers for that replication factor")
                })?;
            *load.entry(next).or_default() += 1;
            replicas.push(next);
        }
        while replicas.len() > target_rf {
            // Never the first replica: it is the preferred leader.
            let (ix, _) = replicas
                .iter()
                .enumerate()
                .skip(1)
                .max_by_key(|(_, b)| (load.get(b).copied().unwrap_or(0), **b))
                .ok_or_else(|| KafkaError::config("Cannot remove the preferred leader"))?;
            let removed = replicas.remove(ix);
            if let Some(n) = load.get_mut(&removed) {
                *n = n.saturating_sub(1);
            }
        }
        plan.push(PartitionAssignment::new(topic, *partition, replicas));
    }
    Ok(plan)
}

/// Plans a replication change for `topic` from the live cluster.
pub fn plan_replication_change(
    admin: &crate::kafka::admin::KafkaAdminClient,
    topic: &str,
    replication: usize,
) -> KafkaResult<Vec<PartitionAssignment>> {
    // The profile's request timeout, like every other admin read.
    let timeout = None;
    let brokers: Vec<i32> = admin
        .list_brokers(timeout)?
        .iter()
        .filter(|b| b.is_online)
        .map(|b| b.id)
        .collect();
    let mut load: std::collections::HashMap<i32, usize> = std::collections::HashMap::new();
    for t in admin.list_topic_summaries(timeout)? {
        for p in &t.partitions {
            for r in &p.replicas {
                *load.entry(*r).or_default() += 1;
            }
        }
    }
    let detail = admin.describe_topic(topic, timeout)?;
    let current: Vec<(i32, Vec<i32>)> = detail
        .partitions
        .iter()
        .map(|p| (p.partition_id, p.replicas.clone()))
        .collect();
    change_replication(topic, &current, &brokers, &mut load, replication)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn layout(rows: &[&[i32]]) -> Vec<PartitionAssignment> {
        rows.iter()
            .enumerate()
            .map(|(i, r)| PartitionAssignment::new("t", i as i32, r.to_vec()))
            .collect()
    }

    fn replica_counts(plan: &ReassignmentPlan, brokers: &[i32]) -> Vec<usize> {
        brokers
            .iter()
            .map(|b| {
                plan.partitions
                    .iter()
                    .filter(|p| p.replicas.contains(b))
                    .count()
            })
            .collect()
    }

    fn leader_counts(plan: &ReassignmentPlan, brokers: &[i32]) -> Vec<usize> {
        brokers
            .iter()
            .map(|b| {
                plan.partitions
                    .iter()
                    .filter(|p| p.replicas[0] == *b)
                    .count()
            })
            .collect()
    }

    #[test]
    fn minimal_plan_leaves_a_balanced_layout_alone() {
        let now = layout(&[&[1, 2], &[2, 3], &[3, 1]]);
        let plan = generate_minimal_move_plan(&now, &[1, 2, 3], None).unwrap();
        assert_eq!(replicas_to_move(&now, &plan), 0);
        let same: Vec<&Vec<i32>> = plan.partitions.iter().map(|p| &p.replicas).collect();
        assert_eq!(same, now.iter().map(|p| &p.replicas).collect::<Vec<_>>());
    }

    #[test]
    fn minimal_plan_moves_only_a_share_onto_a_new_broker() {
        // 6 partitions x rf 2 on brokers 1-3 (4 replicas each); broker 4 joins.
        let now = layout(&[&[1, 2], &[2, 3], &[3, 1], &[1, 2], &[2, 3], &[3, 1]]);
        let plan = generate_minimal_move_plan(&now, &[1, 2, 3, 4], None).unwrap();
        plan.validate().unwrap();
        assert_eq!(replicas_to_move(&now, &plan), 3, "{plan:?}");
        assert_eq!(replica_counts(&plan, &[1, 2, 3, 4]), vec![3, 3, 3, 3]);
        let leaders = leader_counts(&plan, &[1, 2, 3, 4]);
        assert!(leaders.iter().all(|&n| n <= 2), "{leaders:?}");
        // The rotation planner would move far more.
        let fresh = generate_balanced_plan("t", 6, &[1, 2, 3, 4], 2, 0).unwrap();
        assert!(replicas_to_move(&now, &fresh) > 3);
    }

    #[test]
    fn minimal_plan_moves_only_replicas_off_a_removed_broker() {
        let now = layout(&[&[1, 2], &[2, 3], &[3, 4], &[4, 1]]);
        let plan = generate_minimal_move_plan(&now, &[1, 2, 3], None).unwrap();
        // Broker 4 held 2 replicas; nothing else moves.
        assert_eq!(replicas_to_move(&now, &plan), 2);
        assert!(plan.partitions.iter().all(|p| !p.replicas.contains(&4)));
        let counts = replica_counts(&plan, &[1, 2, 3]);
        assert!(counts.iter().all(|&n| n == 2 || n == 3), "{counts:?}");
        // Partitions that did not touch broker 4 keep their leader.
        assert_eq!(plan.partitions[0].replicas, vec![1, 2]);
        assert_eq!(plan.partitions[1].replicas, vec![2, 3]);
    }

    #[test]
    fn minimal_plan_raises_the_replication_factor_in_place() {
        let now = layout(&[&[1], &[2], &[3]]);
        let plan = generate_minimal_move_plan(&now, &[1, 2, 3], Some(2)).unwrap();
        assert_eq!(replicas_to_move(&now, &plan), 3);
        for (before, after) in now.iter().zip(&plan.partitions) {
            assert_eq!(after.replicas.len(), 2);
            assert_eq!(after.replicas[0], before.replicas[0], "leader kept");
        }
        assert_eq!(replica_counts(&plan, &[1, 2, 3]), vec![2, 2, 2]);
    }

    #[test]
    fn minimal_plan_spreads_a_skewed_layout_and_its_leaders() {
        // Everything on brokers 1 and 2, all led by 1.
        let now = layout(&[&[1, 2], &[1, 2], &[1, 2], &[1, 2]]);
        let plan = generate_minimal_move_plan(&now, &[1, 2, 3, 4], None).unwrap();
        plan.validate().unwrap();
        assert_eq!(replica_counts(&plan, &[1, 2, 3, 4]), vec![2, 2, 2, 2]);
        // Greedy leader choice: within one of even.
        assert!(leader_counts(&plan, &[1, 2, 3, 4]).iter().all(|&n| n <= 2));
        assert_eq!(replicas_to_move(&now, &plan), 4);
    }

    #[test]
    fn minimal_plan_stays_balanced_when_a_partition_needs_several_replicas() {
        // Brokers 4 and 5 are replaced by 3: p0 needs two new replicas.
        let now = layout(&[&[4, 5], &[1, 2], &[1, 2]]);
        let plan = generate_minimal_move_plan(&now, &[1, 2, 3], None).unwrap();
        plan.validate().unwrap();
        assert_eq!(replica_counts(&plan, &[1, 2, 3]), vec![2, 2, 2], "{plan:?}");
        // Two for p0 and one more so broker 3 gets its share.
        assert_eq!(replicas_to_move(&now, &plan), 3);

        // The same at scale.
        let mut rows: Vec<&[i32]> = vec![&[4, 5]; 10];
        rows.extend(vec![&[1, 2][..]; 20]);
        let now = layout(&rows);
        let plan = generate_minimal_move_plan(&now, &[1, 2, 3], None).unwrap();
        plan.validate().unwrap();
        assert_eq!(replica_counts(&plan, &[1, 2, 3]), vec![20, 20, 20]);
    }

    #[test]
    fn minimal_plan_is_valid_and_even_for_random_layouts() {
        // Small LCG: deterministic, no extra dependency.
        let mut seed = 0x2545_f491_4f6c_dd1du64;
        let mut next = |n: usize| {
            seed = seed
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            (seed >> 33) as usize % n
        };
        for _ in 0..500 {
            let old_brokers = 1 + next(6);
            let new: Vec<i32> = (1..=(1 + next(6) as i32)).collect();
            let rf = 1 + next(new.len().min(old_brokers));
            let now: Vec<PartitionAssignment> = (0..1 + next(40))
                .map(|p| {
                    let mut replicas: Vec<i32> = Vec::new();
                    while replicas.len() < rf {
                        let b = 1 + next(old_brokers) as i32;
                        if !replicas.contains(&b) {
                            replicas.push(b);
                        }
                    }
                    PartitionAssignment::new("t", p as i32, replicas)
                })
                .collect();
            let plan = generate_minimal_move_plan(&now, &new, None).unwrap();
            plan.validate().unwrap();
            let total = now.len() * rf;
            let (lo, hi) = (total / new.len(), total.div_ceil(new.len()));
            for (b, n) in new.iter().zip(replica_counts(&plan, &new)) {
                assert!(
                    (lo..=hi).contains(&n),
                    "broker {b}: {n} not in {lo}..={hi}: {now:?} -> {plan:?}"
                );
            }
            for after in &plan.partitions {
                assert_eq!(after.replicas.len(), rf);
                assert!(after.replicas.iter().all(|b| new.contains(b)));
            }
        }
    }

    #[test]
    fn minimal_plan_counts_a_repeated_replica_once() {
        let now = vec![PartitionAssignment::new("t", 0, vec![1, 1])];
        let plan = generate_minimal_move_plan(&now, &[1, 2], None).unwrap();
        assert_eq!(plan.partitions[0].replicas, vec![1]);
        let empty = vec![PartitionAssignment::new("t", 0, vec![])];
        let err = generate_minimal_move_plan(&empty, &[1, 2], None).unwrap_err();
        assert!(err.to_string().contains("t-0 has no replicas"), "{err}");
    }

    #[test]
    fn minimal_plan_checks_its_input() {
        let now = layout(&[&[1, 2]]);
        assert!(generate_minimal_move_plan(&now, &[], None).is_err());
        assert!(generate_minimal_move_plan(&now, &[1], None).is_err());
        assert!(generate_minimal_move_plan(&now, &[1, 2], Some(0)).is_err());
        // A repeated broker id counts once.
        assert!(generate_minimal_move_plan(&now, &[1, 1], None).is_err());
    }

    #[test]
    fn pre_move_replicas_drops_only_adding() {
        let m = crate::model::cluster::OngoingReassignment {
            topic: "t".into(),
            partition: 0,
            replicas: vec![1, 2, 4],
            adding: vec![4],
            removing: vec![2],
        };
        assert_eq!(pre_move_replicas(&[1, 2, 4], Some(&m)), vec![1, 2]);
        assert_eq!(pre_move_replicas(&[1, 2], None), vec![1, 2]);
        assert_eq!(pre_move_replicas(&[], Some(&m)), Vec::<i32>::new());
    }

    #[test]
    fn balanced_plan_dedups_brokers_and_checks_rf_against_distinct_count() {
        let plan = generate_balanced_plan("t", 3, &[1, 1, 2], 2, 0).unwrap();
        assert!(plan.partitions.iter().all(|p| p.replicas.len() == 2));
        assert!(plan
            .partitions
            .iter()
            .all(|p| p.replicas[0] != p.replicas[1]));
        assert!(generate_balanced_plan("t", 3, &[1, 1, 1], 2, 0).is_err());
    }

    #[test]
    fn balanced_plan_rejects_absurd_partition_counts() {
        assert!(generate_balanced_plan("t", 100_000_000, &[1, 2], 1, 0).is_err());
        assert!(generate_balanced_plan("t", MAX_PLAN_PARTITIONS, &[1], 1, 0).is_ok());
    }

    #[test]
    fn dedup_keeps_first_occurrence_order() {
        assert_eq!(dedup_preserving_order(&["a", "b", "a"]), vec!["a", "b"]);
    }

    #[test]
    fn validate_rejects_bad_plans() {
        let ok = PartitionAssignment::new("t", 0, vec![1, 2]);
        assert!(ReassignmentPlan::new(vec![ok.clone()]).validate().is_ok());
        assert!(ReassignmentPlan::new(vec![]).validate().is_err());
        let empty = PartitionAssignment::new("t", 1, vec![]);
        assert!(ReassignmentPlan::new(vec![empty]).validate().is_err());
        let dup_replica = PartitionAssignment::new("t", 1, vec![3, 3]);
        assert!(ReassignmentPlan::new(vec![dup_replica]).validate().is_err());
        assert!(ReassignmentPlan::new(vec![ok.clone(), ok])
            .validate()
            .is_err());
    }

    #[test]
    fn change_replication_moves_as_little_as_possible() {
        use std::collections::HashMap;
        let parts = vec![(0, vec![1, 2]), (1, vec![2, 3]), (2, vec![3, 1])];
        let brokers = [1, 2, 3, 4];
        let mut load: std::collections::HashMap<i32, usize> =
            [(1, 2), (2, 2), (3, 2), (4, 0)].into_iter().collect();
        let up = change_replication("t", &parts, &brokers, &mut load.clone(), 3).unwrap();
        // Current replicas stay in place; broker 4 (empty) takes new ones first.
        assert_eq!(up[0].replicas[..2], [1, 2]);
        assert_eq!(up[0].replicas[2], 4);
        assert!(up.iter().all(|a| a.replicas.len() == 3));

        let down = change_replication("t", &parts, &brokers, &mut load, 1).unwrap();
        assert_eq!(
            down.iter().map(|a| a.replicas.clone()).collect::<Vec<_>>(),
            vec![vec![1], vec![2], vec![3]]
        );
        assert!(change_replication("t", &parts, &brokers, &mut HashMap::new(), 5).is_err());
        assert!(
            change_replication("t", &parts, &brokers, &mut HashMap::new(), 2)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn test_generate_balanced_plan_distribution() {
        let brokers = vec![1, 2, 3];
        let plan = generate_balanced_plan("orders-v1", 6, &brokers, 2, 0).unwrap();

        assert_eq!(plan.version, 1);
        assert_eq!(plan.partitions.len(), 6);

        // Check each partition has RF=2 and distinct brokers
        for p in &plan.partitions {
            assert_eq!(p.replicas.len(), 2);
            assert_ne!(p.replicas[0], p.replicas[1]);
        }

        // Check leader distribution across partitions:
        // p0: [1, 2], p1: [2, 3], p2: [3, 1], p3: [1, 2], p4: [2, 3], p5: [3, 1]
        assert_eq!(plan.partitions[0].replicas, vec![1, 2]);
        assert_eq!(plan.partitions[1].replicas, vec![2, 3]);
        assert_eq!(plan.partitions[2].replicas, vec![3, 1]);
        assert_eq!(plan.partitions[3].replicas, vec![1, 2]);
        assert_eq!(plan.partitions[4].replicas, vec![2, 3]);
        assert_eq!(plan.partitions[5].replicas, vec![3, 1]);
    }

    #[test]
    fn test_generate_balanced_plan_rf_exceeds_brokers_fails() {
        let brokers = vec![1, 2];
        let result = generate_balanced_plan("orders-v1", 4, &brokers, 3, 0);
        assert!(result.is_err());
    }

    #[test]
    fn test_generate_balanced_plan_rotates_leaders_across_topics() {
        let brokers = vec![1, 2, 3];
        let mut placed = 0;
        let mut leaders = std::collections::HashMap::new();
        for topic in ["a", "b", "c", "d", "e", "f"] {
            let plan = generate_balanced_plan(topic, 1, &brokers, 2, placed).unwrap();
            placed += plan.partitions.len();
            *leaders.entry(plan.partitions[0].replicas[0]).or_insert(0) += 1;
        }
        assert_eq!(leaders.get(&1), Some(&2));
        assert_eq!(leaders.get(&2), Some(&2));
        assert_eq!(leaders.get(&3), Some(&2));

        let plan = generate_balanced_plan("t", 2, &brokers, 2, 4).unwrap();
        assert_eq!(plan.partitions[0].replicas, vec![2, 3]);
        assert_eq!(plan.partitions[1].replicas, vec![3, 1]);
    }

    #[test]
    fn test_partition_move_delta_calculation() {
        let current = vec![1, 2];
        let target = vec![2, 3];
        let delta = PartitionMoveDelta::calculate("orders-v1", 0, &current, &target);

        assert!(delta.has_movement());
        assert_eq!(delta.added_replicas, vec![3]);
        assert_eq!(delta.removed_replicas, vec![1]);

        // No movement test
        let no_change = PartitionMoveDelta::calculate("orders-v1", 0, &current, &current);
        assert!(!no_change.has_movement());
    }

    #[test]
    fn test_reassignment_plan_json_roundtrip() {
        let assignment = PartitionAssignment::new("test-topic", 0, vec![101, 102]);
        let plan = ReassignmentPlan::new(vec![assignment]);

        let json = plan.to_json_pretty().unwrap();
        assert!(json.contains("\"version\": 1"));
        assert!(json.contains("\"test-topic\""));

        let restored = ReassignmentPlan::from_json(&json).unwrap();
        assert_eq!(plan, restored);
    }

    #[test]
    fn test_evaluate_partition_status() {
        let target = vec![1, 2];

        // Fully completed: live replicas == target and all in ISR
        assert_eq!(
            evaluate_partition_status(&[1, 2], &[1, 2], &target),
            ReassignmentStatus::Completed
        );

        // In progress: still replicating
        assert_eq!(
            evaluate_partition_status(&[1, 2, 3], &[1, 3], &target),
            ReassignmentStatus::InProgress
        );
    }

    #[test]
    fn validate_rejects_negative_numbers_and_planning_edges() {
        let neg_partition = PartitionAssignment::new("t", -1, vec![1]);
        assert!(ReassignmentPlan::new(vec![neg_partition])
            .validate()
            .is_err());
        let neg_broker = PartitionAssignment::new("t", 0, vec![1, -2]);
        assert!(ReassignmentPlan::new(vec![neg_broker]).validate().is_err());
        assert!(generate_balanced_plan("t", 3, &[1, 2], 0, 0).is_err());
        assert!(generate_balanced_plan("t", 3, &[], 1, 0).is_err());
        let empty = generate_balanced_plan("t", 0, &[1], 1, 0).unwrap();
        assert!(empty.partitions.is_empty());
        assert!(empty.validate().is_err());
        // A huge start offset still picks a valid broker.
        let plan = generate_balanced_plan("t", 2, &[7, 8, 9], 2, usize::MAX / 2).unwrap();
        assert!(plan
            .partitions
            .iter()
            .all(|p| p.replicas.iter().all(|r| [7, 8, 9].contains(r))));
        assert!(ReassignmentPlan::from_json("{\"version\":1}").is_err());
        assert!(ReassignmentPlan::from_json("").is_err());
    }
}
