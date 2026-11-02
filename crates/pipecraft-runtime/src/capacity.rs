//! Pure, reproducible capacity scaling from provider-supplied observations.
//! No host metrics are sampled here; the caller supplies them explicitly.

use serde::{Deserialize, Serialize};

fn default_initial() -> u32 {
    4
}
fn default_ceiling() -> u32 {
    8
}
fn one() -> u32 {
    1
}
fn default_clean_growth() -> u32 {
    5
}
fn default_min_samples() -> u32 {
    4
}
fn default_bad_groups() -> u32 {
    2
}
fn default_failure_ratio() -> f64 {
    0.25
}
fn default_reserve() -> u64 {
    1024
}
fn default_mem_point() -> u64 {
    700
}
fn default_cpu_ratio() -> f64 {
    0.85
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CapacityProfile {
    #[serde(default = "default_initial")]
    pub initial: u32,
    #[serde(default = "default_ceiling")]
    pub ceiling: u32,
    #[serde(default = "one")]
    pub floor: u32,
    #[serde(default = "default_clean_growth")]
    pub grow_every_clean: u32,
    #[serde(default = "default_min_samples")]
    pub min_samples: u32,
    #[serde(default = "default_bad_groups")]
    pub min_bad_groups: u32,
    #[serde(default = "default_failure_ratio")]
    pub failure_ratio: f64,
    #[serde(default = "default_reserve")]
    pub memory_reserve_mib: u64,
    #[serde(default = "default_mem_point")]
    pub mib_per_point: u64,
    #[serde(default = "default_cpu_ratio")]
    pub cpu_load_limit: f64,
    #[serde(default)]
    pub fixed: Option<u32>,
}
impl Default for CapacityProfile {
    fn default() -> Self {
        Self {
            initial: 4,
            ceiling: 8,
            floor: 1,
            grow_every_clean: 5,
            min_samples: 4,
            min_bad_groups: 2,
            failure_ratio: 0.25,
            memory_reserve_mib: 1024,
            mib_per_point: 700,
            cpu_load_limit: 0.85,
            fixed: None,
        }
    }
}
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CapacityEvidence {
    #[serde(default)]
    pub clean: u32,
    #[serde(default)]
    pub bad: u32,
    #[serde(default)]
    pub bad_groups: u32,
    #[serde(default)]
    pub memory_available_mib: Option<u64>,
    #[serde(default)]
    pub cpu_load_ratio: Option<f64>,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CapacityResult {
    pub points: u32,
    pub reason: String,
}

pub fn calculate(
    profile: &CapacityProfile,
    evidence: &CapacityEvidence,
) -> Result<CapacityResult, String> {
    if profile.floor == 0
        || profile.ceiling < profile.floor
        || profile.grow_every_clean == 0
        || profile.mib_per_point == 0
        || !profile.failure_ratio.is_finite()
        || !(0.0..=1.0).contains(&profile.failure_ratio)
        || !profile.cpu_load_limit.is_finite()
        || !(0.0..=1.0).contains(&profile.cpu_load_limit)
        || evidence
            .cpu_load_ratio
            .is_some_and(|ratio| !ratio.is_finite() || ratio < 0.0)
    {
        return Err("invalid capacity profile or resource evidence".into());
    }
    if let Some(fixed) = profile.fixed {
        return Ok(CapacityResult {
            points: fixed.max(profile.floor),
            reason: "FIXED".into(),
        });
    }
    let samples = evidence.clean.saturating_add(evidence.bad);
    let widespread = samples >= profile.min_samples
        && evidence.bad_groups >= profile.min_bad_groups
        && f64::from(evidence.bad) / f64::from(samples.max(1)) >= profile.failure_ratio;
    let (mut points, mut reason) = if widespread {
        (profile.floor, "WIDESPREAD_FAILURES")
    } else {
        let earned = evidence.clean / profile.grow_every_clean;
        let base = if evidence.bad > 0 {
            profile.floor
        } else {
            profile.initial
        };
        let name = if earned > 0 {
            "GROWN_BY_EVIDENCE"
        } else if evidence.bad > 0 {
            "RECOVERING"
        } else {
            "INITIAL"
        };
        (base.saturating_add(earned), name)
    };
    points = points.min(profile.ceiling);
    if let Some(mem) = evidence.memory_available_mib {
        let mem_points = mem.saturating_sub(profile.memory_reserve_mib) / profile.mib_per_point;
        if mem_points < u64::from(points) {
            points = (mem_points.min(u64::from(u32::MAX)) as u32).max(profile.floor);
            reason = "MEMORY_PRESSURE";
        }
    }
    if evidence
        .cpu_load_ratio
        .is_some_and(|ratio| ratio >= profile.cpu_load_limit)
    {
        points = (points / 2).max(profile.floor);
        reason = "CPU_PRESSURE";
    }
    Ok(CapacityResult {
        points: points.max(profile.floor),
        reason: reason.into(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn capacity_grows_and_recovers_from_widespread_failures() {
        let profile = CapacityProfile::default();
        let mut evidence = CapacityEvidence::default();
        assert_eq!(calculate(&profile, &evidence).unwrap().points, 4);
        evidence.clean = 5;
        assert_eq!(calculate(&profile, &evidence).unwrap().points, 5);
        evidence.bad = 2;
        evidence.bad_groups = 2;
        assert_eq!(
            calculate(&profile, &evidence).unwrap().reason,
            "WIDESPREAD_FAILURES"
        );
        evidence.memory_available_mib = Some(1200);
        assert_eq!(calculate(&profile, &evidence).unwrap().points, 1);
    }
    #[test]
    fn fixed_capacity_and_invalid_inputs() {
        let mut profile = CapacityProfile {
            fixed: Some(3),
            ..CapacityProfile::default()
        };
        assert_eq!(
            calculate(&profile, &CapacityEvidence::default())
                .unwrap()
                .points,
            3
        );
        profile.fixed = None;
        profile.failure_ratio = f64::NAN;
        assert!(calculate(&profile, &CapacityEvidence::default()).is_err());
    }
}
