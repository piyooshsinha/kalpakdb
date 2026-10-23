//! Non-authoritative comparison of retention observations. Never releases data.
use serde::Deserialize;
use serde_json::{json, Value};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ExpectedFences {
    pub storage_generation: String,
    pub local_generation: u64,
    pub block_reference_fence: u64,
}

pub(crate) fn validate(expected: &ExpectedFences, observation: Value) -> Value {
    let mut blockers = Vec::new();
    let mut fences_match = true;
    for (actual, wanted, reason) in [
        (
            &observation["storage_generation"],
            json!(expected.storage_generation),
            "storage_generation_mismatch",
        ),
        (
            &observation["local_guard"]["local_generation"],
            json!(expected.local_generation),
            "local_generation_unknown_or_changed",
        ),
        (
            &observation["metadata"]["block_reference_fence"],
            json!(expected.block_reference_fence),
            "reference_fence_unknown_or_changed",
        ),
    ] {
        if actual.is_null() || *actual != wanted {
            fences_match = false;
            blockers.push(reason);
        }
    }
    if observation["local_guard"]["guarded"] != true {
        blockers.push("local_guard_missing");
    }
    if observation["local_guard"]["write_failed"] != false {
        blockers.push("journal_health_unknown_or_failed");
    }
    for (value, reason) in [
        (
            &observation["metadata"]["binding_references"],
            "binding_references_present_or_unknown",
        ),
        (
            &observation["metadata"]["leases"]["pending"],
            "pending_leases_present_or_unknown",
        ),
        (
            &observation["metadata"]["required_replicas"],
            "replica_requirement_present_or_unknown",
        ),
    ] {
        if value.as_u64() != Some(0) {
            blockers.push(reason);
        }
    }
    // Even matched fences with no observed references are not safe authority:
    // samples are neither atomic across planes nor linearizable cluster-wide.
    blockers.extend([
        "in_flight_writes_not_coordinated",
        "legacy_provenance_not_verified",
        "cluster_gc_watermark_missing",
        "linearizable_release_validation_missing",
        "restore_rollback_not_fenced",
        "physical_gc_disabled",
    ]);
    json!({"mode": "dry_run", "authoritative": false, "reclaimable": false,
        "release_authorized": false, "gc_enabled": false,
        "observed_fences_match": fences_match, "blockers": blockers,
        "observation": observation})
}

#[cfg(test)]
mod tests {
    use super::*;
    fn expected() -> ExpectedFences {
        ExpectedFences {
            storage_generation: "incarnation".into(),
            local_generation: 7,
            block_reference_fence: 11,
        }
    }
    fn sample() -> Value {
        json!({"storage_generation": "incarnation",
            "local_guard": {"guarded": true, "local_generation": 7, "write_failed": false},
            "metadata": {"block_reference_fence": 11, "binding_references": 0,
                "required_replicas": 0, "leases": {"pending": 0, "expired": 1}}})
    }
    #[test]
    fn matching_fences_and_expired_lease_never_authorize_release() {
        let report = validate(&expected(), sample());
        assert_eq!(report["observed_fences_match"], true);
        assert_eq!(report["release_authorized"], false);
        assert_eq!(report["reclaimable"], false);
        assert_eq!(report["blockers"].as_array().unwrap().len(), 6);
    }
    #[test]
    fn stale_and_unknown_fences_fail_closed() {
        for (section, field) in [
            ("local_guard", "local_generation"),
            ("metadata", "block_reference_fence"),
        ] {
            for value in [Value::Null, json!(12)] {
                let mut sample = sample();
                sample[section][field] = value;
                assert_eq!(
                    validate(&expected(), sample)["observed_fences_match"],
                    false
                );
            }
        }
        let mut sample = sample();
        sample["storage_generation"] = json!("replacement node");
        assert_eq!(
            validate(&expected(), sample)["observed_fences_match"],
            false
        );
    }
    #[test]
    fn shared_bindings_leases_and_replica_requirements_are_all_reported() {
        let mut sample = sample();
        sample["metadata"]["binding_references"] = json!(2);
        sample["metadata"]["leases"]["pending"] = json!(1);
        sample["metadata"]["required_replicas"] = json!(3);
        assert_eq!(
            validate(&expected(), sample)["blockers"]
                .as_array()
                .unwrap()
                .len(),
            9
        );
        assert!(!validate(&expected(), json!({}))["blockers"]
            .as_array()
            .unwrap()
            .is_empty());
    }
}
