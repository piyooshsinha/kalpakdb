//! Opt-in physical diagnostics; findings do not mutate logical block state.
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering::Relaxed},
    Arc,
};
use std::time::Duration;

use crate::server::AppState;
use kalpak_storage::online_scrub::OnlineScrubber;

#[derive(Default)]
pub(crate) struct Metrics {
    running: AtomicBool,
    bytes: AtomicU64,
    verified: AtomicU64,
    findings: AtomicU64,
    resets: AtomicU64,
    yields: AtomicU64,
    errors: AtomicU64,
}

impl Metrics {
    pub(crate) fn snapshot(&self) -> serde_json::Value {
        serde_json::json!({"running": self.running.load(Relaxed),
            "bytes_read": self.bytes.load(Relaxed), "verified": self.verified.load(Relaxed),
            "physical_findings": self.findings.load(Relaxed), "generation_resets": self.resets.load(Relaxed),
            "foreground_yields": self.yields.load(Relaxed), "errors": self.errors.load(Relaxed)})
    }
}

pub(crate) fn start(state: Arc<AppState>, mut scanner: OnlineScrubber<std::fs::File>, rate: u64) {
    state.online_scrub.running.store(true, Relaxed);
    tokio::spawn(async move {
        loop {
            // Never queue background work ahead of an already queued probe.
            let Ok(permit) = state.scrub_slots.clone().try_acquire_owned() else {
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            };
            let store = state.store.clone();
            let work = tokio::task::spawn_blocking(move || {
                let _permit = permit;
                let result = scanner.step(store.cold());
                (scanner, result)
            })
            .await;
            let step = match work {
                Ok((returned, Ok(step))) => {
                    scanner = returned;
                    step
                }
                other => {
                    state.online_scrub.errors.fetch_add(1, Relaxed);
                    state.online_scrub.running.store(false, Relaxed);
                    match other {
                        Ok((_, Err(error))) => eprintln!("online scrub stopped: {error}"),
                        Err(error) => eprintln!("online scrub task stopped: {error}"),
                        _ => unreachable!(),
                    }
                    return;
                }
            };
            let metrics = &state.online_scrub;
            metrics.bytes.fetch_add(step.bytes_read, Relaxed);
            metrics.verified.fetch_add(step.verified, Relaxed);
            metrics
                .resets
                .fetch_add(u64::from(step.generation_reset), Relaxed);
            metrics.yields.fetch_add(u64::from(step.yielded), Relaxed);
            for finding in step.findings {
                if metrics.findings.fetch_add(1, Relaxed) < 16 {
                    eprintln!(
                        "online scrub physical finding (not a logical repair verdict): {finding:?}"
                    );
                }
            }
            // Charge page-rounded reads, including repeated pages. No catch-up
            // bursts; checkpoint I/O is additional to this read budget.
            let delay = if step.page_budget_bytes == 0 {
                Duration::from_millis(100)
            } else {
                Duration::from_secs_f64(step.page_budget_bytes as f64 / (rate * 1024 * 1024) as f64)
                    .max(Duration::from_millis(1))
            };
            tokio::time::sleep(delay).await;
        }
    });
}
