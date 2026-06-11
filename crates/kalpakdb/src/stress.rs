//! `kalpakdb stress`: concurrent agent workload generator.
//!
//! Drives N simulated agents against a running node, each looping the real
//! workflow — chain keys, lookup (miss), batch-offload chunks, bind with
//! lineage, lookup (hit), fetch warm blocks — and reports throughput and
//! latency percentiles per operation. This is the pre-deployment answer to
//! "what does p99 look like under load", runnable against any node or
//! cluster member.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde_json::{json, Value};

#[derive(Default)]
struct Lat {
    lookup_ms: Vec<f64>,
    put_ms: Vec<f64>,
    bind_ms: Vec<f64>,
    get_ms: Vec<f64>,
}

fn pct(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return f64::NAN;
    }
    let idx = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[idx]
}

fn report(label: &str, mut v: Vec<f64>) {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    println!(
        "  {label:<10} {:>7} ops   p50 {:>7.2} ms   p95 {:>7.2} ms   p99 {:>7.2} ms   max {:>7.2} ms",
        v.len(),
        pct(&v, 0.50),
        pct(&v, 0.95),
        pct(&v, 0.99),
        v.last().copied().unwrap_or(f64::NAN),
    );
}

pub struct StressOpts {
    pub base: String,
    pub agents: usize,
    pub secs: u64,
    pub chunk_kb: usize,
}

pub async fn stress(opts: StressOpts) -> Result<(), Box<dyn std::error::Error>> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()?;

    // Fail fast if the node is unreachable.
    client
        .get(format!("{}/v1/stats", opts.base))
        .send()
        .await
        .map_err(|e| format!("cannot reach {}: {e}", opts.base))?;

    println!(
        "stressing {} with {} agents for {}s ({} KiB chunks)…",
        opts.base, opts.agents, opts.secs, opts.chunk_kb
    );

    let stop = Arc::new(AtomicBool::new(false));
    let errors = Arc::new(AtomicU64::new(0));
    let lat = Arc::new(Mutex::new(Lat::default()));
    let started = Instant::now();

    let mut tasks = Vec::new();
    for worker in 0..opts.agents {
        let base = opts.base.clone();
        let client = client.clone();
        let stop = stop.clone();
        let errors = errors.clone();
        let lat = lat.clone();
        let chunk = opts.chunk_kb * 1024;

        tasks.push(tokio::spawn(async move {
            // One identity per simulated agent.
            let agent = format!("{:02x}", worker as u8 + 1).repeat(32);
            let _ = client
                .post(format!("{base}/v1/agents"))
                .json(&json!({ "agent": agent, "display_name": format!("stress-{worker}") }))
                .send()
                .await;

            let fp = json!({
                "model_id": "stress/model",
                "tokenizer_hash": "tok",
                "kv_layout": "fp16/paged-16"
            });
            let mut round: u64 = 0;

            while !stop.load(Ordering::Relaxed) {
                round += 1;
                // Fresh token stream per round: pass 1 misses, pass 2 hits.
                let salt = ((worker as u64) << 32 | round) as u32;
                let chunks: [Vec<u32>; 3] = [vec![salt, worker as u32, 1], vec![2, 3], vec![4]];
                // Chain keys server-side via /v1/keys (root, then extend).
                let mut keys: Vec<Value> = Vec::with_capacity(3);
                let mut key_err = false;
                for tokens in &chunks {
                    let parent = keys.last().cloned().unwrap_or(Value::Null);
                    let resp = client
                        .post(format!("{base}/v1/keys"))
                        .json(&json!({
                            "fingerprint": &fp,
                            "tokens": tokens,
                            "parent": parent
                        }))
                        .send()
                        .await;
                    match resp {
                        Ok(r) => match r.json::<Value>().await {
                            Ok(v) => keys.push(v),
                            Err(_) => {
                                key_err = true;
                                break;
                            }
                        },
                        Err(_) => {
                            key_err = true;
                            break;
                        }
                    }
                }
                if key_err || keys.len() != 3 {
                    errors.fetch_add(1, Ordering::Relaxed);
                    continue;
                }

                // lookup (miss path)
                let t = Instant::now();
                let _ = client
                    .post(format!("{base}/v1/manifest/lookup"))
                    .json(&json!({ "chain": keys }))
                    .send()
                    .await;
                let lookup1 = t.elapsed().as_secs_f64() * 1e3;

                // batch offload: 3 chunks, one group commit
                let mut body = Vec::with_capacity(4 + 3 * (4 + chunk));
                body.extend_from_slice(&3u32.to_le_bytes());
                for i in 0..3u64 {
                    let mut payload = vec![0u8; chunk];
                    payload[..8].copy_from_slice(&((salt as u64) ^ (i << 56)).to_le_bytes());
                    body.extend_from_slice(&(chunk as u32).to_le_bytes());
                    body.extend_from_slice(&payload);
                }
                let t = Instant::now();
                let ids: Vec<String> = match client
                    .post(format!("{base}/v1/blocks/batch"))
                    .body(body)
                    .send()
                    .await
                {
                    Ok(r) => match r.json::<Value>().await {
                        Ok(v) => v["ids"]
                            .as_array()
                            .map(|a| {
                                a.iter()
                                    .filter_map(|x| x.as_str().map(String::from))
                                    .collect()
                            })
                            .unwrap_or_default(),
                        Err(_) => {
                            errors.fetch_add(1, Ordering::Relaxed);
                            continue;
                        }
                    },
                    Err(_) => {
                        errors.fetch_add(1, Ordering::Relaxed);
                        continue;
                    }
                };
                let put = t.elapsed().as_secs_f64() * 1e3;
                if ids.len() != 3 {
                    errors.fetch_add(1, Ordering::Relaxed);
                    continue;
                }

                // bind the whole chain atomically: ONE consensus round
                let bindings: Vec<Value> = keys
                    .iter()
                    .enumerate()
                    .map(|(i, key)| {
                        let parent = if i == 0 {
                            Value::Null
                        } else {
                            keys[i - 1].clone()
                        };
                        json!({ "key": key, "blocks": ids[..=i], "parent": parent })
                    })
                    .collect();
                let t = Instant::now();
                if client
                    .post(format!("{base}/v1/manifest/bind-chain"))
                    .json(&json!({ "agent": agent, "bindings": bindings }))
                    .send()
                    .await
                    .is_err()
                {
                    errors.fetch_add(1, Ordering::Relaxed);
                }
                let bind = t.elapsed().as_secs_f64() * 1e3;

                // lookup (hit path) + warm fetch of the deepest blocks
                let t = Instant::now();
                let _ = client
                    .post(format!("{base}/v1/manifest/lookup"))
                    .json(&json!({ "chain": keys }))
                    .send()
                    .await;
                let lookup2 = t.elapsed().as_secs_f64() * 1e3;

                let t = Instant::now();
                for id in &ids {
                    if client
                        .get(format!("{base}/v1/blocks/{id}"))
                        .send()
                        .await
                        .is_err()
                    {
                        errors.fetch_add(1, Ordering::Relaxed);
                    }
                }
                let get = t.elapsed().as_secs_f64() * 1e3;

                let mut l = lat.lock().unwrap();
                l.lookup_ms.push(lookup1);
                l.lookup_ms.push(lookup2);
                l.put_ms.push(put);
                l.bind_ms.push(bind);
                l.get_ms.push(get);
            }
        }));
    }

    tokio::time::sleep(Duration::from_secs(opts.secs)).await;
    stop.store(true, Ordering::Relaxed);
    for t in tasks {
        let _ = t.await;
    }

    let elapsed = started.elapsed().as_secs_f64();
    let l = std::mem::take(&mut *lat.lock().unwrap());
    let rounds = l.put_ms.len();
    println!(
        "\n{} workflow rounds in {elapsed:.1}s  ({:.1} rounds/s, {} errors)",
        rounds,
        rounds as f64 / elapsed,
        errors.load(Ordering::Relaxed),
    );
    println!("latency per operation (ms):");
    report("lookup", l.lookup_ms);
    report("batch put", l.put_ms);
    report("bind chain", l.bind_ms);
    report("get x3", l.get_ms);
    Ok(())
}
