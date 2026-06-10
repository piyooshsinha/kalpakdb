//! Three-node cluster integration test: real HTTP between in-process nodes.
//!
//! Boots three `serve()` instances on ephemeral ports, forms a cluster
//! (init → add learners → promote to voters), writes metadata through the
//! leader, and verifies it replicates to every follower's state machine.

use std::time::{Duration, Instant};

use kalpakdb::server::{serve, ServeOpts};
use serde_json::{json, Value};

async fn spawn_node(node_id: u64, port: u16, dir: &std::path::Path) -> String {
    let addr = format!("127.0.0.1:{port}");
    let opts = ServeOpts {
        data_dir: dir
            .join(format!("n{node_id}"))
            .to_string_lossy()
            .into_owned(),
        addr: addr.clone(),
        warm_bytes: 16 * 1024 * 1024,
        node_id,
        bootstrap: false,
    };
    tokio::spawn(async move {
        if let Err(e) = serve(opts).await {
            eprintln!("node exited: {e}");
        }
    });
    // Wait until the API answers.
    let client = reqwest::Client::new();
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if client
            .get(format!("http://{addr}/v1/stats"))
            .send()
            .await
            .is_ok()
        {
            return addr;
        }
        assert!(Instant::now() < deadline, "node {node_id} did not start");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn post(client: &reqwest::Client, url: String, body: Value) -> Value {
    let resp = client.post(&url).json(&body).send().await.unwrap();
    let status = resp.status();
    let v: Value = resp.json().await.unwrap();
    assert!(status.is_success(), "POST {url} failed: {v}");
    v
}

async fn stats(client: &reqwest::Client, addr: &str) -> Value {
    client
        .get(format!("http://{addr}/v1/stats"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn three_node_cluster_replicates_metadata() {
    let dir = tempfile::tempdir().unwrap();
    let client = reqwest::Client::new();

    let n1 = spawn_node(1, 17511, dir.path()).await;
    let n2 = spawn_node(2, 17512, dir.path()).await;
    let n3 = spawn_node(3, 17513, dir.path()).await;

    // Form the cluster from node 1.
    post(
        &client,
        format!("http://{n1}/v1/cluster/init"),
        json!({ "members": { "1": n1 } }),
    )
    .await;
    post(
        &client,
        format!("http://{n1}/v1/cluster/add-learner"),
        json!({ "node_id": 2, "addr": n2 }),
    )
    .await;
    post(
        &client,
        format!("http://{n1}/v1/cluster/add-learner"),
        json!({ "node_id": 3, "addr": n3 }),
    )
    .await;
    post(
        &client,
        format!("http://{n1}/v1/cluster/promote"),
        json!({ "voters": [1, 2, 3] }),
    )
    .await;

    // Write metadata through the leader.
    let agent = "07".repeat(32);
    post(
        &client,
        format!("http://{n1}/v1/agents"),
        json!({ "agent": agent, "display_name": "distributed-researcher" }),
    )
    .await;

    // Upload a block on the leader and bind a prefix to it.
    let block_resp: Value = client
        .post(format!("http://{n1}/v1/blocks"))
        .body("kv-tensor-replicated")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let block_id = block_resp["id"].as_str().unwrap().to_string();

    let key = json!({
        "fingerprint": {
            "model_id": "test/model",
            "tokenizer_hash": "tok",
            "kv_layout": "fp16/paged-16"
        },
        // BLAKE3 of tokens [1,2,3] — value irrelevant to replication, any
        // 32-byte hex works as a prefix hash.
        "prefix_hash": "11".repeat(32),
    });
    post(
        &client,
        format!("http://{n1}/v1/manifest/bind"),
        json!({ "agent": agent, "key": key, "blocks": [block_id] }),
    )
    .await;

    // Every node's applied state machine must converge: 1 agent, 1 binding.
    let deadline = Instant::now() + Duration::from_secs(10);
    'outer: loop {
        for addr in [&n1, &n2, &n3] {
            let s = stats(&client, addr).await;
            let agents = s["control_plane"]["agents"].as_u64().unwrap_or(0);
            let bindings = s["control_plane"]["bindings"].as_u64().unwrap_or(0);
            if agents != 1 || bindings != 1 {
                assert!(
                    Instant::now() < deadline,
                    "replication did not converge on {addr}: {s}"
                );
                tokio::time::sleep(Duration::from_millis(150)).await;
                continue 'outer;
            }
        }
        break;
    }

    // All three nodes agree on the leader.
    for addr in [&n1, &n2, &n3] {
        let s = stats(&client, addr).await;
        assert_eq!(
            s["control_plane"]["leader"].as_u64(),
            Some(1),
            "node at {addr} disagrees on leadership: {s}"
        );
    }

    // The lookup chain served from a follower sees the replicated binding.
    let lookup: Value = post(
        &client,
        format!("http://{n2}/v1/manifest/lookup"),
        json!({ "chain": [key] }),
    )
    .await;
    assert_eq!(lookup["hit_depth"].as_u64(), Some(0));
    assert_eq!(lookup["blocks"].as_array().unwrap().len(), 1);
}
