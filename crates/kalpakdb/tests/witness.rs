//! Witness quorum: two data nodes + one consensus-only witness.
//!
//! This is the dev-hardware topology from the project plan (storage box +
//! compute box + lightweight third vote). The test verifies that the witness
//! gives the pair strict quorum: when one data node dies, the survivor plus
//! the witness still commit writes — and the witness itself never stores
//! data-plane blocks.

use std::time::{Duration, Instant};

use kalpakdb::server::{serve, serve_witness, ServeOpts};
use serde_json::{json, Value};

struct NodeProc {
    addr: String,
    kill: std::sync::mpsc::Sender<()>,
}

fn spawn(node_id: u64, port: u16, dir: &std::path::Path, witness: bool) -> NodeProc {
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
    let (kill, killed) = std::sync::mpsc::channel::<()>();
    std::thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.spawn(async move {
            if witness {
                let _ = serve_witness(opts).await;
            } else {
                let _ = serve(opts).await;
            }
        });
        let _ = killed.recv();
        rt.shutdown_timeout(Duration::from_millis(200));
    });
    NodeProc { addr, kill }
}

async fn wait_online(client: &reqwest::Client, addr: &str) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if client
            .get(format!("http://{addr}/v1/stats"))
            .send()
            .await
            .is_ok()
        {
            return;
        }
        assert!(Instant::now() < deadline, "node at {addr} did not start");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn post_ok(client: &reqwest::Client, url: String, body: Value) -> Value {
    let resp = client.post(&url).json(&body).send().await.unwrap();
    let status = resp.status();
    let v: Value = resp.json().await.unwrap();
    assert!(status.is_success(), "POST {url} failed: {v}");
    v
}

#[tokio::test(flavor = "multi_thread")]
async fn witness_gives_two_data_nodes_quorum() {
    let dir = tempfile::tempdir().unwrap();
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .unwrap();

    // Two data nodes + one witness (the project-plan topology).
    let storage = spawn(1, 17541, dir.path(), false);
    let compute = spawn(2, 17542, dir.path(), false);
    let witness = spawn(3, 17543, dir.path(), true);
    for n in [&storage, &compute, &witness] {
        wait_online(&client, &n.addr).await;
    }

    post_ok(
        &client,
        format!("http://{}/v1/cluster/init", storage.addr),
        json!({ "members": { "1": storage.addr } }),
    )
    .await;
    for (id, n) in [(2, &compute), (3, &witness)] {
        post_ok(
            &client,
            format!("http://{}/v1/cluster/add-learner", storage.addr),
            json!({ "node_id": id, "addr": n.addr }),
        )
        .await;
    }
    post_ok(
        &client,
        format!("http://{}/v1/cluster/promote", storage.addr),
        json!({ "voters": [1, 2, 3] }),
    )
    .await;

    // The witness reports its role and rejects data-plane writes.
    let ws: Value = client
        .get(format!("http://{}/v1/stats", witness.addr))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(ws["role"].as_str(), Some("witness"));
    let block_attempt = client
        .post(format!("http://{}/v1/blocks", witness.addr))
        .body("should not land")
        .send()
        .await
        .unwrap();
    assert!(
        !block_attempt.status().is_success(),
        "witness must not accept data-plane blocks"
    );

    // Kill the compute node: storage + witness = 2 of 3 votes, quorum holds.
    compute.kill.send(()).unwrap();

    let deadline = Instant::now() + Duration::from_secs(15);
    let mut recovered = false;
    while Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(300)).await;
        let resp = client
            .post(format!("http://{}/v1/agents", storage.addr))
            .json(&json!({ "agent": "0c".repeat(32), "display_name": "quorum-held" }))
            .send()
            .await;
        if let Ok(r) = resp {
            if r.status().is_success() {
                recovered = true;
                break;
            }
        }
    }
    assert!(
        recovered,
        "storage + witness should keep committing after compute node death"
    );

    // The witness replicated the metadata (it is a real voter)…
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let ws: Value = client
            .get(format!("http://{}/v1/stats", witness.addr))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        if ws["control_plane"]["agents"].as_u64() == Some(1) {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "witness did not apply replicated metadata: {ws}"
        );
        tokio::time::sleep(Duration::from_millis(150)).await;
    }
}
