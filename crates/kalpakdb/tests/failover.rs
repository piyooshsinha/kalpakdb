//! Leader failover: kill the leader and verify the cluster elects a new one
//! and keeps accepting writes.
//!
//! Each node runs in its own OS thread with its own tokio runtime, so
//! shutting the runtime down kills everything that node owns — the HTTP
//! server *and* its Raft core (heartbeats stop, which is what triggers the
//! election). Aborting a task on a shared runtime would not be a faithful
//! crash.

use std::time::{Duration, Instant};

use kalpakdb::server::{serve, ServeOpts};
use serde_json::{json, Value};

struct NodeProc {
    addr: String,
    kill: std::sync::mpsc::Sender<()>,
}

fn spawn_node_proc(node_id: u64, port: u16, dir: &std::path::Path) -> NodeProc {
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
        grpc_addr: None,
    };
    let (kill, killed) = std::sync::mpsc::channel::<()>();
    std::thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.spawn(async move {
            let _ = serve(opts).await;
        });
        // Block until the test kills this node (or drops the sender).
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
async fn cluster_survives_leader_failure() {
    let dir = tempfile::tempdir().unwrap();
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .unwrap();

    let n1 = spawn_node_proc(1, 17531, dir.path());
    let n2 = spawn_node_proc(2, 17532, dir.path());
    let n3 = spawn_node_proc(3, 17533, dir.path());
    for n in [&n1, &n2, &n3] {
        wait_online(&client, &n.addr).await;
    }

    // Form the cluster with node 1 as initial leader.
    post_ok(
        &client,
        format!("http://{}/v1/cluster/init", n1.addr),
        json!({ "members": { "1": n1.addr } }),
    )
    .await;
    for (id, n) in [(2, &n2), (3, &n3)] {
        post_ok(
            &client,
            format!("http://{}/v1/cluster/add-learner", n1.addr),
            json!({ "node_id": id, "addr": n.addr }),
        )
        .await;
    }
    post_ok(
        &client,
        format!("http://{}/v1/cluster/promote", n1.addr),
        json!({ "voters": [1, 2, 3] }),
    )
    .await;

    // A write through the original leader lands.
    post_ok(
        &client,
        format!("http://{}/v1/agents", n1.addr),
        json!({ "agent": "0a".repeat(32), "display_name": "pre-failover" }),
    )
    .await;

    // Kill the leader. Heartbeats stop; the survivors hold an election.
    n1.kill.send(()).unwrap();

    // A new leader (2 or 3) must emerge, and writes must succeed again —
    // through either survivor, thanks to leader forwarding.
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut recovered = false;
    while Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(300)).await;
        let resp = client
            .post(format!("http://{}/v1/agents", n2.addr))
            .json(&json!({ "agent": "0b".repeat(32), "display_name": "post-failover" }))
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
        "cluster did not accept writes after leader death"
    );

    // Both survivors converge on the post-failover state and agree on a
    // leader that is not the dead node 1.
    let deadline = Instant::now() + Duration::from_secs(10);
    'outer: loop {
        for n in [&n2, &n3] {
            let s: Value = client
                .get(format!("http://{}/v1/stats", n.addr))
                .send()
                .await
                .unwrap()
                .json()
                .await
                .unwrap();
            let agents = s["control_plane"]["agents"].as_u64().unwrap_or(0);
            let leader = s["control_plane"]["leader"].as_u64();
            if agents != 2 || leader.is_none() || leader == Some(1) {
                assert!(
                    Instant::now() < deadline,
                    "survivor {} did not converge: {s}",
                    n.addr
                );
                tokio::time::sleep(Duration::from_millis(200)).await;
                continue 'outer;
            }
        }
        break;
    }
}
