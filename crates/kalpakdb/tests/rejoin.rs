//! Restart-and-rejoin: a node dies, the cluster moves on, the node comes
//! back from its durable Raft log under the same identity and catches up on
//! everything it missed — no re-init, no operator intervention.

use std::time::{Duration, Instant};

use kalpakdb::server::{serve, ServeOpts};
use serde_json::{json, Value};

struct NodeProc {
    addr: String,
    kill: std::sync::mpsc::Sender<()>,
}

fn spawn(node_id: u64, port: u16, dir: std::path::PathBuf) -> NodeProc {
    let addr = format!("127.0.0.1:{port}");
    let opts = ServeOpts {
        data_dir: dir.to_string_lossy().into_owned(),
        addr: addr.clone(),
        warm_bytes: 16 * 1024 * 1024,
        node_id,
        bootstrap: false,
        grpc_addr: None,
        compact_secs: 0,
        require_signatures: false,
        read_token: None,
        tls_cert: None,
        tls_key: None,
        mesh: None,
    };
    let (kill, killed) = std::sync::mpsc::channel::<()>();
    std::thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.spawn(async move {
            let _ = serve(opts).await;
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

async fn agent_count(client: &reqwest::Client, addr: &str) -> u64 {
    let s: Value = client
        .get(format!("http://{addr}/v1/stats"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    s["control_plane"]["agents"].as_u64().unwrap_or(0)
}

#[tokio::test(flavor = "multi_thread")]
async fn restarted_node_rejoins_and_catches_up() {
    let dir = tempfile::tempdir().unwrap();
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .unwrap();

    let n1 = spawn(1, 17561, dir.path().join("n1"));
    let n2 = spawn(2, 17562, dir.path().join("n2"));
    let n3 = spawn(3, 17563, dir.path().join("n3"));
    for n in [&n1, &n2, &n3] {
        wait_online(&client, &n.addr).await;
    }

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

    // One write while everyone is up.
    post_ok(
        &client,
        format!("http://{}/v1/agents", n1.addr),
        json!({ "agent": "01".repeat(32), "display_name": "before-crash" }),
    )
    .await;

    // Node 3 crashes. The cluster keeps committing without it.
    n3.kill.send(()).unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    for i in 2u8..=4 {
        post_ok(
            &client,
            format!("http://{}/v1/agents", n1.addr),
            json!({
                "agent": format!("{:02x}", i).repeat(32),
                "display_name": format!("while-down-{i}")
            }),
        )
        .await;
    }
    assert_eq!(agent_count(&client, &n1.addr).await, 4);

    // Node 3 restarts from its own durable state: same id, same data dir,
    // no bootstrap, no cluster-management calls.
    let n3 = spawn(3, 17563, dir.path().join("n3"));
    wait_online(&client, &n3.addr).await;

    // It must catch up on the three writes it missed.
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if agent_count(&client, &n3.addr).await == 4 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "rejoined node did not catch up with the cluster"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    // And it serves replicated metadata: a lookup-by-stats cross-check on
    // a write that happened entirely while it was down.
    let writes_after: u64 = 1;
    post_ok(
        &client,
        format!("http://{}/v1/agents", n2.addr),
        json!({ "agent": "05".repeat(32), "display_name": "after-rejoin" }),
    )
    .await;
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if agent_count(&client, &n3.addr).await == 4 + writes_after {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "rejoined node stopped replicating new writes"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}
