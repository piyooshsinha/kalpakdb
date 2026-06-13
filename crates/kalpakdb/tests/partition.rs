//! Network partition: a node is cut off while STILL ALIVE (unlike the crash
//! tests) — the classic disruptive case, because the isolated node keeps
//! electioneering with rising terms and must not destabilize the majority,
//! and the heal must converge without operator help.
//!
//! Mechanics: cluster membership addresses point at per-node TCP proxies,
//! so all node-to-node traffic crosses them; disabling a proxy (and killing
//! its live connections) is a real partition for inbound traffic to that
//! node while the node itself keeps running.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use kalpakdb::server::{serve, ServeOpts};
use serde_json::{json, Value};

/// A kill-switch TCP forwarder: `enabled=false` refuses new connections and
/// aborts live ones — inbound traffic to the target stops dead.
struct Proxy {
    enabled: Arc<AtomicBool>,
    conns: Arc<Mutex<Vec<tokio::task::JoinHandle<()>>>>,
}

impl Proxy {
    async fn spawn(listen: &str, target: String) -> Self {
        let enabled = Arc::new(AtomicBool::new(true));
        let conns: Arc<Mutex<Vec<tokio::task::JoinHandle<()>>>> = Arc::default();
        let listener = tokio::net::TcpListener::bind(listen).await.unwrap();
        let (en, cs) = (enabled.clone(), conns.clone());
        tokio::spawn(async move {
            loop {
                let Ok((mut inbound, _)) = listener.accept().await else {
                    return;
                };
                if !en.load(Ordering::Relaxed) {
                    continue; // partitioned: drop the connection
                }
                let target = target.clone();
                let handle = tokio::spawn(async move {
                    if let Ok(mut outbound) = tokio::net::TcpStream::connect(&target).await {
                        let _ = tokio::io::copy_bidirectional(&mut inbound, &mut outbound).await;
                    }
                });
                cs.lock().unwrap().push(handle);
            }
        });
        Self { enabled, conns }
    }

    fn set_partitioned(&self, partitioned: bool) {
        self.enabled.store(!partitioned, Ordering::Relaxed);
        if partitioned {
            // Sever in-flight connections too; a partition is not polite.
            for h in self.conns.lock().unwrap().drain(..) {
                h.abort();
            }
        }
    }
}

fn spawn_node(node_id: u64, port: u16, dir: std::path::PathBuf) {
    let opts = ServeOpts {
        data_dir: dir.to_string_lossy().into_owned(),
        addr: format!("127.0.0.1:{port}"),
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
    tokio::spawn(async move {
        let _ = serve(opts).await;
    });
}

async fn post_ok(client: &reqwest::Client, url: String, body: Value) -> Value {
    let resp = client.post(&url).json(&body).send().await.unwrap();
    let status = resp.status();
    let v: Value = resp.json().await.unwrap();
    assert!(status.is_success(), "POST {url} failed: {v}");
    v
}

async fn control(client: &reqwest::Client, addr: &str) -> Value {
    client
        .get(format!("http://{addr}/v1/stats"))
        .send()
        .await
        .unwrap()
        .json::<Value>()
        .await
        .unwrap()["control_plane"]
        .clone()
}

#[tokio::test(flavor = "multi_thread")]
async fn cluster_survives_partition_and_heals() {
    let dir = tempfile::tempdir().unwrap();
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .unwrap();

    // Real node ports and the proxy ports the CLUSTER knows them by.
    let real = [17611u16, 17612, 17613];
    let prox = [17621u16, 17622, 17623];
    let mut proxies = Vec::new();
    for i in 0..3 {
        spawn_node(i as u64 + 1, real[i], dir.path().join(format!("n{i}")));
        proxies.push(
            Proxy::spawn(
                &format!("127.0.0.1:{}", prox[i]),
                format!("127.0.0.1:{}", real[i]),
            )
            .await,
        );
    }
    // Wait for the real API ports (client traffic bypasses the proxies).
    let deadline = Instant::now() + Duration::from_secs(10);
    for p in real {
        while client
            .get(format!("http://127.0.0.1:{p}/v1/stats"))
            .send()
            .await
            .is_err()
        {
            assert!(Instant::now() < deadline, "node on {p} did not start");
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    // Form the cluster THROUGH the proxy addresses.
    let n1 = format!("127.0.0.1:{}", real[0]);
    post_ok(
        &client,
        format!("http://{n1}/v1/cluster/init"),
        json!({ "members": { "1": format!("127.0.0.1:{}", prox[0]) } }),
    )
    .await;
    for (i, port) in prox.iter().enumerate().skip(1) {
        post_ok(
            &client,
            format!("http://{n1}/v1/cluster/add-learner"),
            json!({ "node_id": i + 1, "addr": format!("127.0.0.1:{port}") }),
        )
        .await;
    }
    post_ok(
        &client,
        format!("http://{n1}/v1/cluster/promote"),
        json!({ "voters": [1, 2, 3] }),
    )
    .await;

    post_ok(
        &client,
        format!("http://{n1}/v1/agents"),
        json!({ "agent": "01".repeat(32), "display_name": "pre-partition" }),
    )
    .await;

    // PARTITION node 3: alive, but unreachable by its peers. It will time
    // out, raise its term, and campaign uselessly — the majority must keep
    // committing regardless.
    proxies[2].set_partitioned(true);
    let term_before = control(&client, &n1).await["term"].as_u64().unwrap();

    for i in 2u8..=3 {
        post_ok(
            &client,
            format!("http://{n1}/v1/agents"),
            json!({
                "agent": format!("{i:02x}").repeat(32),
                "display_name": format!("during-partition-{i}")
            }),
        )
        .await;
    }
    // Give the isolated node time to churn elections.
    tokio::time::sleep(Duration::from_secs(2)).await;
    let c1 = control(&client, &n1).await;
    assert_eq!(
        c1["agents"].as_u64(),
        Some(3),
        "majority must keep committing during the partition: {c1}"
    );
    // The isolated node has NOT applied the partition-era writes.
    let c3 = control(&client, &format!("127.0.0.1:{}", real[2])).await;
    assert!(
        c3["agents"].as_u64().unwrap() < 3,
        "an isolated node cannot have committed quorum writes: {c3}"
    );

    // HEAL. The rejoining node (possibly with an inflated term) must not
    // wedge the cluster: everyone converges on one leader and full state.
    proxies[2].set_partitioned(false);
    let deadline = Instant::now() + Duration::from_secs(20);
    'outer: loop {
        let mut leaders = Vec::new();
        for p in real {
            let c = control(&client, &format!("127.0.0.1:{p}")).await;
            if c["agents"].as_u64() != Some(3) {
                assert!(
                    Instant::now() < deadline,
                    "node on {p} did not converge after heal: {c}"
                );
                tokio::time::sleep(Duration::from_millis(250)).await;
                continue 'outer;
            }
            leaders.push(c["leader"].as_u64());
        }
        leaders.dedup();
        assert_eq!(leaders.len(), 1, "split brain after heal: {leaders:?}");
        assert!(leaders[0].is_some(), "no leader after heal");
        break;
    }

    // And the healed cluster still accepts writes.
    post_ok(
        &client,
        format!("http://{n1}/v1/agents"),
        json!({ "agent": "04".repeat(32), "display_name": "post-heal" }),
    )
    .await;
    let term_after = control(&client, &n1).await["term"].as_u64().unwrap();
    assert!(
        term_after >= term_before,
        "terms move monotonically through partitions"
    );
}
