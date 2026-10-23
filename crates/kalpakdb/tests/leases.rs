use kalpak_client::KalpakClient;
use kalpak_core::{BlockId, CacheKey, ModelFingerprint};
use serde_json::{json, Value};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

struct Node {
    child: Option<Child>,
    addr: String,
}
impl Node {
    fn stop(&mut self) {
        if let Some(mut child) = self.child.take() {
            child.kill().unwrap();
            child.wait().unwrap();
        }
    }
    fn base(&self) -> String {
        format!("http://{}", self.addr)
    }
}
impl Drop for Node {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

async fn node(dir: &std::path::Path, id: u64) -> Node {
    node_at(dir, id, None).await
}

async fn node_at(dir: &std::path::Path, id: u64, address: Option<String>) -> Node {
    let addr = address.unwrap_or_else(|| {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.local_addr().unwrap().to_string()
    });
    let child = Command::new(env!("CARGO_BIN_EXE_kalpakdb"))
        .arg("serve")
        .arg(dir)
        .args(["--addr", &addr, "--node-id", &id.to_string(), "--join"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let mut node = Node {
        child: Some(child),
        addr,
    };
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if reqwest::get(format!("{}/healthz", node.base()))
            .await
            .is_ok()
        {
            return node;
        }
        assert!(node.child.as_mut().unwrap().try_wait().unwrap().is_none());
        assert!(Instant::now() < deadline, "node did not start");
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn post(client: &reqwest::Client, base: &str, path: &str, body: Value) {
    let response = client
        .post(format!("{base}{path}"))
        .json(&body)
        .send()
        .await
        .unwrap();
    let status = response.status();
    assert!(
        status.is_success(),
        "{path}: {}",
        response.text().await.unwrap()
    );
}

async fn leader(client: &reqwest::Client, nodes: &[Node], exclude: Option<u64>) -> usize {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        for (index, node) in nodes.iter().enumerate().filter(|(_, n)| n.child.is_some()) {
            let stats: Value = client
                .get(format!("{}/v1/stats", node.base()))
                .send()
                .await
                .unwrap()
                .json()
                .await
                .unwrap();
            let id = stats["control_plane"]["node_id"].as_u64().unwrap();
            if stats["control_plane"]["leader"] == id && exclude != Some(id) {
                return index;
            }
        }
        assert!(Instant::now() < deadline, "no replacement leader");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test]
async fn signed_reservation_survives_origin_loss_and_retries_cannot_rebind_old_data() {
    let dir = tempfile::tempdir().unwrap();
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();
    let mut nodes = vec![
        node(&dir.path().join("one"), 1).await,
        node(&dir.path().join("two"), 2).await,
        node(&dir.path().join("three"), 3).await,
    ];
    post(
        &client,
        &nodes[0].base(),
        "/v1/cluster/init",
        json!({"members": {"1": nodes[0].addr}}),
    )
    .await;
    for (index, id) in [(1, 2), (2, 3)] {
        post(
            &client,
            &nodes[0].base(),
            "/v1/cluster/add-learner",
            json!({"node_id": id, "addr": nodes[index].addr}),
        )
        .await;
    }
    post(
        &client,
        &nodes[0].base(),
        "/v1/cluster/promote",
        json!({"voters": [1, 2, 3]}),
    )
    .await;
    let signer = ed25519_dalek::SigningKey::from_bytes(&[96; 32]);
    let db = KalpakClient::with_signer(nodes[1].base(), signer.clone());
    let agent = db.agent_id().unwrap();
    db.register_agent(agent, "leased agent").await.unwrap();
    let block = BlockId::of(b"leased tensor");
    let reservation = BlockId::of(b"unique lease one");
    db.begin_upload_lease(reservation, vec![block])
        .await
        .unwrap(); // follower -> leader
    let cancelled = BlockId::of(b"cancel-after-failover");
    db.begin_upload_lease(cancelled, vec![block]).await.unwrap();
    let key = CacheKey::root(
        ModelFingerprint::new("lease/model", "tokenizer", "layout"),
        &[1],
    );
    assert!(
        db.commit_upload_lease(reservation, key.clone(), vec![block], None, 0)
            .await
            .is_err(),
        "missing payload must not bind"
    );
    for node in &nodes {
        let response = client
            .post(format!("{}/v1/blocks", node.base()))
            .header("x-kalpak-internal", "1")
            .body("leased tensor")
            .send()
            .await
            .unwrap();
        assert!(response.status().is_success());
    }
    let old_leader = leader(&client, &nodes, None).await;
    let old_id = old_leader as u64 + 1;
    nodes[old_leader].stop();
    let current = leader(&client, &nodes, Some(old_id)).await;
    let db = KalpakClient::with_signer(nodes[current].base(), signer);
    db.cancel_upload_lease(cancelled).await.unwrap();
    db.cancel_upload_lease(cancelled).await.unwrap();
    assert!(db.begin_upload_lease(cancelled, vec![block]).await.is_err());
    assert!(db
        .commit_upload_lease(cancelled, key.clone(), vec![block], None, 0)
        .await
        .is_err());
    db.commit_upload_lease(reservation, key.clone(), vec![block], None, 0)
        .await
        .unwrap();
    let block_two = db.put_block(b"newer tensor".to_vec()).await.unwrap();
    let second = BlockId::of(b"unique lease two");
    db.begin_upload_lease(second, vec![block_two])
        .await
        .unwrap();
    assert!(db
        .commit_upload_lease(second, key.clone(), vec![block_two], None, 0)
        .await
        .is_err());
    db.commit_upload_lease(second, key.clone(), vec![block_two], None, 1)
        .await
        .unwrap();
    db.commit_upload_lease(reservation, key.clone(), vec![block], None, 0)
        .await
        .unwrap();
    assert_eq!(
        db.lookup(std::slice::from_ref(&key))
            .await
            .unwrap()
            .unwrap()
            .blocks,
        vec![block_two]
    );
    let listing: Value = client
        .get(format!(
            "{}/v1/agents/{agent}/bindings",
            nodes[current].base()
        ))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(listing["bindings"][0]["lease_revision"], 2);
    let stats: Value = client
        .get(format!("{}/v1/stats", nodes[current].base()))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(stats["replicated_leases"]["committed"], 2);
    assert_eq!(stats["replicated_leases"]["cancelled"], 1);
    assert_eq!(stats["replicated_leases"]["pending"], 0);
    assert_eq!(stats["upload_retention"]["gc_enabled"], false);
    assert_eq!(
        client
            .post(format!("{}/v1/admin/compact", nodes[current].base()))
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::CONFLICT
    );
    // New endpoints never accept the legacy internal header as identity.
    let forged = client
        .post(format!("{}/v1/leases/begin", nodes[current].base()))
        .header("x-kalpak-internal", "1")
        .json(&json!({"agent": agent, "lease": BlockId::of(b"forged"),
            "blocks": [block], "signature": "00".repeat(64)}))
        .send()
        .await
        .unwrap();
    assert_eq!(forged.status(), reqwest::StatusCode::UNAUTHORIZED);
    let wrong_domain = kalpak_core::signing::sign_hex(
        &ed25519_dalek::SigningKey::from_bytes(&[96; 32]),
        &kalpak_core::signing::lease_begin_message(&agent, &cancelled, &[block]),
    );
    let forged = client
        .post(format!("{}/v1/leases/cancel", nodes[current].base()))
        .header("x-kalpak-internal", "1")
        .json(&json!({"agent": agent, "lease": cancelled,
            "signature": wrong_domain}))
        .send()
        .await
        .unwrap();
    assert_eq!(forged.status(), reqwest::StatusCode::UNAUTHORIZED);
    // Kill and reopen the committing node's actual directory, without a
    // snapshot-only simulation. Its peer and recovered log restore quorum.
    let address = nodes[current].addr.clone();
    nodes[current].stop();
    let name = ["one", "two", "three"][current];
    nodes[current] = node_at(&dir.path().join(name), current as u64 + 1, Some(address)).await;
    let current = leader(&client, &nodes, Some(old_id)).await;
    let db = KalpakClient::with_signer(
        nodes[current].base(),
        ed25519_dalek::SigningKey::from_bytes(&[96; 32]),
    );
    db.commit_upload_lease(reservation, key.clone(), vec![block], None, 0)
        .await
        .unwrap();
    db.cancel_upload_lease(cancelled).await.unwrap();
    assert!(db.begin_upload_lease(cancelled, vec![block]).await.is_err());
    assert_eq!(
        db.lookup(&[key]).await.unwrap().unwrap().blocks,
        vec![block_two]
    );
}

#[tokio::test]
async fn concurrent_cancel_and_commit_have_one_winner_without_releasing_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let client = reqwest::Client::new();
    let nodes = vec![node(dir.path(), 1).await];
    post(
        &client,
        &nodes[0].base(),
        "/v1/cluster/init",
        json!({"members": {"1": nodes[0].addr}}),
    )
    .await;
    leader(&client, &nodes, None).await;
    let db = KalpakClient::with_signer(
        nodes[0].base(),
        ed25519_dalek::SigningKey::from_bytes(&[97; 32]),
    );
    let agent = db.agent_id().unwrap();
    db.register_agent(agent, "race").await.unwrap();
    let block = db.put_block(b"guarded race bytes".to_vec()).await.unwrap();
    for n in 0..8u32 {
        let lease = BlockId::of(&n.to_le_bytes());
        let key = CacheKey::root(ModelFingerprint::new("race", "tok", "layout"), &[n]);
        db.begin_upload_lease(lease, vec![block]).await.unwrap();
        let (committed, cancelled) = tokio::join!(
            db.commit_upload_lease(lease, key.clone(), vec![block], None, 0),
            db.cancel_upload_lease(lease)
        );
        match (&committed, &cancelled) {
            (Ok(()), Err(kalpak_client::ClientError::Server { status: 409, .. })) => {
                assert_eq!(
                    db.lookup(std::slice::from_ref(&key))
                        .await
                        .unwrap()
                        .unwrap()
                        .blocks,
                    vec![block]
                );
                assert!(db.cancel_upload_lease(lease).await.is_err());
            }
            (Err(kalpak_client::ClientError::Server { status: 409, .. }), Ok(())) => {
                assert!(db
                    .lookup(std::slice::from_ref(&key))
                    .await
                    .unwrap()
                    .is_none());
                db.cancel_upload_lease(lease).await.unwrap();
                assert!(db
                    .commit_upload_lease(lease, key.clone(), vec![block], None, 0)
                    .await
                    .is_err());
            }
            _ => panic!(
                "expected one terminal winner, got commit={committed:?}, cancel={cancelled:?}"
            ),
        }
    }
    assert_eq!(db.get_block(&block).await.unwrap(), b"guarded race bytes");
    let stats: Value = client
        .get(format!("{}/v1/stats", nodes[0].base()))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(stats["upload_retention"]["guarded_blocks"], 1);
    assert_eq!(stats["replicated_leases"]["total"], 8);
    assert_eq!(stats["replicated_leases"]["pending"], 0);
    assert_eq!(stats["upload_retention"]["gc_enabled"], false);
}
