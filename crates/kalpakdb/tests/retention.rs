use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

struct Node(Child);
impl Drop for Node {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

async fn start(dir: &std::path::Path) -> (Node, String) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    let node = Node(
        Command::new(env!("CARGO_BIN_EXE_kalpakdb"))
            .arg("serve")
            .arg(dir)
            .args(["--addr", &addr.to_string()])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    );
    let base = format!("http://{addr}");
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Ok(response) = reqwest::get(format!("{base}/v1/stats")).await {
            if response.status().is_success() {
                break;
            }
        }
        assert!(Instant::now() < deadline, "node did not start");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    (node, base)
}

#[tokio::test]
async fn unbound_uploads_survive_compaction_requests_and_real_restart() {
    let dir = tempfile::tempdir().unwrap();
    // Seal an existing unguarded block to exercise upgrade safety as well.
    let store = kalpak_storage::BlockStore::open_with_roll(dir.path(), 4096).unwrap();
    let legacy = store.put(b"legacy-unbound").unwrap();
    store.put(b"active-tail").unwrap();
    drop(store);
    let client = reqwest::Client::new();
    let (node, base) = start(dir.path()).await;
    let upload = client
        .post(format!("{base}/v1/blocks"))
        .body("unbound-new")
        .send();
    let compact = client.post(format!("{base}/v1/admin/compact")).send();
    let (upload, compact) = tokio::join!(upload, compact);
    assert!(upload.unwrap().status().is_success());
    assert_eq!(compact.unwrap().status(), reqwest::StatusCode::CONFLICT);
    let mut batch = 2u32.to_le_bytes().to_vec();
    for payload in [b"batch-a".as_slice(), b"batch-b".as_slice()] {
        batch.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        batch.extend_from_slice(payload);
    }
    // The preceding public upload can still own admission while its
    // background fan-out task finishes. Retry only documented backpressure;
    // surface journal/storage errors with their response body.
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let response = client
            .post(format!("{base}/v1/blocks/batch"))
            .body(batch.clone())
            .send()
            .await
            .unwrap();
        let status = response.status();
        if status.is_success() {
            break;
        }
        let body = response.text().await.unwrap();
        assert!(
            status == reqwest::StatusCode::SERVICE_UNAVAILABLE && Instant::now() < deadline,
            "batch upload failed: {status}: {body}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    drop(node); // SIGKILL; no orderly application shutdown
    let (_node, base) = start(dir.path()).await;
    let stats: serde_json::Value = client
        .get(format!("{base}/v1/stats"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(stats["upload_retention"]["guarded_blocks"], 3);
    assert_eq!(stats["upload_retention"]["gc_enabled"], false);
    assert_eq!(stats["upload_retention"]["write_failed"], false);
    for (id, guarded, present) in [
        (legacy, false, true),
        (kalpak_core::BlockId::of(b"unbound-new"), true, true),
        (kalpak_core::BlockId::of(b"absent"), false, false),
    ] {
        let plan: serde_json::Value = client
            .get(format!("{base}/v1/reclamation/{id}"))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(plan["authoritative"], false);
        assert_eq!(plan["reclaimable"], false);
        assert_eq!(plan["gc_enabled"], false);
        assert_eq!(plan["local_guard"]["guarded"], guarded);
        assert_eq!(plan["local_payload_present"], present);
        assert_eq!(plan["metadata"]["binding_references"], 0);
        assert_eq!(plan["metadata"]["leases"]["pending"], 0);
    }
    assert_eq!(
        client
            .get(format!("{base}/v1/reclamation/invalid"))
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::BAD_REQUEST
    );
    for id in [
        legacy,
        kalpak_core::BlockId::of(b"unbound-new"),
        kalpak_core::BlockId::of(b"batch-a"),
        kalpak_core::BlockId::of(b"batch-b"),
    ] {
        assert!(client
            .get(format!("{base}/v1/blocks/{id}"))
            .send()
            .await
            .unwrap()
            .status()
            .is_success());
    }
    assert_eq!(
        client
            .post(format!("{base}/v1/admin/compact"))
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::CONFLICT
    );
    // Binding is deliberately delayed until after the process restart.
    let db = kalpak_client::KalpakClient::new(&base);
    let agent = "0b".repeat(32).parse().unwrap();
    db.register_agent(agent, "delayed binder").await.unwrap();
    let key = kalpak_core::CacheKey::root(
        kalpak_core::ModelFingerprint::new("retention/model", "tok", "layout"),
        &[1, 2],
    );
    db.bind_prefix(
        agent,
        key,
        vec![legacy, kalpak_core::BlockId::of(b"unbound-new")],
    )
    .await
    .unwrap();
    let stats: serde_json::Value = client
        .get(format!("{base}/v1/stats"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        stats["upload_retention"]["guarded_blocks"], 3,
        "a binding must not release local guards without a coordinated watermark"
    );
}

#[test]
fn automatic_compaction_is_rejected_before_initializing_storage() {
    let dir = tempfile::tempdir().unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_kalpakdb"))
        .arg("serve")
        .arg(dir.path())
        .args(["--compact-secs", "1"])
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("coordinated GC watermark"));
    assert!(!dir.path().join("seg-00000000.klpk").exists());
}
