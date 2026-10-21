//! Real process crashes, not graceful shutdowns. Verify each acknowledged
//! disk copy offline and with every other node absent (no read repair).
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use kalpak_core::BlockId;
use kalpak_proto::v1::block_service_client::BlockServiceClient;
use kalpak_proto::v1::PutBlockChunk;
use serde_json::json;

struct Node {
    child: Option<Child>,
    dir: PathBuf,
    id: u64,
    port: u16,
}

impl Node {
    fn start(&mut self) {
        assert!(self.child.is_none());
        let mut command = Command::new(env!("CARGO_BIN_EXE_kalpakdb"));
        command.arg("serve").arg(&self.dir).args([
            "--addr",
            &format!("127.0.0.1:{}", self.port),
            "--node-id",
            &self.id.to_string(),
            "--join",
            "--warm-mb",
            "1",
        ]);
        if self.id == 1 {
            command.args(["--grpc-addr", &format!("127.0.0.1:{}", self.port + 2)]);
        }
        self.child = Some(
            command
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .unwrap(),
        );
    }

    fn kill(&mut self) {
        if let Some(mut child) = self.child.take() {
            child.kill().expect("kill test node");
            child.wait().expect("reap test node");
        }
    }

    fn url(&self, path: &str) -> String {
        format!("http://127.0.0.1:{}{path}", self.port)
    }

    async fn wait(&mut self, client: &reqwest::Client) {
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            assert!(
                self.child.as_mut().unwrap().try_wait().unwrap().is_none(),
                "node exited during startup"
            );
            if client.get(self.url("/healthz")).send().await.is_ok() {
                return;
            }
            assert!(Instant::now() < deadline, "node startup timed out");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
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

async fn local_copies(client: &reqwest::Client, node: &Node, payloads: &[Vec<u8>]) {
    for payload in payloads {
        let id = BlockId::of(payload);
        let response = client
            .get(node.url(&format!("/v1/blocks/{id}")))
            .header("x-kalpak-internal", "1") // prohibit cascading reads
            .send()
            .await
            .unwrap();
        assert!(response.status().is_success(), "node {} lost {id}", node.id);
        assert_eq!(response.bytes().await.unwrap().as_ref(), payload.as_slice());
    }
}

fn offline_copies(dir: &Path, payloads: &[Vec<u8>]) {
    let store = kalpak_storage::BlockStore::open(dir).unwrap();
    assert!(store.verify_all().is_clean());
    for payload in payloads {
        assert_eq!(
            store.get(&BlockId::of(payload)).unwrap().as_slice(),
            payload.as_slice()
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn acknowledged_http_batch_and_grpc_copies_survive_sigkill_and_isolated_restarts() {
    let temp = tempfile::tempdir().unwrap();
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap();
    let mut first = Node {
        child: None,
        dir: temp.path().join("first"),
        id: 1,
        port: 17781,
    };
    let mut second = Node {
        child: None,
        dir: temp.path().join("second"),
        id: 2,
        port: 17782,
    };
    first.start();
    second.start();
    first.wait(&client).await;
    second.wait(&client).await;
    for (path, body) in [
        (
            "/v1/cluster/init",
            json!({"members": {"1": "127.0.0.1:17781"}}),
        ),
        (
            "/v1/cluster/add-learner",
            json!({"node_id": 2, "addr": "127.0.0.1:17782"}),
        ),
        ("/v1/cluster/promote", json!({"voters": [1, 2]})),
    ] {
        let response = client
            .post(first.url(path))
            .json(&body)
            .send()
            .await
            .unwrap();
        assert!(
            response.status().is_success(),
            "{}",
            response.text().await.unwrap()
        );
    }

    let payloads = vec![
        vec![1; 65_537],
        vec![2; 131_073],
        vec![3; 262_145],
        vec![4; 1_048_577],
    ];
    let response = client
        .post(first.url("/v1/blocks"))
        .header("x-kalpak-replicas", "2")
        .body(payloads[0].clone())
        .send()
        .await
        .unwrap();
    assert!(response.status().is_success());
    let mut batch = 2u32.to_le_bytes().to_vec();
    for payload in &payloads[1..3] {
        batch.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        batch.extend_from_slice(payload);
    }
    let response = client
        .post(first.url("/v1/blocks/batch"))
        .header("x-kalpak-replicas", "2")
        .body(batch)
        .send()
        .await
        .unwrap();
    assert!(response.status().is_success());
    let mut grpc = BlockServiceClient::connect("http://127.0.0.1:17783")
        .await
        .unwrap();
    let mut request = tonic::Request::new(tokio_stream::iter([PutBlockChunk {
        data: payloads[3].clone().into(),
        last_chunk: true,
    }]));
    request
        .metadata_mut()
        .insert("x-kalpak-replicas", "2".parse().unwrap());
    let response = grpc.put_blocks(request).await.unwrap().into_inner();
    assert_eq!(response.ids, vec![BlockId::of(&payloads[3]).to_string()]);

    // Kill the receiving node. Its peer must serve every acknowledged copy
    // with no read-repair dependency and no Raft quorum requirement.
    first.kill();
    local_copies(&client, &second, &payloads).await;
    second.kill();
    for node in [&first, &second] {
        offline_copies(&node.dir, &payloads);
    }

    // Each node starts ALONE: neither its warm cache nor its peer can hide
    // a missing durable copy. Health, not readiness, gates these reads.
    for node in [&mut first, &mut second] {
        node.start();
        node.wait(&client).await;
        local_copies(&client, node, &payloads).await;
        let response = client
            .post(node.url("/v1/blocks"))
            .header("x-kalpak-replicas", "2")
            .body("cannot-ack-two-while-alone")
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 503);
        if node.id == 1 {
            let mut grpc = BlockServiceClient::connect("http://127.0.0.1:17783")
                .await
                .unwrap();
            let mut request = tonic::Request::new(tokio_stream::iter([PutBlockChunk {
                data: b"grpc-peer-unavailable-after-restart".to_vec().into(),
                last_chunk: true,
            }]));
            request
                .metadata_mut()
                .insert("x-kalpak-replicas", "2".parse().unwrap());
            assert_eq!(
                grpc.put_blocks(request).await.unwrap_err().code(),
                tonic::Code::Unavailable
            );
        }
        node.kill();
    }

    // A failed two-copy upload leaves durable repair intent. Kill its
    // origin, bring the empty peer back first, then restart the origin.
    // No client resubmission may be needed to create the missing copy.
    let repair_payload = b"repair-me-after-origin-restart";
    let repair_id = BlockId::of(repair_payload);
    first.start();
    first.wait(&client).await;
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let response = client
            .post(first.url("/v1/blocks"))
            .header("x-kalpak-replicas", "2")
            .body(repair_payload.to_vec())
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 503);
        let local = client
            .get(first.url(&format!("/v1/blocks/{repair_id}")))
            .header("x-kalpak-internal", "1")
            .send()
            .await
            .unwrap();
        if local.status().is_success() {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "failed upload did not persist locally"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    first.kill();
    second.start();
    second.wait(&client).await;
    let absent = client
        .get(second.url(&format!("/v1/blocks/{repair_id}")))
        .header("x-kalpak-internal", "1")
        .send()
        .await
        .unwrap();
    assert_eq!(absent.status(), 404);
    first.start();
    first.wait(&client).await;
    let deadline = Instant::now() + Duration::from_secs(25);
    loop {
        let response = client
            .get(second.url(&format!("/v1/blocks/{repair_id}")))
            .header("x-kalpak-internal", "1")
            .send()
            .await
            .unwrap();
        if response.status().is_success() {
            assert_eq!(response.bytes().await.unwrap().as_ref(), repair_payload);
            break;
        }
        assert!(
            Instant::now() < deadline,
            "persistent repair did not restore peer copy"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    for node in [&first, &second] {
        let response = client
            .post(node.url("/v1/admin/compact"))
            .send()
            .await
            .unwrap();
        assert!(
            !response.status().is_success(),
            "retained replica allowed destructive GC"
        );
        assert!(response.text().await.unwrap().contains("retention"));
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn new_leader_repairs_after_permanent_origin_loss() {
    let temp = tempfile::tempdir().unwrap();
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap();
    let mut origin = Node {
        child: None,
        dir: temp.path().join("origin"),
        id: 1,
        port: 17881,
    };
    let mut retained = Node {
        child: None,
        dir: temp.path().join("retained"),
        id: 2,
        port: 17882,
    };
    let mut replacement = Node {
        child: None,
        dir: temp.path().join("replacement"),
        id: 3,
        port: 17884,
    };
    for node in [&mut origin, &mut retained, &mut replacement] {
        node.start();
        node.wait(&client).await;
    }
    for (path, body) in [
        (
            "/v1/cluster/init",
            json!({"members": {"1": "127.0.0.1:17881"}}),
        ),
        (
            "/v1/cluster/add-learner",
            json!({"node_id": 2, "addr": "127.0.0.1:17882"}),
        ),
        (
            "/v1/cluster/add-learner",
            json!({"node_id": 3, "addr": "127.0.0.1:17884"}),
        ),
        ("/v1/cluster/promote", json!({"voters": [1, 2, 3]})),
    ] {
        let response = client
            .post(origin.url(path))
            .json(&body)
            .send()
            .await
            .unwrap();
        assert!(
            response.status().is_success(),
            "{}",
            response.text().await.unwrap()
        );
    }
    let payload = b"repair-responsibility-outlives-origin";
    let id = BlockId::of(payload);
    // Keep the replacement offline during upload so no background scan can
    // pre-populate it before the failure this test is meant to exercise.
    replacement.kill();
    let response = client
        .post(origin.url("/v1/blocks"))
        .header("x-kalpak-replicas", "2")
        .body(payload.to_vec())
        .send()
        .await
        .unwrap();
    assert!(response.status().is_success());
    assert!(kalpak_storage::BlockStore::open(&replacement.dir)
        .unwrap()
        .get(&id)
        .is_err());
    origin.kill(); // deliberately never restart its process or read its ledger
    replacement.start();
    replacement.wait(&client).await;
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let response = client
            .get(replacement.url(&format!("/v1/blocks/{id}")))
            .header("x-kalpak-internal", "1")
            .send()
            .await
            .unwrap();
        if response.status().is_success() {
            assert_eq!(response.bytes().await.unwrap().as_ref(), payload);
            break;
        }
        assert!(
            Instant::now() < deadline,
            "new leader did not restore replica count"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    local_copies(&client, &retained, &[payload.to_vec()]).await;

    // Reuse node ID and endpoint with a genuinely empty storage directory.
    // The old location must not be treated as proof that bytes survived.
    let old_generation = std::fs::read_to_string(retained.dir.join("storage-generation")).unwrap();
    retained.kill();
    let mut fresh = Node {
        child: None,
        dir: temp.path().join("fresh-storage"),
        id: 2,
        port: 17882,
    };
    fresh.start();
    fresh.wait(&client).await;
    let new_generation = std::fs::read_to_string(fresh.dir.join("storage-generation")).unwrap();
    assert_ne!(old_generation, new_generation);
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let response = client
            .get(fresh.url(&format!("/v1/blocks/{id}")))
            .header("x-kalpak-internal", "1")
            .send()
            .await
            .unwrap();
        if response.status().is_success() {
            assert_eq!(response.bytes().await.unwrap().as_ref(), payload);
            break;
        }
        assert!(
            Instant::now() < deadline,
            "fresh incarnation was mistaken for an existing copy"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Corrupt a cold record while the node still has a correct warm copy.
    // Verification must bypass RAM and append a replacement, not dedup it.
    use std::io::{Seek, Write};
    let segment = fresh.dir.join("seg-00000000.klpk");
    let bytes = std::fs::read(&segment).unwrap();
    let offset = bytes
        .windows(payload.len())
        .position(|window| window == payload)
        .unwrap();
    let old_len = bytes.len() as u64;
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .open(&segment)
        .unwrap();
    file.seek(std::io::SeekFrom::Start(offset as u64)).unwrap();
    file.write_all(&[payload[0] ^ 0xff]).unwrap();
    file.sync_all().unwrap();
    drop(file);
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let response = client
            .get(fresh.url(&format!("/v1/admin/blocks/{id}/presence")))
            .send()
            .await
            .unwrap();
        if response.status().is_success() {
            let value: serde_json::Value = response.json().await.unwrap();
            if value["cold_verified"] == true
                && std::fs::metadata(&segment).unwrap().len() > old_len
            {
                break;
            }
        }
        assert!(
            Instant::now() < deadline,
            "indexed corruption was not replaced from a healthy peer"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    fresh.kill();
    offline_copies(&fresh.dir, &[payload.to_vec()]);
}
