//! Signed writes: a node running `--require-signatures` rejects unsigned or
//! forged metadata mutations and accepts properly signed ones — making
//! "state mutations are signed and attributable" literal.

use std::time::{Duration, Instant};

use ed25519_dalek::SigningKey;
use kalpak_client::KalpakClient;
use kalpak_core::{AgentId, CacheKey, ModelFingerprint};
use kalpakdb::server::{serve, ServeOpts};
use serde_json::json;

const ADDR: &str = "127.0.0.1:17591";

async fn spawn_node(dir: &std::path::Path) {
    let opts = ServeOpts {
        data_dir: dir.to_string_lossy().into_owned(),
        addr: ADDR.to_string(),
        warm_bytes: 16 * 1024 * 1024,
        node_id: 1,
        bootstrap: true,
        grpc_addr: None,
        compact_secs: 0,
        require_signatures: true,
        tls_cert: None,
        tls_key: None,
    };
    tokio::spawn(async move {
        let _ = serve(opts).await;
    });
    let client = reqwest::Client::new();
    let deadline = Instant::now() + Duration::from_secs(10);
    while client
        .get(format!("http://{ADDR}/v1/stats"))
        .send()
        .await
        .is_err()
    {
        assert!(Instant::now() < deadline, "node did not start");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn signed_writes_are_enforced() {
    let dir = tempfile::tempdir().unwrap();
    spawn_node(dir.path()).await;
    let http = reqwest::Client::new();

    let signing = SigningKey::from_bytes(&[21; 32]);
    let agent = AgentId::from_verifying_key(&signing.verifying_key());

    // 1. Unsigned register is rejected with 401.
    let resp = http
        .post(format!("http://{ADDR}/v1/agents"))
        .json(&json!({ "agent": agent, "display_name": "no-sig" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 401, "unsigned register must fail");

    // 2. A signature from a DIFFERENT key over the same message is rejected.
    let msg = kalpak_core::signing::register_message(&agent, "forged");
    let intruder = SigningKey::from_bytes(&[99; 32]);
    let forged = kalpak_core::signing::sign_hex(&intruder, &msg);
    let resp = http
        .post(format!("http://{ADDR}/v1/agents"))
        .json(&json!({ "agent": agent, "display_name": "forged", "signature": forged }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 401, "forged register must fail");

    // 3. The SDK with the agent's own key succeeds across the whole
    // workflow: register, bind, chain-bind.
    let db = KalpakClient::with_signer(format!("http://{ADDR}"), signing.clone());
    assert_eq!(db.agent_id(), Some(agent));
    db.register_agent(agent, "signed-agent").await.unwrap();

    let b0 = db.put_block(b"kv-signed-0".to_vec()).await.unwrap();
    let b1 = db.put_block(b"kv-signed-1".to_vec()).await.unwrap();
    let fp = ModelFingerprint::new("test/model", "tok", "fp16/paged-16");
    let k0 = CacheKey::root(fp, &[1, 2]);
    let k1 = k0.extend(&[3]);
    let k2 = k1.extend(&[4]);

    db.bind_prefix(agent, k0.clone(), vec![b0]).await.unwrap();
    db.bind_chain(
        agent,
        vec![
            (k1.clone(), vec![b0, b1], Some(k0.clone())),
            (k2.clone(), vec![b0, b1], Some(k1.clone())),
        ],
    )
    .await
    .unwrap();

    let hit = db.lookup(&[k0, k1, k2]).await.unwrap().expect("hit");
    assert_eq!(hit.depth, 2);

    // 4. A signature over DIFFERENT content does not authorize this bind
    // (valid signature, wrong message).
    let other_msg = kalpak_core::signing::register_message(&agent, "something else");
    let mismatched = kalpak_core::signing::sign_hex(&signing, &other_msg);
    let resp = http
        .post(format!("http://{ADDR}/v1/manifest/bind"))
        .json(&json!({
            "agent": agent,
            "key": CacheKey::root(
                ModelFingerprint::new("test/model", "tok", "fp16/paged-16"),
                &[9, 9],
            ),
            "blocks": [b0],
            "signature": mismatched,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 401, "wrong-message sig must fail");

    // 5. An unsigned client against this node gets a clear 401, not a hang.
    let unsigned = KalpakClient::new(format!("http://{ADDR}"));
    let err = unsigned.register_agent(agent, "nope").await.unwrap_err();
    assert!(matches!(
        err,
        kalpak_client::ClientError::Server { status: 401, .. }
    ));

    // 6. Reads never require signatures: stats and lookups stay open.
    assert!(unsigned.stats().await.is_ok());
}
