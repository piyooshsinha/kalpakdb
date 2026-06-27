//! Input-robustness of the HTTP API: malformed or hostile requests must
//! produce clean 4xx responses, never a panic, hang, or runaway allocation.
//!
//! The batch endpoint parses a hand-rolled length-prefixed binary framing
//! with an attacker-controlled block count — historically the riskiest
//! parser on the surface. These tests pin its failure behavior.

use std::time::{Duration, Instant};

use kalpakdb::server::{serve, ServeOpts};

async fn spawn(addr: &str) {
    let dir = tempfile::tempdir().unwrap();
    let opts = ServeOpts {
        data_dir: dir.path().to_string_lossy().into_owned(),
        addr: addr.to_string(),
        warm_bytes: 16 * 1024 * 1024,
        node_id: 1,
        bootstrap: true,
        grpc_addr: None,
        compact_secs: 0,
        require_signatures: false,
        max_block_bytes: None,
        read_token: None,
        tls_cert: None,
        tls_key: None,
        mesh: None,
    };
    // Keep the tempdir alive for the test's lifetime.
    std::mem::forget(dir);
    tokio::spawn(async move {
        let _ = serve(opts).await;
    });
    let client = reqwest::Client::new();
    let deadline = Instant::now() + Duration::from_secs(10);
    while client
        .get(format!("http://{addr}/v1/stats"))
        .send()
        .await
        .is_err()
    {
        assert!(Instant::now() < deadline, "node did not start");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn post_batch(client: &reqwest::Client, addr: &str, body: Vec<u8>) -> reqwest::StatusCode {
    client
        .post(format!("http://{addr}/v1/blocks/batch"))
        .body(body)
        .send()
        .await
        .unwrap()
        .status()
}

#[tokio::test(flavor = "multi_thread")]
async fn batch_endpoint_rejects_malformed_framing_without_crashing() {
    const ADDR: &str = "127.0.0.1:17671";
    spawn(ADDR).await;
    let client = reqwest::Client::new();

    // The DoS case: a 4-byte body claiming u32::MAX blocks. A naive
    // `Vec::with_capacity(count)` would try to reserve tens of GB and abort
    // the process; we must get a clean 400 and a still-alive node.
    let mut huge = Vec::new();
    huge.extend_from_slice(&u32::MAX.to_le_bytes());
    assert_eq!(post_batch(&client, ADDR, huge).await.as_u16(), 400);

    // A large count with no block data behind it: also a clean 400.
    let mut bogus = Vec::new();
    bogus.extend_from_slice(&1_000_000u32.to_le_bytes());
    assert_eq!(post_batch(&client, ADDR, bogus).await.as_u16(), 400);

    // Empty body — below the 4-byte header.
    assert_eq!(post_batch(&client, ADDR, vec![]).await.as_u16(), 400);

    // Count=1 but the declared block length runs past the body.
    let mut overrun = Vec::new();
    overrun.extend_from_slice(&1u32.to_le_bytes()); // count
    overrun.extend_from_slice(&9999u32.to_le_bytes()); // len far past body
    overrun.extend_from_slice(b"short");
    assert_eq!(post_batch(&client, ADDR, overrun).await.as_u16(), 400);

    // Count=2 but only one block's bytes present (truncated mid-stream).
    let mut partial = Vec::new();
    partial.extend_from_slice(&2u32.to_le_bytes());
    partial.extend_from_slice(&3u32.to_le_bytes());
    partial.extend_from_slice(b"abc");
    // second block's length prefix is missing entirely
    assert_eq!(post_batch(&client, ADDR, partial).await.as_u16(), 400);

    // The node is still healthy after all that abuse, and a *well-formed*
    // batch still works — proving we rejected the bad input without
    // wedging the server.
    let mut good = Vec::new();
    good.extend_from_slice(&1u32.to_le_bytes());
    good.extend_from_slice(&5u32.to_le_bytes());
    good.extend_from_slice(b"hello");
    let resp = client
        .post(format!("http://{ADDR}/v1/blocks/batch"))
        .body(good)
        .send()
        .await
        .unwrap();
    assert!(
        resp.status().is_success(),
        "node unhealthy after malformed input"
    );
    let v: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(v["ids"].as_array().unwrap().len(), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn malformed_ids_and_json_are_clean_4xx() {
    const ADDR: &str = "127.0.0.1:17672";
    spawn(ADDR).await;
    let client = reqwest::Client::new();

    // A non-hex / wrong-length block id is a 400, not a 500 or panic.
    let bad_id = client
        .get(format!("http://{ADDR}/v1/blocks/not-a-valid-hex-id"))
        .send()
        .await
        .unwrap();
    assert_eq!(bad_id.status().as_u16(), 400);

    // A well-formed but absent id is a 404.
    let missing = kalpak_core::BlockId::of(b"never stored").to_string();
    let resp = client
        .get(format!("http://{ADDR}/v1/blocks/{missing}"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 404);

    // Garbage JSON to a JSON endpoint is a 4xx, not a 500.
    let bad_json = client
        .post(format!("http://{ADDR}/v1/manifest/lookup"))
        .header("content-type", "application/json")
        .body("{ not valid json ")
        .send()
        .await
        .unwrap();
    assert!(
        bad_json.status().is_client_error(),
        "bad JSON should be 4xx"
    );
}
