//! HTTP body-size limits: block-ingest routes must accept real KV blocks
//! (far larger than axum's 2 MiB default, which would silently 413 them),
//! while metadata/JSON routes stay bounded so a raised block limit doesn't
//! widen the JSON buffering surface. A block over the configured ceiling is
//! rejected, not buffered.

use std::time::{Duration, Instant};

use kalpakdb::server::{serve, ServeOpts};

async fn spawn_cap(addr: &str, max_block_bytes: usize) {
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
        max_block_bytes: Some(max_block_bytes),
        read_token: None,
        tls_cert: None,
        tls_key: None,
        mesh: None,
    };
    std::mem::forget(dir); // keep the dir for the node's lifetime
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

#[tokio::test(flavor = "multi_thread")]
async fn block_ingest_accepts_large_blocks_but_bounds_metadata() {
    const ADDR: &str = "127.0.0.1:17673";
    spawn_cap(ADDR, 8 * 1024 * 1024).await; // 8 MiB block ceiling
    let client = reqwest::Client::new();

    // A 4 MiB block — over axum's 2 MiB default — is accepted.
    let resp = client
        .post(format!("http://{ADDR}/v1/blocks"))
        .body(vec![7u8; 4 * 1024 * 1024])
        .send()
        .await
        .unwrap();
    assert!(
        resp.status().is_success(),
        "4 MiB block must be accepted, got {}",
        resp.status()
    );

    // A block over the configured ceiling is refused. A body-limit
    // rejection can surface either as a 413 response or, if the server
    // resets the connection mid-upload, as a transport error — both prove
    // the oversized body was not accepted.
    let refused = client
        .post(format!("http://{ADDR}/v1/blocks"))
        .body(vec![0u8; 9 * 1024 * 1024])
        .send()
        .await;
    assert!(
        refused.is_err() || refused.as_ref().unwrap().status().as_u16() == 413,
        "block over max_block_bytes must be refused (413 or reset), got {refused:?}"
    );

    // Metadata routes keep the small default limit, so the raised block
    // limit doesn't widen the JSON buffering surface. (Same 413-or-reset
    // tolerance as above.)
    let refused = client
        .post(format!("http://{ADDR}/v1/manifest/lookup"))
        .header("content-type", "application/json")
        .body(vec![b' '; 4 * 1024 * 1024])
        .send()
        .await;
    assert!(
        refused.is_err() || refused.as_ref().unwrap().status().as_u16() == 413,
        "oversized metadata body must be refused (413 or reset), got {refused:?}"
    );
}
