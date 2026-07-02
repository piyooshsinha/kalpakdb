//! Liveness/readiness endpoints for orchestrators: /healthz and /readyz
//! must be present, correct, and — crucially — unauthenticated even when
//! the node demands a read token, since a probe cannot carry credentials.

use std::time::{Duration, Instant};

use kalpakdb::server::{serve, ServeOpts};

fn opts(dir: &std::path::Path, addr: &str, read_token: Option<String>) -> ServeOpts {
    ServeOpts {
        data_dir: dir.to_string_lossy().into_owned(),
        addr: addr.to_string(),
        warm_bytes: 16 * 1024 * 1024,
        node_id: 1,
        bootstrap: true,
        grpc_addr: None,
        compact_secs: 0,
        require_signatures: false,
        max_block_bytes: None,
        read_token,
        tls_cert: None,
        tls_key: None,
        mesh: None,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn health_and_readiness_endpoints() {
    let dir = tempfile::tempdir().unwrap();
    let addr = "127.0.0.1:17681";
    tokio::spawn(async move {
        let _ = serve(opts(
            std::path::Path::new(&dir.path().to_owned()),
            addr,
            None,
        ))
        .await;
    });
    let client = reqwest::Client::new();

    // /healthz is 200 as soon as the process serves HTTP (liveness).
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Ok(r) = client.get(format!("http://{addr}/healthz")).send().await {
            assert_eq!(r.status().as_u16(), 200);
            assert_eq!(r.text().await.unwrap(), "ok");
            break;
        }
        assert!(Instant::now() < deadline, "healthz never came up");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // /readyz becomes 200 once the single node elects itself leader.
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let r = client
            .get(format!("http://{addr}/readyz"))
            .send()
            .await
            .unwrap();
        if r.status().as_u16() == 200 {
            assert_eq!(r.text().await.unwrap(), "ready");
            break;
        }
        // Before election it must be a clean 503, never a 5xx-other or hang.
        assert_eq!(r.status().as_u16(), 503, "readyz should be 503 until ready");
        assert!(Instant::now() < deadline, "node never became ready");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn health_endpoints_bypass_read_token() {
    let dir = tempfile::tempdir().unwrap();
    let addr = "127.0.0.1:17682";
    tokio::spawn(async move {
        let _ = serve(opts(
            std::path::Path::new(&dir.path().to_owned()),
            addr,
            Some("secret".to_string()),
        ))
        .await;
    });
    let client = reqwest::Client::new();
    let deadline = Instant::now() + Duration::from_secs(10);
    while client
        .get(format!("http://{addr}/healthz"))
        .send()
        .await
        .is_err()
    {
        assert!(Instant::now() < deadline, "node did not start");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // A probe carries no token: health endpoints must still answer.
    assert_eq!(
        client
            .get(format!("http://{addr}/healthz"))
            .send()
            .await
            .unwrap()
            .status()
            .as_u16(),
        200
    );
    assert!(client
        .get(format!("http://{addr}/readyz"))
        .send()
        .await
        .unwrap()
        .status()
        .is_success());
    // ...but a token-guarded read is still refused without the token.
    assert_eq!(
        client
            .get(format!("http://{addr}/v1/stats"))
            .send()
            .await
            .unwrap()
            .status()
            .as_u16(),
        401
    );
}
