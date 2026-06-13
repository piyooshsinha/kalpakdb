//! --read-token: observability reads demand a Bearer token; the data path,
//! metrics (Prometheus scrapers), and mutations are unaffected.

use std::time::{Duration, Instant};

use kalpakdb::server::{serve, ServeOpts};

const ADDR: &str = "127.0.0.1:17641";

#[tokio::test(flavor = "multi_thread")]
async fn read_token_guards_observability_only() {
    let dir = tempfile::tempdir().unwrap();
    let opts = ServeOpts {
        data_dir: dir.path().to_string_lossy().into_owned(),
        addr: ADDR.to_string(),
        warm_bytes: 16 * 1024 * 1024,
        node_id: 1,
        bootstrap: true,
        grpc_addr: None,
        compact_secs: 0,
        require_signatures: false,
        max_block_bytes: None,
        read_token: Some("s3cret".to_string()),
        tls_cert: None,
        tls_key: None,
        mesh: None,
    };
    tokio::spawn(async move {
        let _ = serve(opts).await;
    });
    let http = reqwest::Client::new();
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Ok(r) = http.get(format!("http://{ADDR}/metrics")).send().await {
            if r.status().is_success() {
                break;
            }
        }
        assert!(Instant::now() < deadline, "node did not start");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let get = |path: &str, auth: Option<&str>| {
        let mut req = http.get(format!("http://{ADDR}{path}"));
        if let Some(t) = auth {
            req = req.header("authorization", format!("Bearer {t}"));
        }
        req.send()
    };

    // Guarded reads: 401 without/with-wrong, 200 with the token.
    for path in ["/v1/stats", "/v1/agents/list"] {
        assert_eq!(get(path, None).await.unwrap().status().as_u16(), 401);
        assert_eq!(
            get(path, Some("wrong")).await.unwrap().status().as_u16(),
            401
        );
        assert_eq!(
            get(path, Some("s3cret")).await.unwrap().status().as_u16(),
            200
        );
    }

    // The WS upgrade honors ?token= (browsers cannot set headers).
    let ws_no = http
        .get(format!("http://{ADDR}/v1/ws"))
        .header("connection", "upgrade")
        .header("upgrade", "websocket")
        .header("sec-websocket-version", "13")
        .header("sec-websocket-key", "dGhlIHNhbXBsZSBub25jZQ==")
        .send()
        .await
        .unwrap();
    assert_eq!(ws_no.status().as_u16(), 401);
    let ws_ok = http
        .get(format!("http://{ADDR}/v1/ws?token=s3cret"))
        .header("connection", "upgrade")
        .header("upgrade", "websocket")
        .header("sec-websocket-version", "13")
        .header("sec-websocket-key", "dGhlIHNhbXBsZSBub25jZQ==")
        .send()
        .await
        .unwrap();
    assert_eq!(ws_ok.status().as_u16(), 101, "ws with token must upgrade");

    // Unaffected: metrics (scrapers), and the agent data path.
    assert_eq!(get("/metrics", None).await.unwrap().status().as_u16(), 200);
    let put = http
        .post(format!("http://{ADDR}/v1/blocks"))
        .body("open data path")
        .send()
        .await
        .unwrap();
    assert!(put.status().is_success());
}
