//! One-step-ahead speculative prefetch: a lookup that hits a prefix warms
//! the blocks of that prefix's CHILDREN in the prefix tree, so the agent's
//! next, deeper request is already in RAM.

use std::time::{Duration, Instant};

use kalpak_client::KalpakClient;
use kalpak_core::{AgentId, CacheKey, ModelFingerprint};
use kalpakdb::server::{serve, ServeOpts};

async fn warm_stats(db: &KalpakClient) -> (u64, u64) {
    let s = db.stats().await.unwrap();
    (
        s["data_plane"]["hits"].as_u64().unwrap(),
        s["data_plane"]["misses"].as_u64().unwrap(),
    )
}

#[tokio::test(flavor = "multi_thread")]
async fn lookup_prefetches_child_blocks() {
    let dir = tempfile::tempdir().unwrap();
    let opts = ServeOpts {
        data_dir: dir.path().to_string_lossy().into_owned(),
        addr: "127.0.0.1:17571".to_string(),
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
    tokio::spawn(async move {
        let _ = serve(opts).await;
    });
    let db = KalpakClient::new("http://127.0.0.1:17571");
    let deadline = Instant::now() + Duration::from_secs(10);
    while db.stats().await.is_err() {
        assert!(Instant::now() < deadline, "node did not start");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let agent = AgentId::from_verifying_key(
        &ed25519_dalek::SigningKey::from_bytes(&[3; 32]).verifying_key(),
    );
    db.register_agent(agent, "lookahead").await.unwrap();

    let fp = ModelFingerprint::new("test/model", "tok", "fp16/paged-16");
    let k0 = CacheKey::root(fp, &[1, 2, 3]);
    let k1 = k0.extend(&[4, 5]);

    let b0 = db.put_block(b"parent-kv".to_vec()).await.unwrap();
    let b1 = db.put_block(b"child-kv".to_vec()).await.unwrap();
    db.bind_prefix(agent, k0.clone(), vec![b0]).await.unwrap();
    db.bind_prefix_under(agent, k0.clone(), k1.clone(), vec![b0, b1])
        .await
        .unwrap();

    // Restart-equivalent: evict everything from warm by reopening the node?
    // Simpler: the blocks ARE warm from the puts. To observe the lookahead
    // we count warm hits: lookup [k0] (hit at depth 0) triggers background
    // prefetch of k0's blocks AND k1's blocks. Wait for those warm touches,
    // then a get of the CHILD's block must be a warm hit with no new miss.
    let (h_before, m_before) = warm_stats(&db).await;
    let hit = db
        .lookup(std::slice::from_ref(&k0))
        .await
        .unwrap()
        .expect("hit");
    assert_eq!(hit.depth, 0);

    // The background prefetch touches b0 (hit set) + b0,b1 (child set).
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let (h, _) = warm_stats(&db).await;
        if h >= h_before + 3 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "lookahead prefetch did not touch the child's blocks"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Fetching the child's block now is a warm hit; misses never moved.
    assert_eq!(db.get_block(&b1).await.unwrap(), b"child-kv");
    let (_, m_after) = warm_stats(&db).await;
    assert_eq!(m_after, m_before, "child fetch should never touch disk");
}
