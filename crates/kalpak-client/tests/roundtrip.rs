//! SDK integration test: the full agent workflow against an in-process node.

use ed25519_dalek::SigningKey;
use kalpak_client::KalpakClient;
use kalpak_core::{AgentId, CacheKey, ModelFingerprint};
use kalpakdb::server::{serve, ServeOpts};

async fn spawn_node(port: u16, dir: &std::path::Path) -> KalpakClient {
    let addr = format!("127.0.0.1:{port}");
    let opts = ServeOpts {
        data_dir: dir.to_string_lossy().into_owned(),
        addr: addr.clone(),
        warm_bytes: 16 * 1024 * 1024,
        node_id: 1,
        bootstrap: true,
    };
    tokio::spawn(async move {
        let _ = serve(opts).await;
    });
    let client = KalpakClient::new(format!("http://{addr}"));
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while client.stats().await.is_err() {
        assert!(std::time::Instant::now() < deadline, "node did not start");
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    client
}

#[tokio::test(flavor = "multi_thread")]
async fn agent_workflow_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let db = spawn_node(17521, dir.path()).await;

    let agent = AgentId::from_verifying_key(&SigningKey::from_bytes(&[9; 32]).verifying_key());
    db.register_agent(agent, "sdk-agent").await.unwrap();

    // Offload two KV chunks.
    let b0 = db.put_block(b"kv-chunk-zero".to_vec()).await.unwrap();
    let b1 = db.put_block(b"kv-chunk-one".to_vec()).await.unwrap();

    // Chain keys and bind both depths.
    let fp = ModelFingerprint::new("test/model", "tok", "fp16/paged-16");
    let k0 = CacheKey::root(fp, &[10, 20, 30]);
    let k1 = k0.extend(&[40]);
    db.bind_prefix(agent, k0.clone(), vec![b0]).await.unwrap();
    db.bind_prefix(agent, k1.clone(), vec![b0, b1])
        .await
        .unwrap();

    // A later session with the same prefix finds the deepest binding…
    let hit = db
        .lookup(&[k0.clone(), k1.clone()])
        .await
        .unwrap()
        .expect("prefix should be cached");
    assert_eq!(hit.depth, 1);
    assert_eq!(hit.blocks, vec![b0, b1]);

    // …and fetches its blocks back intact.
    assert_eq!(
        db.get_block(&hit.blocks[0]).await.unwrap(),
        b"kv-chunk-zero"
    );
    assert_eq!(db.get_block(&hit.blocks[1]).await.unwrap(), b"kv-chunk-one");

    // A divergent token stream is a clean miss, never a wrong hit.
    let other = CacheKey::root(
        ModelFingerprint::new("test/model", "tok", "fp16/paged-16"),
        &[10, 20, 31],
    );
    assert!(db.lookup(&[other]).await.unwrap().is_none());

    // Errors surface as typed server errors.
    let missing = kalpak_core::BlockId::of(b"never-stored");
    let err = db.get_block(&missing).await.unwrap_err();
    assert!(matches!(
        err,
        kalpak_client::ClientError::Server { status: 404, .. }
    ));
}
