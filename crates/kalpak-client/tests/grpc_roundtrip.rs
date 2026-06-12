//! SDK gRPC streaming client against an in-process node: large block
//! chunk-streamed in, group-committed, streamed back; plus the two-phase
//! write (blocks via gRPC, bind via HTTP/Raft) end to end.
#![cfg(feature = "grpc")]

use ed25519_dalek::SigningKey;
use kalpak_client::grpc::KalpakGrpcClient;
use kalpak_client::KalpakClient;
use kalpak_core::{AgentId, CacheKey, ModelFingerprint};
use kalpakdb::server::{serve, ServeOpts};

#[tokio::test(flavor = "multi_thread")]
async fn grpc_sdk_two_phase_write() {
    let dir = tempfile::tempdir().unwrap();
    let opts = ServeOpts {
        data_dir: dir.path().to_string_lossy().into_owned(),
        addr: "127.0.0.1:17581".to_string(),
        warm_bytes: 32 * 1024 * 1024,
        node_id: 1,
        bootstrap: true,
        grpc_addr: Some("127.0.0.1:17582".to_string()),
        compact_secs: 0,
        require_signatures: false,
        tls_cert: None,
        tls_key: None,
        mesh: None,
    };
    tokio::spawn(async move {
        let _ = serve(opts).await;
    });

    let http = KalpakClient::new("http://127.0.0.1:17581");
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while http.stats().await.is_err() {
        assert!(std::time::Instant::now() < deadline, "node did not start");
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    let mut grpc = KalpakGrpcClient::connect("http://127.0.0.1:17582")
        .await
        .unwrap();

    // Phase 1: stream the tensors over gRPC — a 3 MiB block exercises
    // multi-chunk streaming both directions.
    let big: Vec<u8> = (0..3 * 1024 * 1024u32).map(|i| (i % 241) as u8).collect();
    let blocks = vec![b"small-chunk".to_vec(), big.clone()];
    let ids = grpc.put_blocks(&blocks).await.unwrap();
    assert_eq!(ids.len(), 2);
    assert_eq!(ids[1], kalpak_core::BlockId::of(&big));
    assert_eq!(grpc.get_block(&ids[1]).await.unwrap(), big);

    // Phase 2: bind the tiny metadata through Raft over HTTP.
    let agent = AgentId::from_verifying_key(&SigningKey::from_bytes(&[8; 32]).verifying_key());
    http.register_agent(agent, "grpc-sdk-agent").await.unwrap();
    let key = CacheKey::root(
        ModelFingerprint::new("test/model", "tok", "fp16/paged-16"),
        &[42, 43],
    );
    http.bind_prefix(agent, key.clone(), ids.clone())
        .await
        .unwrap();

    // The HTTP plane sees blocks streamed via gRPC, and the lookup returns
    // them — the planes share one store.
    let hit = http
        .lookup(std::slice::from_ref(&key))
        .await
        .unwrap()
        .expect("bound prefix must hit");
    assert_eq!(hit.blocks, ids);
    assert_eq!(http.get_block(&ids[0]).await.unwrap(), b"small-chunk");
}
