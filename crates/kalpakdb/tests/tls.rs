//! TLS for the client-facing API: a node serving HTTPS with a self-signed
//! certificate accepts clients that trust the CA, and the full (signed)
//! workflow runs over the encrypted channel.

use std::time::{Duration, Instant};

use ed25519_dalek::SigningKey;
use kalpak_client::KalpakClient;
use kalpak_core::{AgentId, CacheKey, ModelFingerprint};
use kalpakdb::server::{serve, ServeOpts};

const ADDR: &str = "127.0.0.1:17601";

#[tokio::test(flavor = "multi_thread")]
async fn https_serves_the_signed_workflow() {
    let dir = tempfile::tempdir().unwrap();

    // Self-signed cert, exactly what `kalpakdb cert` produces.
    let ck =
        rcgen::generate_simple_self_signed(vec!["localhost".to_string(), "127.0.0.1".to_string()])
            .unwrap();
    let cert_pem = ck.cert.pem();
    let cert_path = dir.path().join("cert.pem");
    let key_path = dir.path().join("key.pem");
    std::fs::write(&cert_path, &cert_pem).unwrap();
    std::fs::write(&key_path, ck.signing_key.serialize_pem()).unwrap();

    let opts = ServeOpts {
        data_dir: dir.path().join("data").to_string_lossy().into_owned(),
        addr: ADDR.to_string(),
        warm_bytes: 16 * 1024 * 1024,
        node_id: 1,
        bootstrap: true,
        grpc_addr: None,
        compact_secs: 0,
        require_signatures: true,
        tls_cert: Some(cert_path.to_string_lossy().into_owned()),
        tls_key: Some(key_path.to_string_lossy().into_owned()),
    };
    tokio::spawn(async move {
        let _ = serve(opts).await;
    });

    // A client that trusts the CA completes the signed workflow over HTTPS.
    let signing = SigningKey::from_bytes(&[31; 32]);
    let agent = AgentId::from_verifying_key(&signing.verifying_key());
    let db = KalpakClient::with_options(
        format!("https://{ADDR}"),
        Some(signing),
        Some(cert_pem.as_bytes()),
    )
    .unwrap();

    let deadline = Instant::now() + Duration::from_secs(10);
    while db.stats().await.is_err() {
        assert!(Instant::now() < deadline, "TLS node did not start");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    db.register_agent(agent, "tls-agent").await.unwrap();
    let id = db.put_block(b"kv-over-tls".to_vec()).await.unwrap();
    let fp = ModelFingerprint::new("test/model", "tok", "fp16/paged-16");
    let k0 = CacheKey::root(fp, &[1]);
    db.bind_prefix(agent, k0.clone(), vec![id]).await.unwrap();
    let hit = db.lookup(&[k0]).await.unwrap().expect("hit over TLS");
    assert_eq!(db.get_block(&hit.blocks[0]).await.unwrap(), b"kv-over-tls");

    // A client that does NOT trust the CA is rejected during the handshake.
    let untrusting = KalpakClient::new(format!("https://{ADDR}"));
    assert!(matches!(
        untrusting.stats().await,
        Err(kalpak_client::ClientError::Transport(_))
    ));

    // Plain HTTP against the TLS port fails outright: no cleartext fallback.
    let plain = KalpakClient::new(format!("http://{ADDR}"));
    assert!(plain.stats().await.is_err());
}
