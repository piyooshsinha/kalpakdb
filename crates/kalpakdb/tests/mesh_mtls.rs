//! Mesh mTLS: node-to-node traffic over mutually-authenticated TLS.
//!
//! Membership addresses point at mesh listeners that demand a client
//! certificate signed by the cluster CA — presenting one IS mesh
//! membership. Proves: a cluster forms and replicates over the mesh; a
//! client without the cluster identity cannot touch the mesh port; and the
//! internal-trust header can no longer be spoofed on the public listener
//! to bypass signature enforcement.

use std::time::{Duration, Instant};

use kalpakdb::pki::write_mesh_pki;
use kalpakdb::server::{serve, MeshOpts, ServeOpts};
use serde_json::{json, Value};

fn spawn_node(
    node_id: u64,
    port: u16,
    mesh_port: u16,
    dir: &std::path::Path,
    pki: &(String, String, String),
) {
    let opts = ServeOpts {
        data_dir: dir
            .join(format!("n{node_id}"))
            .to_string_lossy()
            .into_owned(),
        addr: format!("127.0.0.1:{port}"),
        warm_bytes: 16 * 1024 * 1024,
        node_id,
        bootstrap: false,
        grpc_addr: None,
        compact_secs: 0,
        require_signatures: true,
        tls_cert: None,
        tls_key: None,
        mesh: Some(MeshOpts {
            addr: format!("127.0.0.1:{mesh_port}"),
            ca: pki.0.clone(),
            cert: pki.1.clone(),
            key: pki.2.clone(),
        }),
    };
    tokio::spawn(async move {
        let _ = serve(opts).await;
    });
}

/// A client holding the cluster identity (for cluster-mgmt calls, which
/// live on the mesh listener's router too).
fn mesh_client(pki: &(String, String, String)) -> reqwest::Client {
    let ca = std::fs::read(&pki.0).unwrap();
    let mut id = std::fs::read(&pki.1).unwrap();
    id.extend_from_slice(b"\n");
    id.extend_from_slice(&std::fs::read(&pki.2).unwrap());
    reqwest::Client::builder()
        .add_root_certificate(reqwest::Certificate::from_pem(&ca).unwrap())
        .identity(reqwest::Identity::from_pem(&id).unwrap())
        .timeout(Duration::from_secs(2))
        .build()
        .unwrap()
}

async fn post_ok(client: &reqwest::Client, url: String, body: Value) -> Value {
    let resp = client.post(&url).json(&body).send().await.unwrap();
    let status = resp.status();
    let v: Value = resp.json().await.unwrap();
    assert!(status.is_success(), "POST {url} failed: {v}");
    v
}

#[tokio::test(flavor = "multi_thread")]
async fn cluster_replicates_over_mutual_tls() {
    let dir = tempfile::tempdir().unwrap();
    let pki = write_mesh_pki(
        &dir.path().join("pki").to_string_lossy(),
        &["localhost".to_string(), "127.0.0.1".to_string()],
    )
    .unwrap();

    let (p1, p2) = (17641u16, 17642);
    let (m1, m2) = (17651u16, 17652);
    spawn_node(1, p1, m1, dir.path(), &pki);
    spawn_node(2, p2, m2, dir.path(), &pki);

    let public = reqwest::Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    for p in [p1, p2] {
        while public
            .get(format!("http://127.0.0.1:{p}/v1/stats"))
            .send()
            .await
            .is_err()
        {
            assert!(Instant::now() < deadline, "node on {p} did not start");
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    // 1. The mesh port rejects clients without the cluster identity.
    let trusts_ca_only = reqwest::Client::builder()
        .add_root_certificate(
            reqwest::Certificate::from_pem(&std::fs::read(&pki.0).unwrap()).unwrap(),
        )
        .timeout(Duration::from_secs(2))
        .build()
        .unwrap();
    assert!(
        trusts_ca_only
            .get(format!("https://127.0.0.1:{m1}/v1/stats"))
            .send()
            .await
            .is_err(),
        "mesh must reject connections without a cluster client certificate"
    );

    // 2. Cluster formation and replication run entirely over the mesh.
    let mesh = mesh_client(&pki);
    post_ok(
        &mesh,
        format!("https://127.0.0.1:{m1}/v1/cluster/init"),
        json!({ "members": { "1": format!("127.0.0.1:{m1}") } }),
    )
    .await;
    post_ok(
        &mesh,
        format!("https://127.0.0.1:{m1}/v1/cluster/add-learner"),
        json!({ "node_id": 2, "addr": format!("127.0.0.1:{m2}") }),
    )
    .await;
    post_ok(
        &mesh,
        format!("https://127.0.0.1:{m1}/v1/cluster/promote"),
        json!({ "voters": [1, 2] }),
    )
    .await;

    // A SIGNED write through node 1's public port replicates to node 2
    // (the replication itself crossing the mTLS mesh).
    let signing = ed25519_dalek::SigningKey::from_bytes(&[51; 32]);
    let agent = kalpak_core::AgentId::from_verifying_key(&signing.verifying_key());
    let msg = kalpak_core::signing::register_message(&agent, "mesh-agent");
    let sig = kalpak_core::signing::sign_hex(&signing, &msg);
    post_ok(
        &public,
        format!("http://127.0.0.1:{p1}/v1/agents"),
        json!({ "agent": agent, "display_name": "mesh-agent", "signature": sig }),
    )
    .await;
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let s: Value = public
            .get(format!("http://127.0.0.1:{p2}/v1/stats"))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        if s["control_plane"]["agents"].as_u64() == Some(1) {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "replication over the mTLS mesh did not converge: {s}"
        );
        tokio::time::sleep(Duration::from_millis(150)).await;
    }

    // 3. The spoof is dead: x-kalpak-internal on the PUBLIC port no longer
    // bypasses signature enforcement (it is stripped before handlers).
    let resp = public
        .post(format!("http://127.0.0.1:{p1}/v1/agents"))
        .header("x-kalpak-internal", "1")
        .json(&json!({ "agent": "0f".repeat(32), "display_name": "spoofed" }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status().as_u16(),
        401,
        "internal-header spoofing on the public port must not skip signatures"
    );
}
