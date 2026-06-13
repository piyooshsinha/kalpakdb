//! Online backup / restore: a crash-consistent tar streamed from a LIVE
//! node restores into a working replacement node with all state intact.
//!
//! The correctness argument under live writes: the control plane is
//! archived before the segments, and a bind only ever references blocks
//! that were durable before it (the two-phase write) — so every binding in
//! the backup has its blocks in the backup. Torn tails from racing writes
//! are dropped by the normal recovery paths on open.

use std::time::{Duration, Instant};

use kalpak_client::KalpakClient;
use kalpak_core::{CacheKey, ModelFingerprint};
use kalpakdb::server::{serve, ServeOpts};

fn opts(dir: &std::path::Path, addr: &str) -> ServeOpts {
    ServeOpts {
        data_dir: dir.to_string_lossy().into_owned(),
        addr: addr.to_string(),
        warm_bytes: 16 * 1024 * 1024,
        node_id: 1,
        bootstrap: true,
        grpc_addr: None,
        compact_secs: 0,
        require_signatures: false,
        read_token: None,
        tls_cert: None,
        tls_key: None,
        mesh: None,
    }
}

async fn wait_online(db: &KalpakClient) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while db.stats().await.is_err() {
        assert!(Instant::now() < deadline, "node did not start");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn online_backup_restores_into_a_working_node() {
    let src = tempfile::tempdir().unwrap();
    let dst = tempfile::tempdir().unwrap();

    let o = opts(src.path(), "127.0.0.1:17611");
    tokio::spawn(async move {
        let _ = serve(o).await;
    });
    let db = KalpakClient::new("http://127.0.0.1:17611");
    wait_online(&db).await;

    // State worth saving: an agent, blocks, a bound chain.
    let agent = "0a".repeat(32).parse().unwrap();
    db.register_agent(agent, "survivor").await.unwrap();
    let ids = db
        .put_blocks(&[b"kv-backup-0".to_vec(), b"kv-backup-1".to_vec()])
        .await
        .unwrap();
    let fp = ModelFingerprint::new("test/model", "tok", "fp16/paged-16");
    let k0 = CacheKey::root(fp, &[1, 2]);
    let k1 = k0.extend(&[3]);
    db.bind_chain(
        agent,
        vec![
            (k0.clone(), vec![ids[0]], None),
            (k1.clone(), ids.clone(), Some(k0.clone())),
        ],
    )
    .await
    .unwrap();

    // Online backup from the live node.
    let tar_bytes = reqwest::get("http://127.0.0.1:17611/v1/admin/backup")
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert!(tar_bytes.len() > 1024, "tar should contain real data");

    // Restore into a fresh directory.
    let restored = dst.path().join("restored");
    std::fs::create_dir_all(&restored).unwrap();
    tar::Archive::new(tar_bytes.as_ref())
        .unpack(&restored)
        .unwrap();

    // The restored store passes fsck...
    let store = kalpak_storage::BlockStore::open(&restored).unwrap();
    let report = store.verify_all();
    assert!(report.is_clean(), "restored store must verify: {report:?}");
    assert_eq!(report.blocks_checked, 2);
    drop(store);

    // ...and a node served from it has the full state.
    let o = opts(&restored, "127.0.0.1:17612");
    tokio::spawn(async move {
        let _ = serve(o).await;
    });
    let db2 = KalpakClient::new("http://127.0.0.1:17612");
    wait_online(&db2).await;

    // Raft state machine recovered from the archived log: poll until applied.
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let s = db2.stats().await.unwrap();
        if s["control_plane"]["agents"].as_u64() == Some(1)
            && s["control_plane"]["bindings"].as_u64() == Some(2)
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "restored node did not recover state: {s}"
        );
        tokio::time::sleep(Duration::from_millis(150)).await;
    }

    let hit = db2
        .lookup(&[k0, k1])
        .await
        .unwrap()
        .expect("restored node must serve the bound chain");
    assert_eq!(hit.depth, 1);
    assert_eq!(db2.get_block(&hit.blocks[0]).await.unwrap(), b"kv-backup-0");
    assert_eq!(db2.get_block(&hit.blocks[1]).await.unwrap(), b"kv-backup-1");
}
