//! The durability contract under real crashes: a node process is SIGKILLed
//! mid-write-storm, and every write it ACKNOWLEDGED before dying must be
//! present after restart — blocks byte-identical, bindings served. The
//! store must also pass fsck after every kill (torn tails dropped, no
//! corruption admitted into the index).
//!
//! This is `kill -9` on the real binary (`CARGO_BIN_EXE_kalpakdb`), not a
//! simulated tear: the kernel reaps the process wherever it happens to be.

use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use kalpak_client::KalpakClient;
use kalpak_core::{BlockId, CacheKey, ModelFingerprint};

const ADDR: &str = "127.0.0.1:17621";

fn spawn_node(dir: &std::path::Path) -> Child {
    Command::new(env!("CARGO_BIN_EXE_kalpakdb"))
        .args([
            "serve",
            dir.to_str().unwrap(),
            "--addr",
            ADDR,
            "--compact-secs",
            "0",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn kalpakdb")
}

async fn wait_online(db: &KalpakClient) {
    let deadline = Instant::now() + Duration::from_secs(15);
    while db.stats().await.is_err() {
        assert!(Instant::now() < deadline, "node did not come up");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn acknowledged_writes_survive_sigkill() {
    let dir = tempfile::tempdir().unwrap();
    let agent = "0d".repeat(32).parse().unwrap();
    let fp = ModelFingerprint::new("test/model", "tok", "fp16/paged-16");

    // Acknowledged state across all rounds: bind -> its blocks/payloads.
    let mut acked: Vec<(CacheKey, BlockId, Vec<u8>)> = Vec::new();
    let mut registered = false;

    for round in 0..4u32 {
        let mut child = spawn_node(dir.path());
        let db = KalpakClient::new(format!("http://{ADDR}"));
        wait_online(&db).await;
        if !registered {
            db.register_agent(agent, "crash-survivor").await.unwrap();
            registered = true;
        }

        // Write storm: rapid put+bind pairs until the axe falls. Only
        // writes whose BIND was acknowledged join the contract.
        let storm = async {
            let mut i = 0u32;
            loop {
                let payload = format!("round-{round}-payload-{i}")
                    .into_bytes()
                    .repeat(50 + (i as usize % 100));
                let key = CacheKey::root(fp.clone(), &[round, i]);
                let Ok(id) = db.put_block(payload.clone()).await else {
                    break;
                };
                if db.bind_prefix(agent, key.clone(), vec![id]).await.is_ok() {
                    acked.push((key, id, payload));
                }
                i += 1;
            }
        };
        // The axe: SIGKILL after a random-ish slice of the storm.
        let fuse = tokio::time::sleep(Duration::from_millis(150 + (round as u64) * 130));
        tokio::select! {
            _ = storm => {},
            _ = fuse => {}
        }
        child.kill().expect("SIGKILL");
        let _ = child.wait();

        // 1. Offline: the store must fsck clean after a real crash —
        //    a torn tail is dropped, never admitted as a corrupt block.
        let store = kalpak_storage::BlockStore::open(dir.path()).unwrap();
        let report = store.verify_all();
        assert!(
            report.is_clean(),
            "round {round}: store dirty after SIGKILL: {report:?}"
        );
        drop(store);
    }

    assert!(
        acked.len() >= 10,
        "storm too slow to be meaningful: only {} acked writes",
        acked.len()
    );

    // Final restart: every acknowledged bind must be served, byte-identical.
    let mut child = spawn_node(dir.path());
    let db = KalpakClient::new(format!("http://{ADDR}"));
    wait_online(&db).await;
    // State machine replays the durable log; wait for the last bind.
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if let Some(hit) = db
            .lookup(std::slice::from_ref(&acked.last().unwrap().0))
            .await
            .unwrap()
        {
            if hit.blocks == vec![acked.last().unwrap().1] {
                break;
            }
        }
        assert!(
            Instant::now() < deadline,
            "restarted node did not replay the last acknowledged bind"
        );
        tokio::time::sleep(Duration::from_millis(150)).await;
    }

    for (key, id, payload) in &acked {
        let hit = db
            .lookup(std::slice::from_ref(key))
            .await
            .unwrap()
            .unwrap_or_else(|| panic!("acknowledged bind lost after crash: {key:?}"));
        assert_eq!(&hit.blocks, &vec![*id]);
        assert_eq!(&db.get_block(id).await.unwrap(), payload);
    }
    child.kill().ok();
    let _ = child.wait();
    eprintln!(
        "crash test: {} acknowledged writes survived 4 SIGKILLs",
        acked.len()
    );
}
