use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

struct Process(Option<Child>);
impl Process {
    fn kill(&mut self) {
        let mut child = self.0.take().unwrap();
        child.kill().unwrap();
        child.wait().unwrap();
    }
}
impl Drop for Process {
    fn drop(&mut self) {
        if let Some(mut child) = self.0.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

#[test]
fn offline_confirmation_is_required_and_findings_have_nonzero_exit_status() {
    let dir = tempfile::tempdir().unwrap();
    let store = kalpak_storage::BlockStore::open(dir.path()).unwrap();
    store.put(b"scrub-me").unwrap();
    drop(store);
    let rejected = Command::new(env!("CARGO_BIN_EXE_kalpakdb"))
        .arg("scrub")
        .arg(dir.path())
        .output()
        .unwrap();
    assert!(!rejected.status.success());
    assert!(!dir.path().join("scrub-progress.json").exists());
    let segment = dir.path().join("seg-00000000.klpk");
    let mut bytes = std::fs::read(&segment).unwrap();
    bytes[0] ^= 0xff;
    std::fs::write(&segment, &bytes).unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_kalpakdb"))
        .arg("scrub")
        .arg(dir.path())
        .args(["--offline", "--mib-per-sec", "1024"])
        .output()
        .unwrap();
    assert!(!output.status.success());
    let lines: Vec<serde_json::Value> = String::from_utf8(output.stdout)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(lines[0]["kind"], "invalid_header");
    assert_eq!(lines.last().unwrap()["findings"], 1);
    assert_eq!(std::fs::read(segment).unwrap(), bytes);
}

#[test]
fn scrub_resumes_after_real_process_kill_without_counting_completed_records_twice() {
    let dir = tempfile::tempdir().unwrap();
    let store = kalpak_storage::BlockStore::open_with_roll(dir.path(), 4096).unwrap();
    store.put(b"first").unwrap();
    store.put(&vec![7; 3 * 1024 * 1024 + 1]).unwrap();
    store.put(b"last").unwrap();
    drop(store);
    let child = Command::new(env!("CARGO_BIN_EXE_kalpakdb"))
        .arg("scrub")
        .arg(dir.path())
        .args(["--offline", "--mib-per-sec", "1"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let mut process = Process(Some(child));
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Ok(bytes) = std::fs::read(dir.path().join("scrub-progress.json")) {
            let saved: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
            if saved["progress"]["segment"].as_u64().unwrap() >= 1
                && saved["progress"]["summary"]["complete"] == false
            {
                break;
            }
        }
        assert!(
            process.0.as_mut().unwrap().try_wait().unwrap().is_none(),
            "scrub exited before interruption"
        );
        assert!(
            Instant::now() < deadline,
            "scrub did not checkpoint its first segment"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    process.kill();
    let output = Command::new(env!("CARGO_BIN_EXE_kalpakdb"))
        .arg("scrub")
        .arg(dir.path())
        .args(["--offline", "--mib-per-sec", "1024"])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let summary: serde_json::Value = serde_json::from_str(
        String::from_utf8(output.stdout)
            .unwrap()
            .lines()
            .last()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(summary["records_verified"], 3);
    assert_eq!(summary["findings"], 0);
    assert_eq!(summary["complete"], true);
}
