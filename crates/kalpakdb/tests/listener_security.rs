//! Unsafe transport configurations must fail before storage is created.

#[test]
fn unsafe_listeners_fail_before_initializing_storage() {
    let temp = tempfile::tempdir().unwrap();
    for (index, flags) in [
        vec!["serve", "--addr", "0.0.0.0:7411"],
        vec!["witness", "--addr", "[::]:7411"],
        vec!["serve", "--addr", "localhost:7411"],
        vec!["serve", "--grpc-addr", "0.0.0.0:7412"],
        vec!["witness", "--grpc-addr", "[::]:7412"],
    ]
    .iter()
    .enumerate()
    {
        let data = temp.path().join(format!("case-{index}"));
        let output = std::process::Command::new(env!("CARGO_BIN_EXE_kalpakdb"))
            .arg(flags[0])
            .arg(&data)
            .args(&flags[1..])
            .output()
            .unwrap();
        assert!(!output.status.success(), "unsafe flags accepted: {flags:?}");
        let error = String::from_utf8_lossy(&output.stderr);
        assert!(error.contains("loopback"), "unexpected failure: {error}");
        assert!(
            !data.exists(),
            "rejected startup created storage: {flags:?}"
        );
    }
}
