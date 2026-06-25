use std::process::Command;

#[test]
fn bench_should_reject_zero_runs_before_network_work() {
    let output = Command::new(env!("CARGO_BIN_EXE_fcl"))
        .args(["bench", "https://example.com/repo.git", "--runs", "0"])
        .output()
        .expect("fcl binary should run");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("--runs must be greater than 0"));
}

#[test]
fn clone_should_reject_non_https_urls() {
    let target = tempfile::tempdir().expect("temporary directory should be created");
    let target = target.path().join("repo");
    let output = Command::new(env!("CARGO_BIN_EXE_fcl"))
        .arg("http://example.com/repo.git")
        .arg(&target)
        .output()
        .expect("fcl binary should run");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("unsupported URL scheme"));
}

#[test]
fn clone_should_accept_no_pipeline_flag_before_url_validation() {
    let target = tempfile::tempdir().expect("temporary directory should be created");
    let target = target.path().join("repo");
    let output = Command::new(env!("CARGO_BIN_EXE_fcl"))
        .arg("--no-pipeline")
        .arg("http://example.com/repo.git")
        .arg(&target)
        .output()
        .expect("fcl binary should run");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("unsupported URL scheme"));
}

#[test]
fn bench_pack_should_reject_missing_pack_path() {
    let target = tempfile::tempdir().expect("temporary directory should be created");
    let pack = target.path().join("missing.pack");
    let output = Command::new(env!("CARGO_BIN_EXE_fcl"))
        .arg("bench")
        .arg("--pack")
        .arg(&pack)
        .output()
        .expect("fcl binary should run");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("does not exist"));
}

#[test]
fn bench_pack_should_reject_clone_git_comparison_flag() {
    let target = tempfile::tempdir().expect("temporary directory should be created");
    let pack = target.path().join("pack.pack");
    std::fs::write(&pack, []).expect("pack placeholder should be written");
    let output = Command::new(env!("CARGO_BIN_EXE_fcl"))
        .arg("bench")
        .arg("--pack")
        .arg(&pack)
        .arg("--compare-git")
        .output()
        .expect("fcl binary should run");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("use --compare-index-pack"));
}

#[test]
fn bench_pack_should_reject_zero_git_pack_threads() {
    let target = tempfile::tempdir().expect("temporary directory should be created");
    let pack = target.path().join("pack.pack");
    std::fs::write(&pack, []).expect("pack placeholder should be written");
    let output = Command::new(env!("CARGO_BIN_EXE_fcl"))
        .arg("bench")
        .arg("--pack")
        .arg(&pack)
        .arg("--compare-index-pack")
        .arg("--git-pack-threads")
        .arg("0")
        .output()
        .expect("fcl binary should run");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("--git-pack-threads values must be greater than 0"));
}
