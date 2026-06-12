//! Kalpak node CLI.
//!
//!   kalpakdb serve <data-dir> [--addr 127.0.0.1:7411] [--warm-mb 256] [--node-id 1] [--join] [--compact-secs 3600]
//!   kalpakdb witness <data-dir> [--addr ...] [--node-id N]   (consensus-only voter)
//!   kalpakdb bench <data-dir> [--blocks 2000] [--size-kb 64]
//!   kalpakdb stress <base-url> [--agents 8] [--secs 10] [--chunk-kb 64]
//!   kalpakdb put <data-dir>            reads payload from stdin, prints id
//!   kalpakdb get <data-dir> <block-id> writes payload to stdout
//!   kalpakdb stat <data-dir>           prints store statistics

use kalpakdb::server;

use std::io::{Read, Write};
use std::process::ExitCode;

use kalpak_core::BlockId;
use kalpak_storage::BlockStore;

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("kalpakdb: {e}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.as_slice() {
        [cmd, dir, rest @ ..] if cmd == "serve" || cmd == "witness" => {
            let mut opts = server::ServeOpts {
                data_dir: dir.clone(),
                addr: "127.0.0.1:7411".to_string(),
                warm_bytes: 256 * 1024 * 1024,
                node_id: 1,
                bootstrap: cmd == "serve",
                grpc_addr: None,
                require_signatures: false,
                tls_cert: None,
                tls_key: None,
                compact_secs: 3600,
            };
            let mut it = rest.iter();
            while let Some(flag) = it.next() {
                match flag.as_str() {
                    "--addr" => opts.addr = it.next().ok_or("--addr needs a value")?.clone(),
                    "--warm-mb" => {
                        let mb: u64 = it.next().ok_or("--warm-mb needs a value")?.parse()?;
                        opts.warm_bytes = mb * 1024 * 1024;
                    }
                    "--node-id" => {
                        opts.node_id = it.next().ok_or("--node-id needs a value")?.parse()?
                    }
                    "--join" => opts.bootstrap = false,
                    "--require-signatures" => opts.require_signatures = true,
                    "--tls-cert" => {
                        opts.tls_cert = Some(it.next().ok_or("--tls-cert needs a value")?.clone())
                    }
                    "--tls-key" => {
                        opts.tls_key = Some(it.next().ok_or("--tls-key needs a value")?.clone())
                    }
                    "--grpc-addr" => {
                        opts.grpc_addr = Some(it.next().ok_or("--grpc-addr needs a value")?.clone())
                    }
                    "--compact-secs" => {
                        opts.compact_secs =
                            it.next().ok_or("--compact-secs needs a value")?.parse()?
                    }
                    other => return Err(format!("unknown flag: {other}").into()),
                }
            }
            let rt = tokio::runtime::Runtime::new()?;
            if cmd == "witness" {
                rt.block_on(server::serve_witness(opts))
            } else {
                rt.block_on(server::serve(opts))
            }
        }
        [cmd, url, rest @ ..] if cmd == "stress" => {
            let mut opts = kalpakdb::stress::StressOpts {
                base: url.trim_end_matches('/').to_string(),
                agents: 8,
                secs: 10,
                chunk_kb: 64,
            };
            let mut it = rest.iter();
            while let Some(flag) = it.next() {
                match flag.as_str() {
                    "--agents" => {
                        opts.agents = it.next().ok_or("--agents needs a value")?.parse()?
                    }
                    "--secs" => opts.secs = it.next().ok_or("--secs needs a value")?.parse()?,
                    "--chunk-kb" => {
                        opts.chunk_kb = it.next().ok_or("--chunk-kb needs a value")?.parse()?
                    }
                    other => return Err(format!("unknown flag: {other}").into()),
                }
            }
            tokio::runtime::Runtime::new()?.block_on(kalpakdb::stress::stress(opts))
        }
        // kalpakdb cert <out-dir> [--hosts h1,h2,...]
        // Self-signed dev certificate for `serve --tls-cert/--tls-key`.
        [cmd, dir, rest @ ..] if cmd == "cert" => {
            let mut hosts = vec!["localhost".to_string(), "127.0.0.1".to_string()];
            let mut it = rest.iter();
            while let Some(flag) = it.next() {
                match flag.as_str() {
                    "--hosts" => {
                        hosts = it
                            .next()
                            .ok_or("--hosts needs a value")?
                            .split(',')
                            .map(|h| h.trim().to_string())
                            .collect()
                    }
                    other => return Err(format!("unknown flag: {other}").into()),
                }
            }
            let ck = rcgen::generate_simple_self_signed(hosts.clone())?;
            std::fs::create_dir_all(dir)?;
            let cert_path = format!("{dir}/kalpak-cert.pem");
            let key_path = format!("{dir}/kalpak-key.pem");
            std::fs::write(&cert_path, ck.cert.pem())?;
            std::fs::write(&key_path, ck.signing_key.serialize_pem())?;
            println!(
                "wrote {cert_path}
wrote {key_path}
hosts: {}",
                hosts.join(", ")
            );
            println!(
                "serve with:  kalpakdb serve <dir> --tls-cert {cert_path} --tls-key {key_path}"
            );
            Ok(())
        }
        [cmd, dir, rest @ ..] if cmd == "bench" => {
            let mut blocks: u64 = 2000;
            let mut size: usize = 64 * 1024;
            let mut it = rest.iter();
            while let Some(flag) = it.next() {
                match flag.as_str() {
                    "--blocks" => blocks = it.next().ok_or("--blocks needs a value")?.parse()?,
                    "--size-kb" => {
                        size = it
                            .next()
                            .ok_or("--size-kb needs a value")?
                            .parse::<usize>()?
                            * 1024
                    }
                    other => return Err(format!("unknown flag: {other}").into()),
                }
            }
            bench(dir, blocks, size)
        }
        [cmd, dir] if cmd == "put" => {
            let store = BlockStore::open(dir)?;
            let mut payload = Vec::new();
            std::io::stdin().read_to_end(&mut payload)?;
            let id = store.put(&payload)?;
            println!("{id}");
            Ok(())
        }
        [cmd, dir, id] if cmd == "get" => {
            let store = BlockStore::open(dir)?;
            let id: BlockId = id.parse()?;
            let payload = store.get(&id)?;
            std::io::stdout().write_all(&payload)?;
            Ok(())
        }
        // kalpakdb key <model-id> <tokenizer-hash> <kv-layout> <t1,t2,..> [t,..]*
        // Prints the chained CacheKey JSON per chunk (root first).
        [cmd, model, tok, layout, chunks @ ..] if cmd == "key" && !chunks.is_empty() => {
            let fp = kalpak_core::ModelFingerprint::new(model, tok, layout);
            let mut key: Option<kalpak_core::CacheKey> = None;
            for chunk in chunks {
                let tokens: Vec<u32> = chunk
                    .split(',')
                    .map(|t| t.trim().parse())
                    .collect::<Result<_, _>>()?;
                let next = match &key {
                    None => kalpak_core::CacheKey::root(fp.clone(), &tokens),
                    Some(prev) => prev.extend(&tokens),
                };
                println!("{}", serde_json::to_string(&next)?);
                key = Some(next);
            }
            Ok(())
        }
        [cmd, dir] if cmd == "stat" => {
            let store = BlockStore::open(dir)?;
            let s = store.stats();
            println!(
                "blocks: {}\nsegments: {}\nbytes_on_disk: {}",
                s.blocks, s.segments, s.bytes_on_disk
            );
            Ok(())
        }
        _ => {
            eprintln!(
                "usage:\n  kalpakdb serve <data-dir> [--addr 127.0.0.1:7411] [--warm-mb 256] [--node-id 1] [--join] [--compact-secs 3600]\n  kalpakdb witness <data-dir> [--addr ...] [--node-id N]   (consensus-only voter)\n  kalpakdb bench <data-dir> [--blocks 2000] [--size-kb 64]\n  kalpakdb stress <base-url> [--agents 8] [--secs 10] [--chunk-kb 64]\n  kalpakdb put <data-dir>            (payload on stdin)\n  kalpakdb get <data-dir> <block-id>\n  kalpakdb stat <data-dir>"
            );
            Err("invalid arguments".into())
        }
    }
}

/// Local storage benchmark: sequential put, cold get (fresh store), and warm
/// get through the tiered store. Distinct payloads defeat dedup so puts
/// measure real writes.
fn bench(dir: &str, blocks: u64, size: usize) -> Result<(), Box<dyn std::error::Error>> {
    use std::time::Instant;

    let dir = format!("{dir}/bench-{}", std::process::id());
    let mibs = |bytes: f64, secs: f64| bytes / (1024.0 * 1024.0) / secs;
    let total_bytes = (blocks as usize * size) as f64;

    let mut payload = vec![0u8; size];
    let store = kalpak_storage::TieredStore::open(&dir, 512 * 1024 * 1024)?;

    let t = Instant::now();
    let mut ids = Vec::with_capacity(blocks as usize);
    for i in 0..blocks {
        payload[..8].copy_from_slice(&i.to_le_bytes());
        ids.push(store.put(&payload)?);
    }
    let put_s = t.elapsed().as_secs_f64();

    // Batch path: same volume, fresh payloads, one fsync per batch of 64.
    let t = Instant::now();
    let mut batch: Vec<Vec<u8>> = Vec::with_capacity(64);
    for i in 0..blocks {
        let mut p = payload.clone();
        p[..8].copy_from_slice(&(i + blocks).to_le_bytes());
        batch.push(p);
        if batch.len() == 64 {
            store.put_many(batch.iter().map(|b| b.as_slice()))?;
            batch.clear();
        }
    }
    if !batch.is_empty() {
        store.put_many(batch.iter().map(|b| b.as_slice()))?;
    }
    let batch_s = t.elapsed().as_secs_f64();

    let t = Instant::now();
    for id in &ids {
        store.get(id)?;
    }
    let warm_s = t.elapsed().as_secs_f64();

    drop(store);
    let store = kalpak_storage::TieredStore::open(&dir, 512 * 1024 * 1024)?;
    let t = Instant::now();
    for id in &ids {
        store.get(id)?;
    }
    let cold_s = t.elapsed().as_secs_f64();

    println!("kalpakdb bench: {blocks} blocks x {} KiB", size / 1024);
    println!(
        "  put       {:>10.0} blk/s  {:>8.1} MiB/s  (fsync per block)",
        blocks as f64 / put_s,
        mibs(total_bytes, put_s)
    );
    println!(
        "  put batch {:>10.0} blk/s  {:>8.1} MiB/s  (group commit, 64/batch)",
        blocks as f64 / batch_s,
        mibs(total_bytes, batch_s)
    );
    println!(
        "  get warm  {:>10.0} blk/s  {:>8.1} MiB/s",
        blocks as f64 / warm_s,
        mibs(total_bytes, warm_s)
    );
    println!(
        "  get cold  {:>10.0} blk/s  {:>8.1} MiB/s  (reopen, disk + verify)",
        blocks as f64 / cold_s,
        mibs(total_bytes, cold_s)
    );
    std::fs::remove_dir_all(&dir)?;
    Ok(())
}
