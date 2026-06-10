//! Kalpak node CLI.
//!
//!   kalpakdb serve <data-dir> [--addr 127.0.0.1:7411] [--warm-mb 256]
//!   kalpakdb put <data-dir>            reads payload from stdin, prints id
//!   kalpakdb get <data-dir> <block-id> writes payload to stdout
//!   kalpakdb stat <data-dir>           prints store statistics

mod server;

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
        [cmd, dir, rest @ ..] if cmd == "serve" => {
            let mut addr = "127.0.0.1:7411".to_string();
            let mut warm_mb: u64 = 256;
            let mut it = rest.iter();
            while let Some(flag) = it.next() {
                match flag.as_str() {
                    "--addr" => addr = it.next().ok_or("--addr needs a value")?.clone(),
                    "--warm-mb" => warm_mb = it.next().ok_or("--warm-mb needs a value")?.parse()?,
                    other => return Err(format!("unknown flag: {other}").into()),
                }
            }
            tokio::runtime::Runtime::new()?.block_on(server::serve(
                dir.clone(),
                addr,
                warm_mb * 1024 * 1024,
            ))
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
                "usage:\n  kalpakdb serve <data-dir> [--addr 127.0.0.1:7411] [--warm-mb 256]\n  kalpakdb put <data-dir>            (payload on stdin)\n  kalpakdb get <data-dir> <block-id>\n  kalpakdb stat <data-dir>"
            );
            Err("invalid arguments".into())
        }
    }
}
