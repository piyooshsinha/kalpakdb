//! Kalpak node CLI. For now: a local block-store smoke tool.
//!
//!   kalpakdb put <data-dir>            reads payload from stdin, prints id
//!   kalpakdb get <data-dir> <block-id> writes payload to stdout
//!   kalpakdb stat <data-dir>           prints store statistics

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
                "usage:\n  kalpakdb put <data-dir>            (payload on stdin)\n  kalpakdb get <data-dir> <block-id>\n  kalpakdb stat <data-dir>"
            );
            Err("invalid arguments".into())
        }
    }
}
