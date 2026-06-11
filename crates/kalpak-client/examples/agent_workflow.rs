//! The full agent workflow against a running node.
//!
//! Start a node, then run the example:
//!
//! ```sh
//! cargo run -p kalpakdb -- serve /tmp/kalpak-demo &
//! cargo run -p kalpak-client --example agent_workflow
//! ```

use kalpak_client::KalpakClient;
use kalpak_core::{AgentId, CacheKey, ModelFingerprint};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = KalpakClient::new("http://127.0.0.1:7411");

    // 1. The agent is a keypair; its public key is its identity.
    let agent: AgentId = "07".repeat(32).parse()?;
    db.register_agent(agent, "example-agent").await?;
    println!("registered agent {agent}");

    // 2. Chunk the token stream and chain cache keys over it.
    let fp = ModelFingerprint::new("example/model", "tok-v1", "fp16/paged-16");
    let k0 = CacheKey::root(fp, &[1, 2, 3, 4]);
    let k1 = k0.extend(&[5, 6, 7, 8]);

    // 3. Probe for the longest cached prefix before prefilling.
    match db.lookup(&[k0.clone(), k1.clone()]).await? {
        Some(hit) => {
            println!(
                "cache hit at depth {} ({} blocks)",
                hit.depth,
                hit.blocks.len()
            );
            for id in &hit.blocks {
                let bytes = db.get_block(id).await?;
                println!(
                    "  reused block {id} ({} bytes, prefetched warm)",
                    bytes.len()
                );
            }
        }
        None => {
            println!("cache miss; offloading freshly computed KV chunks");
            // 4. Offload both chunks in one group-committed batch...
            let chunks = vec![b"kv-tensor-chunk-0".to_vec(), b"kv-tensor-chunk-1".to_vec()];
            let ids = db.put_blocks(&chunks).await?;
            // 5. ...and bind each prefix depth to its blocks.
            db.bind_prefix(agent, k0, vec![ids[0]]).await?;
            db.bind_prefix(agent, k1, vec![ids[0], ids[1]]).await?;
            println!("bound 2 prefix depths; run the example again for a hit");
        }
    }

    println!(
        "\nnode stats:\n{}",
        serde_json::to_string_pretty(&db.stats().await?)?
    );
    Ok(())
}
