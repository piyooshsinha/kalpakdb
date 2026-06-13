//! Property-based model testing: the block store must agree with a trivial
//! in-memory model (`HashMap<BlockId, Vec<u8>>`) under ANY interleaving of
//! puts, batched puts, reads, reopens, and compactions — with segments
//! rolling every 16 KiB so sequences cross sealing boundaries constantly.
//!
//! The invariants proptest hunts counterexamples for:
//! 1. a stored block always reads back byte-identical (never wrong bytes);
//! 2. reopen loses nothing;
//! 3. compaction keeps every live block readable, and never corrupts —
//!    a swept block may only ever report NotFound, never bad data.

use std::collections::HashMap;

use kalpak_core::{BlockId, Error};
use kalpak_storage::BlockStore;
use proptest::prelude::*;

#[derive(Clone, Debug)]
enum Op {
    /// Store one payload (dedup makes repeats interesting).
    Put(Vec<u8>),
    /// Group-commit several payloads at once.
    PutMany(Vec<Vec<u8>>),
    /// Read back the n-th previously stored block (mod stored count).
    Get(usize),
    /// Drop and reopen the store: index rebuilds from segments alone.
    Reopen,
    /// Mark-and-sweep keeping only blocks whose insertion order index is
    /// even / odd / all — sealed-segment GC under a shifting live set.
    Compact(u8),
}

fn payload_strategy() -> impl Strategy<Value = Vec<u8>> {
    // Sizes straddle the 4 KiB record alignment and the 16 KiB roll size.
    prop::collection::vec(any::<u8>(), 1..6000)
}

fn op_strategy() -> impl Strategy<Value = Op> {
    prop_oneof![
        4 => payload_strategy().prop_map(Op::Put),
        2 => prop::collection::vec(payload_strategy(), 1..5).prop_map(Op::PutMany),
        4 => any::<usize>().prop_map(Op::Get),
        1 => Just(Op::Reopen),
        1 => any::<u8>().prop_map(Op::Compact),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 64, ..ProptestConfig::default()
    })]

    #[test]
    fn store_agrees_with_model(ops in prop::collection::vec(op_strategy(), 1..40)) {
        let dir = tempfile::tempdir().unwrap();
        // Tiny roll size: sequences regularly seal segments, so compaction
        // and reopen exercise multi-segment paths, not toy single files.
        let mut store = BlockStore::open_with_roll(dir.path(), 16 * 1024).unwrap();

        // The model: id -> payload, plus insertion order and the live set.
        let mut model: HashMap<BlockId, Vec<u8>> = HashMap::new();
        let mut order: Vec<BlockId> = Vec::new();
        let mut swept: Vec<BlockId> = Vec::new();

        for op in ops {
            match op {
                Op::Put(p) => {
                    let id = store.put(&p).unwrap();
                    prop_assert_eq!(id, BlockId::of(&p), "content addressing");
                    if model.insert(id, p).is_none() {
                        order.push(id);
                    }
                    swept.retain(|s| *s != id); // re-put resurrects
                }
                Op::PutMany(ps) => {
                    let ids = store
                        .put_many(ps.iter().map(|p| p.as_slice()))
                        .unwrap();
                    prop_assert_eq!(ids.len(), ps.len());
                    for (id, p) in ids.into_iter().zip(ps) {
                        if model.insert(id, p).is_none() {
                            order.push(id);
                        }
                        swept.retain(|s| *s != id);
                    }
                }
                Op::Get(n) => {
                    if order.is_empty() {
                        continue;
                    }
                    let id = order[n % order.len()];
                    match store.get(&id) {
                        Ok(bytes) => {
                            // Whether live or in the GC grace window, served
                            // bytes must be EXACTLY the model's bytes.
                            prop_assert_eq!(&bytes, model.get(&id).unwrap());
                        }
                        Err(Error::BlockNotFound(_)) => {
                            prop_assert!(
                                swept.contains(&id),
                                "live block went missing: {id}"
                            );
                        }
                        Err(e) => return Err(TestCaseError::fail(format!(
                            "unexpected error for {id}: {e}"
                        ))),
                    }
                }
                Op::Reopen => {
                    drop(store);
                    store = BlockStore::open_with_roll(dir.path(), 16 * 1024).unwrap();
                }
                Op::Compact(mode) => {
                    // The live set can only contain blocks that still exist:
                    // a block swept by an earlier compaction stays dead
                    // until a re-put resurrects it.
                    let keep: Vec<BlockId> = order
                        .iter()
                        .enumerate()
                        .filter(|(i, _)| match mode % 3 {
                            0 => i % 2 == 0,
                            1 => i % 2 == 1,
                            _ => true,
                        })
                        .map(|(_, id)| *id)
                        .filter(|id| !swept.contains(id))
                        .collect();
                    store.compact(|id| keep.contains(id)).unwrap();
                    for id in &order {
                        if !keep.contains(id) && !swept.contains(id) {
                            swept.push(*id);
                        }
                    }
                    // Live blocks survive compaction unconditionally.
                    for id in &keep {
                        prop_assert_eq!(
                            &store.get(id).unwrap(),
                            model.get(id).unwrap(),
                            "compaction lost a live block"
                        );
                    }
                }
            }
        }

        // Epilogue: a final reopen + fsck must always come up clean, and the
        // full live set must be intact.
        drop(store);
        let store = BlockStore::open_with_roll(dir.path(), 16 * 1024).unwrap();
        let report = store.verify_all();
        prop_assert!(report.is_clean(), "fsck after sequence: {report:?}");
        for id in &order {
            if !swept.contains(id) {
                prop_assert_eq!(&store.get(id).unwrap(), model.get(id).unwrap());
            }
        }
    }

    /// The segment scanner must never panic or admit unverifiable garbage,
    /// no matter what bytes are on disk.
    #[test]
    fn arbitrary_segment_bytes_never_panic(bytes in prop::collection::vec(any::<u8>(), 0..32_768)) {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("seg-00000000.klpk"), &bytes).unwrap();
        // Opening scans the garbage; it must not panic.
        let store = BlockStore::open(dir.path()).unwrap();
        // Anything the scan indexed must either verify or fail hash check —
        // verify_all is exactly that sweep, and must not panic either.
        let _ = store.verify_all();
        // The store must still accept writes after surviving garbage.
        let id = store.put(b"after garbage").unwrap();
        prop_assert_eq!(store.get(&id).unwrap(), b"after garbage".to_vec());
    }
}
