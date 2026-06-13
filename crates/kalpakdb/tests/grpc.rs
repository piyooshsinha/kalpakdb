//! gRPC streaming data plane: chunked put stream into one group commit,
//! chunked get stream out, and cross-plane consistency with the HTTP API.

use std::time::{Duration, Instant};

use kalpak_proto::v1::block_service_client::BlockServiceClient;
use kalpak_proto::v1::{GetBlockRequest, PutBlockChunk};
use kalpakdb::server::{serve, ServeOpts};

#[tokio::test(flavor = "multi_thread")]
async fn grpc_streams_blocks_through_group_commit() {
    let dir = tempfile::tempdir().unwrap();
    let opts = ServeOpts {
        data_dir: dir.path().to_string_lossy().into_owned(),
        addr: "127.0.0.1:17551".to_string(),
        warm_bytes: 16 * 1024 * 1024,
        node_id: 1,
        bootstrap: true,
        grpc_addr: Some("127.0.0.1:17552".to_string()),
        compact_secs: 0,
        require_signatures: false,
        read_token: None,
        tls_cert: None,
        tls_key: None,
        mesh: None,
    };
    tokio::spawn(async move {
        let _ = serve(opts).await;
    });

    // Wait for the gRPC plane to come up.
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut client = loop {
        match BlockServiceClient::connect("http://127.0.0.1:17552").await {
            Ok(c) => break c,
            Err(_) => {
                assert!(Instant::now() < deadline, "gRPC server did not start");
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    };

    // Stream three blocks: a small one, a 3 MiB one split across chunks
    // (larger than any single message), and another small one.
    let big: Vec<u8> = (0..3 * 1024 * 1024u32).map(|i| (i % 251) as u8).collect();
    let mut chunks = vec![PutBlockChunk {
        data: b"first-small-block".to_vec().into(),
        last_chunk: true,
    }];
    for piece in big.chunks(1024 * 1024) {
        chunks.push(PutBlockChunk {
            data: piece.to_vec().into(),
            last_chunk: false,
        });
    }
    chunks.last_mut().unwrap().last_chunk = true;
    chunks.push(PutBlockChunk {
        data: b"trailing-block".to_vec().into(),
        last_chunk: true,
    });

    let resp = client
        .put_blocks(tokio_stream::iter(chunks))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(resp.ids.len(), 3);

    // Content addressing holds across the wire.
    assert_eq!(
        resp.ids[1],
        kalpak_core::BlockId::of(&big).to_string(),
        "streamed chunks must reassemble into the exact payload"
    );

    // Stream the big block back and reassemble it.
    let mut out = Vec::new();
    let mut stream = client
        .get_block(GetBlockRequest {
            id: resp.ids[1].clone(),
        })
        .await
        .unwrap()
        .into_inner();
    let mut saw_last = false;
    while let Some(chunk) = stream.message().await.unwrap() {
        out.extend_from_slice(&chunk.data);
        if chunk.last_chunk {
            saw_last = true;
        }
    }
    assert!(saw_last);
    assert_eq!(out, big);

    // Cross-plane: the HTTP API serves the same block.
    let http = reqwest::get(format!("http://127.0.0.1:17551/v1/blocks/{}", resp.ids[0]))
        .await
        .unwrap();
    assert!(http.status().is_success());
    assert_eq!(http.bytes().await.unwrap(), "first-small-block");

    // A missing block is NOT_FOUND, and a torn stream is rejected.
    let missing = client
        .get_block(GetBlockRequest {
            id: kalpak_core::BlockId::of(b"never stored").to_string(),
        })
        .await;
    match missing {
        Ok(mut s) => assert!(matches!(
            s.get_mut().message().await,
            Err(ref e) if e.code() == tonic::Code::NotFound
        )),
        Err(e) => assert_eq!(e.code(), tonic::Code::NotFound),
    }

    let torn = client
        .put_blocks(tokio_stream::iter(vec![PutBlockChunk {
            data: b"half a block".to_vec().into(),
            last_chunk: false,
        }]))
        .await;
    assert_eq!(torn.unwrap_err().code(), tonic::Code::InvalidArgument);
}
