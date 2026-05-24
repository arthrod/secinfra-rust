use bytes::Bytes;
use futures::{Stream, StreamExt};
use std::io::Write;
use tokio::sync::mpsc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressionType {
    Zstd,
}

#[derive(Debug)]
pub struct CompressedBytes {
    pub bytes: Vec<u8>,
    pub input_bytes: usize,
    pub compressed_bytes: usize,
}

pub fn compress_bytes(
    compression_type: CompressionType,
    level: i32,
    bytes: &[u8],
) -> anyhow::Result<Vec<u8>> {
    match compression_type {
        CompressionType::Zstd => zstd::bulk::compress(bytes, level).map_err(Into::into),
    }
}

pub async fn compress_bytes_async(
    compression_type: CompressionType,
    level: i32,
    bytes: Bytes,
) -> anyhow::Result<Vec<u8>> {
    tokio::task::spawn_blocking(move || compress_bytes(compression_type, level, &bytes)).await?
}

pub async fn compress_byte_stream_async<S, E>(
    compression_type: CompressionType,
    level: i32,
    stream: S,
) -> anyhow::Result<CompressedBytes>
where
    S: Stream<Item = Result<Bytes, E>> + Send,
    E: Into<anyhow::Error> + Send + Sync + 'static,
{
    let (tx, mut rx) = mpsc::channel::<Bytes>(4);
    let compression_task = tokio::task::spawn_blocking(move || -> anyhow::Result<Vec<u8>> {
        match compression_type {
            CompressionType::Zstd => {
                let mut encoder = zstd::stream::write::Encoder::new(Vec::new(), level)?;
                while let Some(chunk) = rx.blocking_recv() {
                    encoder.write_all(&chunk)?;
                }
                encoder.finish().map_err(Into::into)
            }
        }
    });

    let mut input_bytes = 0usize;
    futures::pin_mut!(stream);

    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(Into::into)?;
        input_bytes += chunk.len();
        tx.send(chunk)
            .await
            .map_err(|_| anyhow::anyhow!("compression task stopped before stream ended"))?;
    }
    drop(tx);

    let bytes = compression_task.await??;
    let compressed_bytes = bytes.len();

    Ok(CompressedBytes {
        bytes,
        input_bytes,
        compressed_bytes,
    })
}
