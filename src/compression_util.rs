use bytes::Bytes;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressionType {
    Zstd,
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
