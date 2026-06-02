use std::collections::HashSet;
use std::fs::{self, File};
use std::io::{BufWriter, Read, Seek, Write};
use std::path::{Path, PathBuf};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
    mpsc,
};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow};
use serde_json::Value;
use tracing::info;
use zip::ZipArchive;

use crate::common::sec_user_agent;

const SUBMISSIONS_ZIP_URL: &str =
    "https://www.sec.gov/Archives/edgar/daily-index/bulkdata/submissions.zip";
const DEFAULT_COLUMNS: [&str; 16] = [
    "accessionNumber",
    "filingDate",
    "reportDate",
    "acceptanceDateTime",
    "act",
    "form",
    "fileNumber",
    "filmNumber",
    "items",
    "core_type",
    "size",
    "isXBRL",
    "isInlineXBRL",
    "isXBRLNumeric",
    "primaryDocument",
    "primaryDocDescription",
];
const BATCH_TARGET_ROWS: usize = 100_000;
const CSV_BUFFER_BYTES: usize = 8 * 1024 * 1024;
const WORKER_FILE_BATCH_SIZE: usize = 100;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConstructSubmissionsMetadataStats {
    pub files_processed: usize,
    pub files_skipped: usize,
    pub filings_written: usize,
    pub batches_written: usize,
}

#[derive(Clone)]
struct ColumnSpec {
    name: String,
}

#[derive(Default)]
struct CsvChunkStats {
    rows: usize,
    files: usize,
    json_bytes: usize,
    read_elapsed: Duration,
    parse_elapsed: Duration,
    write_elapsed: Duration,
}

struct SubmissionFileStats {
    rows: usize,
    json_bytes: usize,
    read_elapsed: Duration,
    parse_elapsed: Duration,
    write_elapsed: Duration,
}

struct TempZipFile {
    path: PathBuf,
}

struct CsvWorkerResult {
    csv: Vec<u8>,
    chunk: CsvChunkStats,
}

pub async fn construct_submissions_metadata(
    output_path: impl Into<PathBuf>,
    submissions_zip_path: Option<PathBuf>,
    columns: Option<Vec<String>>,
    threads: Option<usize>,
) -> Result<ConstructSubmissionsMetadataStats> {
    let output_path = output_path.into();
    let columns = resolve_columns(columns)?;
    let threads = resolve_threads(threads)?;

    if let Some(submissions_zip_path) = submissions_zip_path {
        return tokio::task::spawn_blocking(move || {
            write_submissions_metadata_csv_from_path(
                submissions_zip_path,
                output_path,
                columns,
                threads,
            )
        })
        .await?;
    }

    let temp_zip = download_submissions_zip_to_temp().await?;
    tokio::task::spawn_blocking(move || {
        write_submissions_metadata_csv_from_path(&temp_zip.path, output_path, columns, threads)
    })
    .await?
}

pub fn construct_submissions_metadata_from_zip(
    output_path: impl AsRef<Path>,
    submissions_zip_path: impl AsRef<Path>,
    columns: Option<Vec<String>>,
    threads: Option<usize>,
) -> Result<ConstructSubmissionsMetadataStats> {
    let columns = resolve_columns(columns)?;
    let threads = resolve_threads(threads)?;

    write_submissions_metadata_csv_from_path(
        submissions_zip_path.as_ref(),
        output_path,
        columns,
        threads,
    )
}

async fn download_submissions_zip_to_temp() -> Result<TempZipFile> {
    let client = reqwest::Client::builder()
        .user_agent(sec_user_agent())
        .build()
        .context("failed to build SEC HTTP client")?;

    info!(url = SUBMISSIONS_ZIP_URL, "Downloading SEC submissions zip");
    let bytes = client
        .get(SUBMISSIONS_ZIP_URL)
        .send()
        .await
        .context("failed to download SEC submissions zip")?
        .error_for_status()
        .context("SEC submissions zip request failed")?
        .bytes()
        .await
        .context("failed to read SEC submissions zip response")?
        .to_vec();
    let path = temp_submissions_zip_path()?;
    let bytes_len = bytes.len();
    tokio::task::spawn_blocking({
        let path = path.clone();
        move || -> Result<()> {
            let mut file = File::create(&path).with_context(|| {
                format!("failed to create temp submissions zip {}", path.display())
            })?;
            file.write_all(&bytes).with_context(|| {
                format!("failed to write temp submissions zip {}", path.display())
            })?;
            Ok(())
        }
    })
    .await??;
    info!(bytes = bytes_len, path = %path.display(), "Downloaded SEC submissions zip");
    Ok(TempZipFile { path })
}

fn write_submissions_metadata_csv<R>(
    submissions_zip: R,
    output_path: impl AsRef<Path>,
    columns: Vec<ColumnSpec>,
) -> Result<ConstructSubmissionsMetadataStats>
where
    R: Read + Seek,
{
    let started_at = Instant::now();
    let mut archive =
        ZipArchive::new(submissions_zip).context("failed to open submissions zip archive")?;
    let output = File::create(output_path.as_ref()).with_context(|| {
        format!(
            "failed to create submissions metadata CSV {}",
            output_path.as_ref().display()
        )
    })?;
    let mut writer = BufWriter::with_capacity(CSV_BUFFER_BYTES, output);
    write_csv_header(&mut writer, &columns).context("failed to write submissions metadata header")?;

    let mut stats = ConstructSubmissionsMetadataStats {
        files_processed: 0,
        files_skipped: 0,
        filings_written: 0,
        batches_written: 0,
    };
    let mut chunk = CsvChunkStats::default();

    info!(
        zip_entries = archive.len(),
        columns = ?column_names(&columns),
        batch_target_rows = BATCH_TARGET_ROWS,
        output = "csv",
        "Processing SEC submissions metadata zip"
    );

    for index in 0..archive.len() {
        let mut file = archive
            .by_index(index)
            .with_context(|| format!("failed to read submissions zip entry {index}"))?;

        let filename = file.name().to_string();
        if !filename.starts_with("CIK") {
            continue;
        }

        let file_stats = write_submission_csv_file(&filename, &mut file, &columns, &mut writer)
            .with_context(|| format!("failed to process submissions metadata file {filename}"))?;
        stats.files_processed += 1;
        stats.filings_written += file_stats.rows;
        chunk.add_file(file_stats);

        if chunk.rows >= BATCH_TARGET_ROWS {
            flush_csv_chunk(&mut writer, &mut chunk, &mut stats)?;
        }
    }

    flush_csv_chunk(&mut writer, &mut chunk, &mut stats)?;
    writer
        .flush()
        .context("failed to flush submissions metadata CSV")?;

    info!(
        files_processed = stats.files_processed,
        files_skipped = stats.files_skipped,
        filings_written = stats.filings_written,
        batches_written = stats.batches_written,
        total_ms = started_at.elapsed().as_millis(),
        output = "csv",
        "Finished constructing submissions metadata"
    );

    Ok(stats)
}

fn write_submissions_metadata_csv_from_path(
    submissions_zip_path: impl AsRef<Path>,
    output_path: impl AsRef<Path>,
    columns: Vec<ColumnSpec>,
    threads: usize,
) -> Result<ConstructSubmissionsMetadataStats> {
    if threads <= 1 {
        let file = File::open(submissions_zip_path.as_ref()).with_context(|| {
            format!(
                "failed to open submissions zip {}",
                submissions_zip_path.as_ref().display()
            )
        })?;
        return write_submissions_metadata_csv(file, output_path, columns);
    }

    write_submissions_metadata_csv_parallel(submissions_zip_path, output_path, columns, threads)
}

fn write_submissions_metadata_csv_parallel(
    submissions_zip_path: impl AsRef<Path>,
    output_path: impl AsRef<Path>,
    columns: Vec<ColumnSpec>,
    threads: usize,
) -> Result<ConstructSubmissionsMetadataStats> {
    let started_at = Instant::now();
    let submissions_zip_path = submissions_zip_path.as_ref().to_path_buf();
    let filenames = collect_cik_filenames(&submissions_zip_path)?;
    let batches = distribute_filename_batches(&filenames, threads);

    let output = File::create(output_path.as_ref()).with_context(|| {
        format!(
            "failed to create submissions metadata CSV {}",
            output_path.as_ref().display()
        )
    })?;
    let mut writer = BufWriter::with_capacity(CSV_BUFFER_BYTES, output);
    write_csv_header(&mut writer, &columns).context("failed to write submissions metadata header")?;

    let mut stats = ConstructSubmissionsMetadataStats {
        files_processed: 0,
        files_skipped: 0,
        filings_written: 0,
        batches_written: 0,
    };
    let cancel = Arc::new(AtomicBool::new(false));
    let (tx, rx) = mpsc::sync_channel::<Result<CsvWorkerResult>>(threads * 2);

    info!(
        zip_entries = filenames.len(),
        columns = ?column_names(&columns),
        batch_target_rows = BATCH_TARGET_ROWS,
        worker_file_batch_size = WORKER_FILE_BATCH_SIZE,
        threads,
        output = "csv",
        "Processing SEC submissions metadata zip"
    );

    let scoped_result = thread::scope(|scope| -> Result<()> {
        for worker_batches in batches {
            let worker_tx = tx.clone();
            let worker_cancel = Arc::clone(&cancel);
            let worker_zip_path = submissions_zip_path.clone();
            let worker_columns = &columns;
            scope.spawn(move || {
                process_csv_worker_batches(
                    worker_zip_path,
                    worker_batches,
                    worker_columns,
                    worker_tx,
                    worker_cancel,
                );
            });
        }
        drop(tx);

        let mut first_error = None;
        for result in rx {
            match result {
                Ok(mut result) => {
                    if first_error.is_some() {
                        continue;
                    }

                    let write_started_at = Instant::now();
                    if let Err(error) = writer
                        .write_all(&result.csv)
                        .context("failed to write submissions metadata CSV chunk")
                    {
                        cancel.store(true, Ordering::Relaxed);
                        first_error = Some(error);
                        continue;
                    }
                    result.chunk.write_elapsed += write_started_at.elapsed();

                    stats.files_processed += result.chunk.files;
                    stats.filings_written += result.chunk.rows;
                    if let Err(error) = flush_csv_chunk(&mut writer, &mut result.chunk, &mut stats)
                    {
                        cancel.store(true, Ordering::Relaxed);
                        first_error = Some(error);
                    }
                }
                Err(error) => {
                    cancel.store(true, Ordering::Relaxed);
                    if first_error.is_none() {
                        first_error = Some(error);
                    }
                }
            }
        }

        if let Some(error) = first_error {
            return Err(error);
        }

        Ok(())
    });

    scoped_result?;
    writer
        .flush()
        .context("failed to flush submissions metadata CSV")?;

    info!(
        files_processed = stats.files_processed,
        files_skipped = stats.files_skipped,
        filings_written = stats.filings_written,
        batches_written = stats.batches_written,
        total_ms = started_at.elapsed().as_millis(),
        threads,
        output = "csv",
        "Finished constructing submissions metadata"
    );

    Ok(stats)
}

fn collect_cik_filenames(submissions_zip_path: &Path) -> Result<Vec<String>> {
    let file = File::open(submissions_zip_path).with_context(|| {
        format!(
            "failed to open submissions zip {}",
            submissions_zip_path.display()
        )
    })?;
    let mut archive =
        ZipArchive::new(file).context("failed to open submissions zip archive")?;
    let mut filenames = Vec::new();

    for index in 0..archive.len() {
        let file = archive
            .by_index(index)
            .with_context(|| format!("failed to read submissions zip entry {index}"))?;
        let filename = file.name();
        if filename.starts_with("CIK") {
            filenames.push(filename.to_string());
        }
    }

    Ok(filenames)
}

fn distribute_filename_batches(filenames: &[String], threads: usize) -> Vec<Vec<Vec<String>>> {
    let mut worker_batches = vec![Vec::new(); threads];
    for (batch_index, batch) in filenames.chunks(WORKER_FILE_BATCH_SIZE).enumerate() {
        worker_batches[batch_index % threads].push(batch.to_vec());
    }
    worker_batches
}

fn process_csv_worker_batches(
    submissions_zip_path: PathBuf,
    batches: Vec<Vec<String>>,
    columns: &[ColumnSpec],
    tx: mpsc::SyncSender<Result<CsvWorkerResult>>,
    cancel: Arc<AtomicBool>,
) {
    let result = process_csv_worker_batches_inner(
        &submissions_zip_path,
        batches,
        columns,
        &tx,
        &cancel,
    );
    if let Err(error) = result {
        cancel.store(true, Ordering::Relaxed);
        let _ = tx.send(Err(error));
    }
}

fn process_csv_worker_batches_inner(
    submissions_zip_path: &Path,
    batches: Vec<Vec<String>>,
    columns: &[ColumnSpec],
    tx: &mpsc::SyncSender<Result<CsvWorkerResult>>,
    cancel: &AtomicBool,
) -> Result<()> {
    let file = File::open(submissions_zip_path).with_context(|| {
        format!(
            "failed to open submissions zip {}",
            submissions_zip_path.display()
        )
    })?;
    let mut archive =
        ZipArchive::new(file).context("failed to open submissions zip archive")?;
    let mut csv = Vec::with_capacity(CSV_BUFFER_BYTES / 2);
    let mut chunk = CsvChunkStats::default();

    for batch in batches {
        if cancel.load(Ordering::Relaxed) {
            break;
        }

        for filename in batch {
            if cancel.load(Ordering::Relaxed) {
                break;
            }

            let mut file = archive
                .by_name(&filename)
                .with_context(|| format!("failed to read submissions zip entry {filename}"))?;
            let file_stats = write_submission_csv_file(&filename, &mut file, columns, &mut csv)
                .with_context(|| {
                    format!("failed to process submissions metadata file {filename}")
                })?;
            chunk.add_file(file_stats);

            if chunk.rows >= BATCH_TARGET_ROWS {
                send_csv_worker_result(tx, &mut csv, &mut chunk, cancel)?;
            }
        }
    }

    if !cancel.load(Ordering::Relaxed) && chunk.rows > 0 {
        send_csv_worker_result(tx, &mut csv, &mut chunk, cancel)?;
    }

    Ok(())
}

fn send_csv_worker_result(
    tx: &mpsc::SyncSender<Result<CsvWorkerResult>>,
    csv: &mut Vec<u8>,
    chunk: &mut CsvChunkStats,
    cancel: &AtomicBool,
) -> Result<()> {
    let result = CsvWorkerResult {
        csv: std::mem::replace(csv, Vec::with_capacity(CSV_BUFFER_BYTES / 2)),
        chunk: std::mem::take(chunk),
    };

    match tx.send(Ok(result)) {
        Ok(()) => Ok(()),
        Err(_) if cancel.load(Ordering::Relaxed) => Ok(()),
        Err(_) => Err(anyhow!("submissions metadata CSV writer channel closed")),
    }
}

fn write_csv_header<W>(writer: &mut W, columns: &[ColumnSpec]) -> Result<()>
where
    W: Write,
{
    writer.write_all(b"cik")?;
    for column in columns {
        writer.write_all(b",")?;
        write_csv_str(writer, &column.name)?;
    }
    writer.write_all(b"\n")?;
    Ok(())
}

fn write_submission_csv_file<R, W>(
    filename: &str,
    mut reader: R,
    columns: &[ColumnSpec],
    writer: &mut W,
) -> Result<SubmissionFileStats>
where
    R: Read,
    W: Write,
{
    let read_started_at = Instant::now();
    let mut bytes = Vec::new();
    reader
        .read_to_end(&mut bytes)
        .with_context(|| format!("failed to read submissions JSON {filename}"))?;
    let json_bytes = bytes.len();
    let read_elapsed = read_started_at.elapsed();

    write_submission_csv_bytes(filename, &bytes, json_bytes, read_elapsed, columns, writer)
}

fn write_submission_csv_bytes<W>(
    filename: &str,
    bytes: &[u8],
    json_bytes: usize,
    read_elapsed: Duration,
    columns: &[ColumnSpec],
    writer: &mut W,
) -> Result<SubmissionFileStats>
where
    W: Write,
{
    let cik = cik_from_filename(filename)?;

    let parse_started_at = Instant::now();
    let data: Value = serde_json::from_slice(bytes)
        .with_context(|| format!("failed to parse submissions JSON {filename}"))?;
    let filings = filings_data(filename, &data)?;
    let row_count = field_array(filings, "accessionNumber")?.len();
    let column_arrays = columns
        .iter()
        .map(|column| field_array(filings, &column.name))
        .collect::<Result<Vec<_>>>()?;

    if column_arrays.iter().any(|values| values.len() < row_count) {
        return Err(anyhow!("submissions fields have mismatched lengths"));
    }
    let parse_elapsed = parse_started_at.elapsed();

    let cik_string = cik.to_string();
    let cik_bytes = cik_string.as_bytes();
    let write_started_at = Instant::now();
    for row_index in 0..row_count {
        writer.write_all(cik_bytes)?;
        for values in &column_arrays {
            writer.write_all(b",")?;
            write_csv_value(writer, &values[row_index])?;
        }
        writer.write_all(b"\n")?;
    }
    let write_elapsed = write_started_at.elapsed();

    Ok(SubmissionFileStats {
        rows: row_count,
        json_bytes,
        read_elapsed,
        parse_elapsed,
        write_elapsed,
    })
}

fn flush_csv_chunk<W>(
    writer: &mut W,
    chunk: &mut CsvChunkStats,
    stats: &mut ConstructSubmissionsMetadataStats,
) -> Result<()>
where
    W: Write,
{
    if chunk.rows == 0 {
        return Ok(());
    }

    let flush_started_at = Instant::now();
    writer
        .flush()
        .context("failed to flush submissions metadata CSV chunk")?;
    let flush_ms = flush_started_at.elapsed().as_millis();

    stats.batches_written += 1;
    info!(
        batch_index = stats.batches_written,
        batch_rows = chunk.rows,
        batch_files = chunk.files,
        batch_json_bytes = chunk.json_bytes,
        total_rows = stats.filings_written,
        files_processed = stats.files_processed,
        files_skipped = stats.files_skipped,
        read_ms = chunk.read_elapsed.as_millis(),
        parse_ms = chunk.parse_elapsed.as_millis(),
        csv_write_ms = chunk.write_elapsed.as_millis(),
        csv_flush_ms = flush_ms,
        "Wrote submissions metadata CSV chunk"
    );

    *chunk = CsvChunkStats::default();
    Ok(())
}

fn resolve_columns(columns: Option<Vec<String>>) -> Result<Vec<ColumnSpec>> {
    let columns = columns.unwrap_or_else(|| {
        DEFAULT_COLUMNS
            .iter()
            .map(|column| column.to_string())
            .collect()
    });
    let mut seen = HashSet::new();
    let mut resolved = Vec::with_capacity(columns.len());

    if columns.is_empty() {
        return Err(anyhow!(
            "at least one submissions metadata column is required"
        ));
    }

    for column in &columns {
        if column.is_empty() {
            return Err(anyhow!("submissions metadata column name cannot be empty"));
        }
        if column == "cik" {
            return Err(anyhow!(
                "cik is always included and cannot be requested as a submissions metadata column"
            ));
        }
        if !seen.insert(column) {
            return Err(anyhow!("duplicate submissions metadata column {column}"));
        }
        resolved.push(ColumnSpec {
            name: column.clone(),
        });
    }

    Ok(resolved)
}

fn resolve_threads(threads: Option<usize>) -> Result<usize> {
    let threads = threads.unwrap_or(1);
    if threads == 0 {
        return Err(anyhow!("submissions metadata threads must be at least 1"));
    }
    Ok(threads)
}

fn temp_submissions_zip_path() -> Result<PathBuf> {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system time is before unix epoch")?
        .as_nanos();
    Ok(std::env::temp_dir().join(format!(
        "secinfra-submissions-{}-{timestamp}.zip",
        std::process::id()
    )))
}

fn cik_from_filename(filename: &str) -> Result<u64> {
    filename
        .split('.')
        .next()
        .and_then(|stem| stem.split('-').next())
        .and_then(|stem| stem.strip_prefix("CIK"))
        .ok_or_else(|| anyhow!("filename does not start with CIK: {filename}"))?
        .parse::<u64>()
        .with_context(|| format!("failed to parse CIK from {filename}"))
}

impl Drop for TempZipFile {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

fn filings_data<'a>(filename: &str, data: &'a Value) -> Result<&'a Value> {
    if filename.contains("submissions") {
        return Ok(data);
    }

    object_get(data, "filings")
        .and_then(|filings| object_get(filings, "recent"))
        .ok_or_else(|| anyhow!("missing filings.recent"))
}

fn field_array<'a>(filings: &'a Value, field: &str) -> Result<&'a [Value]> {
    object_get(filings, field)
        .and_then(as_array)
        .ok_or_else(|| anyhow!("missing array field {field}"))
}

fn object_get<'a>(value: &'a Value, key: &str) -> Option<&'a Value> {
    match value {
        Value::Object(object) => object.get(key),
        _ => None,
    }
}

fn as_array(value: &Value) -> Option<&[Value]> {
    match value {
        Value::Array(values) => Some(values.as_slice()),
        _ => None,
    }
}

fn write_csv_value<W>(writer: &mut W, value: &Value) -> Result<()>
where
    W: Write,
{
    match value {
        Value::Null => {}
        Value::Bool(value) => {
            writer.write_all(if *value {
                &b"true"[..]
            } else {
                &b"false"[..]
            })?
        }
        Value::Number(value) => write!(writer, "{value}")?,
        Value::String(value) => write_csv_str(writer, value)?,
        Value::Array(_) | Value::Object(_) => write_csv_str(writer, &value.to_string())?,
    }
    Ok(())
}

fn write_csv_str<W>(writer: &mut W, value: &str) -> Result<()>
where
    W: Write,
{
    let bytes = value.as_bytes();
    if !bytes
        .iter()
        .any(|byte| matches!(byte, b',' | b'"' | b'\n' | b'\r'))
    {
        writer.write_all(bytes)?;
        return Ok(());
    }

    writer.write_all(b"\"")?;
    let mut start = 0;
    for (index, byte) in bytes.iter().enumerate() {
        if *byte == b'"' {
            writer.write_all(&bytes[start..index])?;
            writer.write_all(b"\"\"")?;
            start = index + 1;
        }
    }
    writer.write_all(&bytes[start..])?;
    writer.write_all(b"\"")?;
    Ok(())
}

fn column_names(columns: &[ColumnSpec]) -> Vec<&str> {
    columns.iter().map(|column| column.name.as_str()).collect()
}

impl CsvChunkStats {
    fn add_file(&mut self, stats: SubmissionFileStats) {
        self.rows += stats.rows;
        self.files += 1;
        self.json_bytes += stats.json_bytes;
        self.read_elapsed += stats.read_elapsed;
        self.parse_elapsed += stats.parse_elapsed;
        self.write_elapsed += stats.write_elapsed;
    }
}
