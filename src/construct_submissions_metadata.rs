use std::collections::HashSet;
use std::fs::File;
use std::io::{Cursor, Read, Seek};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow};
use arrow_array::builder::{BooleanBuilder, StringBuilder, UInt64Builder};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
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
const STRING_VALUE_BYTES_PER_ROW: usize = 32;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConstructSubmissionsMetadataStats {
    pub files_processed: usize,
    pub files_skipped: usize,
    pub filings_written: usize,
    pub batches_written: usize,
}

struct SubmissionBatchBuffer {
    ciks: Vec<u64>,
    columns: Vec<ColumnBuffer>,
    files: usize,
    json_bytes: usize,
    read_elapsed: Duration,
    parse_elapsed: Duration,
}

#[derive(Clone)]
struct ColumnSpec {
    name: String,
    kind: ColumnKind,
}

#[derive(Clone, Copy)]
enum ColumnKind {
    Utf8,
    UInt64,
    Boolean,
}

enum ColumnBuffer {
    Utf8(StringBuilder),
    UInt64(UInt64Builder),
    Boolean(BooleanBuilder),
}

pub async fn construct_submissions_metadata(
    output_path: impl Into<PathBuf>,
    submissions_zip_path: Option<PathBuf>,
    columns: Option<Vec<String>>,
) -> Result<ConstructSubmissionsMetadataStats> {
    let output_path = output_path.into();
    let columns = resolve_columns(columns)?;

    if let Some(submissions_zip_path) = submissions_zip_path {
        return tokio::task::spawn_blocking(move || {
            let file = File::open(&submissions_zip_path).with_context(|| {
                format!(
                    "failed to open submissions zip {}",
                    submissions_zip_path.display()
                )
            })?;
            write_submissions_metadata_parquet(file, output_path, columns)
        })
        .await?;
    }

    let zip_bytes = download_submissions_zip().await?;
    tokio::task::spawn_blocking(move || {
        write_submissions_metadata_parquet(Cursor::new(zip_bytes), output_path, columns)
    })
    .await?
}

pub fn construct_submissions_metadata_from_zip(
    output_path: impl AsRef<Path>,
    submissions_zip_path: impl AsRef<Path>,
    columns: Option<Vec<String>>,
) -> Result<ConstructSubmissionsMetadataStats> {
    let columns = resolve_columns(columns)?;
    let file = File::open(submissions_zip_path.as_ref()).with_context(|| {
        format!(
            "failed to open submissions zip {}",
            submissions_zip_path.as_ref().display()
        )
    })?;

    write_submissions_metadata_parquet(file, output_path, columns)
}

async fn download_submissions_zip() -> Result<Vec<u8>> {
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
    info!(bytes = bytes.len(), "Downloaded SEC submissions zip");
    Ok(bytes)
}

fn write_submissions_metadata_parquet<R>(
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
    let schema = submissions_schema(&columns);
    let output = File::create(output_path.as_ref()).with_context(|| {
        format!(
            "failed to create submissions metadata parquet {}",
            output_path.as_ref().display()
        )
    })?;
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();
    let mut writer = ArrowWriter::try_new(output, schema.clone(), Some(props))
        .context("failed to create parquet writer")?;
    let mut buffer = SubmissionBatchBuffer::new(&columns);
    let mut stats = ConstructSubmissionsMetadataStats {
        files_processed: 0,
        files_skipped: 0,
        filings_written: 0,
        batches_written: 0,
    };

    info!(
        zip_entries = archive.len(),
        columns = ?column_names(&columns),
        batch_target_rows = BATCH_TARGET_ROWS,
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

        let rows = append_submission_file(&filename, &mut file, &columns, &mut buffer)
            .with_context(|| format!("failed to process submissions metadata file {filename}"))?;
        stats.files_processed += 1;
        stats.filings_written += rows;

        if buffer.len() >= BATCH_TARGET_ROWS {
            flush_batch(&mut writer, schema.clone(), &mut buffer, &mut stats)?;
        }
    }

    flush_batch(&mut writer, schema, &mut buffer, &mut stats)?;
    writer
        .close()
        .context("failed to close submissions metadata parquet writer")?;

    info!(
        files_processed = stats.files_processed,
        files_skipped = stats.files_skipped,
        filings_written = stats.filings_written,
        batches_written = stats.batches_written,
        total_ms = started_at.elapsed().as_millis(),
        "Finished constructing submissions metadata"
    );

    Ok(stats)
}

fn append_submission_file<R>(
    filename: &str,
    mut reader: R,
    columns: &[ColumnSpec],
    buffer: &mut SubmissionBatchBuffer,
) -> Result<usize>
where
    R: Read,
{
    let read_started_at = Instant::now();
    let mut bytes = Vec::new();
    reader
        .read_to_end(&mut bytes)
        .with_context(|| format!("failed to read submissions JSON {filename}"))?;
    let json_bytes = bytes.len();
    let read_elapsed = read_started_at.elapsed();

    append_submission_bytes(
        filename,
        &bytes,
        json_bytes,
        read_elapsed,
        columns,
        buffer,
    )
}

fn append_submission_bytes(
    filename: &str,
    bytes: &[u8],
    json_bytes: usize,
    read_elapsed: Duration,
    columns: &[ColumnSpec],
    buffer: &mut SubmissionBatchBuffer,
) -> Result<usize> {
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

    buffer.reserve(row_count);
    for index in 0..row_count {
        buffer.ciks.push(cik);
        for (column_index, values) in column_arrays.iter().enumerate() {
            buffer.columns[column_index].push(&values[index]);
        }
    }
    buffer.files += 1;
    buffer.json_bytes += json_bytes;
    buffer.read_elapsed += read_elapsed;
    buffer.parse_elapsed += parse_started_at.elapsed();

    Ok(row_count)
}

fn flush_batch<W>(
    writer: &mut ArrowWriter<W>,
    schema: SchemaRef,
    buffer: &mut SubmissionBatchBuffer,
    stats: &mut ConstructSubmissionsMetadataStats,
) -> Result<()>
where
    W: std::io::Write + Send,
{
    if buffer.is_empty() {
        return Ok(());
    }

    let batch_rows = buffer.len();
    let batch_files = buffer.files;
    let batch_json_bytes = buffer.json_bytes;
    let batch_read_ms = buffer.read_elapsed.as_millis();
    let batch_parse_ms = buffer.parse_elapsed.as_millis();

    let build_started_at = Instant::now();
    let batch = buffer.take().into_record_batch(schema)?;
    let batch_build_ms = build_started_at.elapsed().as_millis();

    let write_started_at = Instant::now();
    writer
        .write(&batch)
        .context("failed to write submissions metadata parquet batch")?;
    let parquet_write_ms = write_started_at.elapsed().as_millis();

    stats.batches_written += 1;
    info!(
        batch_index = stats.batches_written,
        batch_rows,
        batch_files,
        batch_json_bytes,
        total_rows = stats.filings_written,
        files_processed = stats.files_processed,
        files_skipped = stats.files_skipped,
        read_ms = batch_read_ms,
        parse_ms = batch_parse_ms,
        batch_build_ms,
        parquet_write_ms,
        "Wrote submissions metadata parquet batch"
    );
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
            kind: column_kind(column),
        });
    }

    Ok(resolved)
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

fn append_metadata_value_string(builder: &mut StringBuilder, value: &Value) {
    match value {
        Value::Null => builder.append_value(""),
        Value::Bool(value) => builder.append_value(if *value { "true" } else { "false" }),
        Value::Number(value) => builder.append_value(value.to_string()),
        Value::String(value) => builder.append_value(value),
        Value::Array(_) | Value::Object(_) => builder.append_value(value.to_string()),
    }
}

fn metadata_value_u64(value: &Value) -> u64 {
    match value {
        Value::Number(value) => value
            .as_u64()
            .or_else(|| value.as_i64().and_then(|value| value.try_into().ok()))
            .or_else(|| value.as_f64().map(|value| value as u64))
            .unwrap_or(0),
        Value::Bool(value) => u64::from(*value),
        Value::String(value) => value.parse().unwrap_or(0),
        Value::Null | Value::Array(_) | Value::Object(_) => 0,
    }
}

fn metadata_value_bool(value: &Value) -> bool {
    match value {
        Value::Bool(value) => *value,
        Value::Number(value) => value
            .as_i64()
            .map(|value| value != 0)
            .or_else(|| value.as_u64().map(|value| value != 0))
            .or_else(|| value.as_f64().map(|value| value != 0.0))
            .unwrap_or(false),
        Value::String(value) => matches!(value.as_str(), "1" | "true" | "TRUE" | "True"),
        Value::Null | Value::Array(_) | Value::Object(_) => false,
    }
}

fn column_kind(column: &str) -> ColumnKind {
    match column {
        "size" => ColumnKind::UInt64,
        "isXBRL" | "isInlineXBRL" | "isXBRLNumeric" => ColumnKind::Boolean,
        _ => ColumnKind::Utf8,
    }
}

fn column_names(columns: &[ColumnSpec]) -> Vec<&str> {
    columns.iter().map(|column| column.name.as_str()).collect()
}

fn submissions_schema(columns: &[ColumnSpec]) -> SchemaRef {
    let mut fields = Vec::with_capacity(columns.len() + 1);
    fields.push(Field::new("cik", DataType::UInt64, false));
    fields.extend(columns.iter().map(|column| {
        Field::new(
            &column.name,
            match column.kind {
                ColumnKind::Utf8 => DataType::Utf8,
                ColumnKind::UInt64 => DataType::UInt64,
                ColumnKind::Boolean => DataType::Boolean,
            },
            false,
        )
    }));
    Arc::new(Schema::new(fields))
}

impl SubmissionBatchBuffer {
    fn new(columns: &[ColumnSpec]) -> Self {
        Self {
            ciks: Vec::with_capacity(BATCH_TARGET_ROWS),
            columns: columns
                .iter()
                .map(|column| ColumnBuffer::new(column.kind))
                .collect(),
            files: 0,
            json_bytes: 0,
            read_elapsed: Duration::ZERO,
            parse_elapsed: Duration::ZERO,
        }
    }

    fn len(&self) -> usize {
        self.ciks.len()
    }

    fn is_empty(&self) -> bool {
        self.ciks.is_empty()
    }

    fn reserve(&mut self, rows: usize) {
        self.ciks.reserve(rows);
        for column in &mut self.columns {
            column.reserve(rows);
        }
    }

    fn take(&mut self) -> Self {
        Self {
            ciks: std::mem::take(&mut self.ciks),
            columns: self.columns.iter_mut().map(ColumnBuffer::take).collect(),
            files: std::mem::take(&mut self.files),
            json_bytes: std::mem::take(&mut self.json_bytes),
            read_elapsed: std::mem::take(&mut self.read_elapsed),
            parse_elapsed: std::mem::take(&mut self.parse_elapsed),
        }
    }

    fn into_record_batch(self, schema: SchemaRef) -> Result<RecordBatch> {
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(self.columns.len() + 1);
        let mut cik_builder = UInt64Builder::with_capacity(self.ciks.len());
        for cik in self.ciks {
            cik_builder.append_value(cik);
        }
        arrays.push(Arc::new(cik_builder.finish()));
        arrays.extend(self.columns.into_iter().map(ColumnBuffer::into_array));

        RecordBatch::try_new(schema, arrays)
            .context("failed to build submissions metadata record batch")
    }
}

impl ColumnBuffer {
    fn new(kind: ColumnKind) -> Self {
        match kind {
            ColumnKind::Utf8 => Self::Utf8(string_builder()),
            ColumnKind::UInt64 => Self::UInt64(UInt64Builder::with_capacity(BATCH_TARGET_ROWS)),
            ColumnKind::Boolean => Self::Boolean(BooleanBuilder::with_capacity(BATCH_TARGET_ROWS)),
        }
    }

    fn reserve(&mut self, rows: usize) {
        match self {
            Self::Utf8(_) | Self::UInt64(_) | Self::Boolean(_) => {
                let _ = rows;
            }
        }
    }

    fn push(&mut self, value: &Value) {
        match self {
            Self::Utf8(values) => append_metadata_value_string(values, value),
            Self::UInt64(values) => values.append_value(metadata_value_u64(value)),
            Self::Boolean(values) => values.append_value(metadata_value_bool(value)),
        }
    }

    fn into_array(mut self) -> ArrayRef {
        match &mut self {
            Self::Utf8(values) => Arc::new(values.finish()),
            Self::UInt64(values) => Arc::new(values.finish()),
            Self::Boolean(values) => Arc::new(values.finish()),
        }
    }

    fn take(&mut self) -> Self {
        match self {
            Self::Utf8(values) => {
                let old = std::mem::replace(values, string_builder());
                Self::Utf8(old)
            }
            Self::UInt64(values) => {
                let old =
                    std::mem::replace(values, UInt64Builder::with_capacity(BATCH_TARGET_ROWS));
                Self::UInt64(old)
            }
            Self::Boolean(values) => {
                let old =
                    std::mem::replace(values, BooleanBuilder::with_capacity(BATCH_TARGET_ROWS));
                Self::Boolean(old)
            }
        }
    }
}

fn string_builder() -> StringBuilder {
    StringBuilder::with_capacity(
        BATCH_TARGET_ROWS,
        BATCH_TARGET_ROWS * STRING_VALUE_BYTES_PER_ROW,
    )
}
