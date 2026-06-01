use std::fs::File;
use std::io::{Cursor, Read, Seek};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow};
use serde_json::Value;
use tracing::warn;
use zip::ZipArchive;

use crate::common::sec_user_agent;

const SUBMISSIONS_ZIP_URL: &str =
    "https://www.sec.gov/Archives/edgar/daily-index/bulkdata/submissions.zip";
const CSV_HEADER: [&str; 4] = ["cik", "accessionNumber", "filingDate", "form"];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConstructSubmissionsDataStats {
    pub files_processed: usize,
    pub files_skipped: usize,
    pub filings_written: usize,
}

pub async fn construct_submissions_data(
    output_path: impl Into<PathBuf>,
    submissions_zip_path: Option<PathBuf>,
) -> Result<ConstructSubmissionsDataStats> {
    let output_path = output_path.into();

    if let Some(submissions_zip_path) = submissions_zip_path {
        return tokio::task::spawn_blocking(move || {
            construct_submissions_data_from_zip(output_path, submissions_zip_path)
        })
        .await?;
    }

    let zip_bytes = download_submissions_zip().await?;
    tokio::task::spawn_blocking(move || {
        let cursor = Cursor::new(zip_bytes);
        write_submissions_csv(cursor, output_path)
    })
    .await?
}

pub fn construct_submissions_data_from_zip(
    output_path: impl AsRef<Path>,
    submissions_zip_path: impl AsRef<Path>,
) -> Result<ConstructSubmissionsDataStats> {
    let file = File::open(submissions_zip_path.as_ref()).with_context(|| {
        format!(
            "failed to open submissions zip {}",
            submissions_zip_path.as_ref().display()
        )
    })?;

    write_submissions_csv(file, output_path)
}

async fn download_submissions_zip() -> Result<Vec<u8>> {
    let client = reqwest::Client::builder()
        .user_agent(sec_user_agent())
        .build()
        .context("failed to build SEC HTTP client")?;

    let response = client
        .get(SUBMISSIONS_ZIP_URL)
        .send()
        .await
        .context("failed to download SEC submissions zip")?
        .error_for_status()
        .context("SEC submissions zip request failed")?;

    Ok(response
        .bytes()
        .await
        .context("failed to read SEC submissions zip response")?
        .to_vec())
}

fn write_submissions_csv<R>(
    submissions_zip: R,
    output_path: impl AsRef<Path>,
) -> Result<ConstructSubmissionsDataStats>
where
    R: Read + Seek,
{
    let mut archive =
        ZipArchive::new(submissions_zip).context("failed to open submissions zip archive")?;
    let mut writer = csv::Writer::from_path(output_path.as_ref()).with_context(|| {
        format!(
            "failed to create submissions CSV {}",
            output_path.as_ref().display()
        )
    })?;

    writer
        .write_record(CSV_HEADER)
        .context("failed to write submissions CSV header")?;

    let mut stats = ConstructSubmissionsDataStats {
        files_processed: 0,
        files_skipped: 0,
        filings_written: 0,
    };

    for index in 0..archive.len() {
        let mut file = match archive.by_index(index) {
            Ok(file) => file,
            Err(error) => {
                stats.files_skipped += 1;
                warn!(%index, %error, "Skipping unreadable submissions zip entry");
                continue;
            }
        };

        let filename = file.name().to_string();
        if !filename.starts_with("CIK") {
            continue;
        }

        match process_submission_file(&filename, &mut file) {
            Ok(records) => {
                for record in records {
                    writer
                        .write_record(record)
                        .context("failed to write submissions CSV record")?;
                    stats.filings_written += 1;
                }
                stats.files_processed += 1;
            }
            Err(error) => {
                stats.files_skipped += 1;
                warn!(filename, %error, "Skipping submissions file");
            }
        }
    }

    writer
        .flush()
        .context("failed to flush submissions CSV writer")?;
    Ok(stats)
}

fn process_submission_file<R>(filename: &str, reader: R) -> Result<Vec<[String; 4]>>
where
    R: Read,
{
    let cik = cik_from_filename(filename)?;
    let data: Value = serde_json::from_reader(reader)
        .with_context(|| format!("failed to parse submissions JSON {filename}"))?;
    let filings = filings_data(filename, &data)?;

    let accessions = field_array(filings, "accessionNumber")?;
    let filing_dates = field_array(filings, "filingDate")?;
    let forms = field_array(filings, "form")?;

    if filing_dates.len() < accessions.len() || forms.len() < accessions.len() {
        return Err(anyhow!("submissions fields have mismatched lengths"));
    }

    let mut records = Vec::with_capacity(accessions.len());
    for index in 0..accessions.len() {
        records.push([
            cik.clone(),
            csv_value(&accessions[index]),
            csv_value(&filing_dates[index]),
            csv_value(&forms[index]),
        ]);
    }

    Ok(records)
}

fn cik_from_filename(filename: &str) -> Result<String> {
    let cik = filename
        .split('.')
        .next()
        .and_then(|stem| stem.split('-').next())
        .and_then(|stem| stem.strip_prefix("CIK"))
        .ok_or_else(|| anyhow!("filename does not start with CIK: {filename}"))?;

    let cik = cik
        .parse::<u64>()
        .with_context(|| format!("failed to parse CIK from {filename}"))?;
    Ok(cik.to_string())
}

fn filings_data<'a>(filename: &str, data: &'a Value) -> Result<&'a Value> {
    if filename.contains("submissions") {
        return Ok(data);
    }

    data.get("filings")
        .and_then(|filings| filings.get("recent"))
        .ok_or_else(|| anyhow!("missing filings.recent"))
}

fn field_array<'a>(filings: &'a Value, field: &str) -> Result<&'a [Value]> {
    filings
        .get(field)
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .ok_or_else(|| anyhow!("missing array field {field}"))
}

fn csv_value(value: &Value) -> String {
    match value {
        Value::Null => String::new(),
        Value::String(value) => value.clone(),
        Value::Bool(value) => value.to_string(),
        Value::Number(value) => value.to_string(),
        Value::Array(_) | Value::Object(_) => value.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use zip::write::SimpleFileOptions;

    use super::*;

    #[test]
    fn writes_sec_field_names_without_mapping() {
        let mut zip_bytes = Cursor::new(Vec::new());
        {
            let mut zip = zip::ZipWriter::new(&mut zip_bytes);
            zip.start_file("CIK0000320193.json", SimpleFileOptions::default())
                .unwrap();
            zip.write_all(
                br#"{
                    "filings": {
                        "recent": {
                            "accessionNumber": ["0000320193-24-000123"],
                            "filingDate": ["2024-10-31"],
                            "form": ["10-Q"]
                        }
                    }
                }"#,
            )
            .unwrap();
            zip.finish().unwrap();
        }

        let output = Cursor::new(Vec::new());
        let output = write_submissions_csv_to_writer(Cursor::new(zip_bytes.into_inner()), output)
            .unwrap()
            .into_inner()
            .unwrap()
            .into_inner();
        let csv = String::from_utf8(output).unwrap();

        assert_eq!(
            csv,
            "cik,accessionNumber,filingDate,form\n320193,0000320193-24-000123,2024-10-31,10-Q\n"
        );
    }

    #[test]
    fn reads_submission_shard_shape() {
        let mut zip_bytes = Cursor::new(Vec::new());
        {
            let mut zip = zip::ZipWriter::new(&mut zip_bytes);
            zip.start_file(
                "CIK0000000001-submissions-001.json",
                SimpleFileOptions::default(),
            )
            .unwrap();
            zip.write_all(
                br#"{
                    "accessionNumber": ["0000000001-24-000001"],
                    "filingDate": ["2024-01-02"],
                    "form": ["8-K"]
                }"#,
            )
            .unwrap();
            zip.finish().unwrap();
        }

        let output = Cursor::new(Vec::new());
        let output = write_submissions_csv_to_writer(Cursor::new(zip_bytes.into_inner()), output)
            .unwrap()
            .into_inner()
            .unwrap()
            .into_inner();
        let csv = String::from_utf8(output).unwrap();

        assert_eq!(
            csv,
            "cik,accessionNumber,filingDate,form\n1,0000000001-24-000001,2024-01-02,8-K\n"
        );
    }

    fn write_submissions_csv_to_writer<R, W>(
        submissions_zip: R,
        output: W,
    ) -> Result<csv::Writer<W>>
    where
        R: Read + Seek,
        W: Write,
    {
        let mut archive =
            ZipArchive::new(submissions_zip).context("failed to open submissions zip archive")?;
        let mut writer = csv::Writer::from_writer(output);
        writer
            .write_record(CSV_HEADER)
            .context("failed to write submissions CSV header")?;

        for index in 0..archive.len() {
            let mut file = archive.by_index(index)?;
            let filename = file.name().to_string();
            if !filename.starts_with("CIK") {
                continue;
            }

            for record in process_submission_file(&filename, &mut file)? {
                writer.write_record(record)?;
            }
        }

        writer.flush()?;
        Ok(writer)
    }
}
