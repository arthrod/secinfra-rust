use quick_xml::Reader;
use quick_xml::events::Event;
use reqwest::Client;
use tracing::{debug, error, warn};

use crate::common::{Submission, SubmissionSource};
use crate::format_accession::format_accession_str;
use crate::rate_limiter::RateLimiter;

const RSS_URL: &str =
    "https://www.sec.gov/cgi-bin/browse-edgar?count=100&action=getcurrent&output=rss";

fn xml_preview(xml: &str) -> String {
    String::from_utf8_lossy(&xml.as_bytes()[..xml.len().min(1000)]).into_owned()
}

fn parse_rss(xml: &str) -> Vec<Submission> {
    let mut submissions = Vec::new();
    let detected_time = chrono::Utc::now();
    let mut reader = Reader::from_str(xml);
    reader.config_mut().trim_text(true);
    let mut buf = Vec::new();

    let mut current_accession: Option<u64> = None;
    let mut current_cik: Option<u64> = None;
    let mut current_type = String::new();
    let mut current_date = String::new();
    let mut in_summary = false;
    let mut in_title = false;
    let mut summary_text = String::new();

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(e)) | Ok(Event::Empty(e)) => match e.name().as_ref() {
                b"category" => {
                    if let Some(term) = e.attributes().flatten().find(|a| a.key.as_ref() == b"term")
                    {
                        current_type = String::from_utf8_lossy(&term.value).into_owned();
                    }
                }
                b"summary" => {
                    in_summary = true;
                    summary_text.clear();
                }
                b"title" => in_title = true,
                _ => {}
            },
            Ok(Event::Text(e)) => {
                let text = e.unescape().unwrap_or_default();
                if in_summary {
                    summary_text.push_str(&text);
                } else if in_title {
                    // "10-K/A - NutriBand Inc. (0001676047) (Filer)"
                    // CIK is in the second-to-last parenthesised group
                    let parens: Vec<&str> = text
                        .split('(')
                        .filter_map(|s| s.split(')').next())
                        .collect();
                    if parens.len() >= 2 {
                        current_cik = parens[parens.len() - 2].parse::<u64>().ok();
                    }
                } else if text.starts_with("urn:tag:sec.gov") {
                    // "urn:tag:sec.gov,2008:accession-number=0001213900-26-058507"
                    if let Some(acc_str) = text.split('=').last() {
                        current_accession =
                            format_accession_str(acc_str, "int").parse::<u64>().ok();
                    }
                }
            }
            Ok(Event::End(e)) => {
                match e.name().as_ref() {
                    b"title" => in_title = false,
                    b"summary" => {
                        in_summary = false;
                        // "Filed:</b> 2026-05-18 <b>AccNo:..."
                        if let Some(pos) = summary_text.find("Filed:</b>") {
                            let after = summary_text[pos + 10..].trim();
                            current_date = after[..10].to_string();
                        }
                    }
                    b"entry" => {
                        if let Some(accession) = current_accession {
                            submissions.push(Submission {
                                accession,
                                submission_type: current_type.clone(),
                                ciks: current_cik.into_iter().collect(),
                                filing_date: current_date.clone(),
                                source: SubmissionSource::Rss,
                                detected_time,
                            });
                        } else {
                            debug!("Skipping RSS entry without accession");
                        }
                        current_accession = None;
                        current_cik = None;
                        current_type.clear();
                        current_date.clear();
                    }
                    _ => {}
                }
            }
            Ok(Event::Eof) => break,
            Err(e) => {
                let preview = xml_preview(xml);
                error!(error = %e, preview = %preview, "XML parse error");
                break;
            }
            _ => {}
        }
        buf.clear();
    }

    debug!(count = submissions.len(), "Parsed RSS submissions");
    submissions
}

/// Fetch and parse the SEC RSS feed, returning all current submissions.
pub async fn poll_rss(client: &Client, limiter: &RateLimiter) -> anyhow::Result<Vec<Submission>> {
    limiter.acquire().await;
    debug!(url = RSS_URL, "Polling SEC RSS feed");
    let resp = client.get(RSS_URL).send().await?;
    let status = resp.status();
    let xml = resp.text().await?;

    if status.is_success() {
        debug!(%status, bytes = xml.len(), "Fetched SEC RSS feed");
    } else {
        warn!(%status, bytes = xml.len(), "Fetched SEC RSS feed with non-success status");
    }

    Ok(parse_rss(&xml))
}
