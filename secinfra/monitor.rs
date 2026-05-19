use std::sync::Arc;
use std::time::{Duration, Instant};

use async_stream::stream;
use chrono::NaiveDate;
use futures::{Stream, StreamExt};
use indexmap::IndexSet;
use reqwest::Client;
use tracing::debug;

use crate::common::{RateLimiter, Submission};
use crate::efts::{backfill_stream, fetch_date};
use crate::rss::poll_rss;

const MAX_ACCESSIONS: usize = 50_000;

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

pub struct Monitor {
    polling_interval_ms: u64,
    validation_interval_ms: u64,
    start_date: Option<NaiveDate>,
    use_rss: bool,
    use_efts: bool,
}

impl Default for Monitor {
    fn default() -> Self {
        Self {
            polling_interval_ms: 200,
            validation_interval_ms: 60_000,
            start_date: None,
            use_rss: true,
            use_efts: true,
        }
    }
}

impl Monitor {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn polling_interval_ms(mut self, ms: u64) -> Self {
        self.polling_interval_ms = ms;
        self
    }

    pub fn validation_interval_ms(mut self, ms: u64) -> Self {
        self.validation_interval_ms = ms;
        self
    }

    pub fn start_date(mut self, date: NaiveDate) -> Self {
        self.start_date = Some(date);
        self
    }

    pub fn use_rss(mut self, val: bool) -> Self {
        self.use_rss = val;
        self
    }

    pub fn use_efts(mut self, val: bool) -> Self {
        self.use_efts = val;
        self
    }

    pub fn build(self) -> impl Stream<Item = Vec<Submission>> {
        assert!(self.use_rss || self.use_efts, "At least one of use_rss or use_efts must be true");
        monitor_submissions_stream(
            self.polling_interval_ms,
            self.validation_interval_ms,
            self.start_date,
            self.use_rss,
            self.use_efts,
        )
    }
}

// ---------------------------------------------------------------------------
// Internals
// ---------------------------------------------------------------------------

fn filter_new(batch: Vec<Submission>, accessions: &mut IndexSet<u64>) -> Vec<Submission> {
    batch
        .into_iter()
        .filter(|s| {
            if accessions.contains(&s.accession) {
                return false;
            }
            if accessions.len() == MAX_ACCESSIONS {
                accessions.shift_remove_index(0);
            }
            accessions.insert(s.accession);
            true
        })
        .collect()
}

fn monitor_submissions_stream(
    polling_interval_ms: u64,
    validation_interval_ms: u64,
    start_date: Option<NaiveDate>,
    use_rss: bool,
    use_efts: bool,
) -> impl Stream<Item = Vec<Submission>> {
    stream! {
        let client = Arc::new(
            Client::builder()
                .user_agent("Mozilla/5.0")
                .build()
                .expect("Failed to build HTTP client"),
        );

        let limiter = Arc::new(RateLimiter::new(polling_interval_ms));

        let mut accessions: IndexSet<u64> = IndexSet::with_capacity(MAX_ACCESSIONS);

        // Backfill
        if let Some(start) = start_date {
            let end = chrono::Utc::now().date_naive();
            debug!(start = %start, end = %end, "Starting backfill");
            let mut bf = backfill_stream(client.clone(), limiter.clone(), start, end);
            while let Some(batch) = bf.next().await {
                let new = filter_new(batch, &mut accessions);
                if !new.is_empty() {
                    yield new;
                }
            }
            debug!("Backfill complete");
        }

        let poll_interval = Duration::from_millis(polling_interval_ms);
        let val_interval = Duration::from_millis(validation_interval_ms);

        // Initialise as already due so first iteration fires immediately
        let mut last_poll = Instant::now().checked_sub(poll_interval).unwrap_or_else(Instant::now);
        let mut last_val = Instant::now().checked_sub(val_interval).unwrap_or_else(Instant::now);

        loop {
            let now = Instant::now();

            // EFTS validation takes priority when due — RSS pauses until complete
            if use_efts && now.duration_since(last_val) >= val_interval {
                let date = chrono::Utc::now().date_naive();
                debug!(%date, "Running EFTS validation");

                let results = fetch_date(&client, &limiter, date).await;
                let new = filter_new(results, &mut accessions);
                if !new.is_empty() {
                    debug!(count = new.len(), "New submissions via EFTS validation");
                    yield new;
                }

                last_val = Instant::now();
                continue; // re-check timers before next RSS poll
            }

            // RSS poll
            if use_rss && now.duration_since(last_poll) >= poll_interval {
                match poll_rss(&client, &limiter).await {
                    Ok(batch) => {
                        let new = filter_new(batch, &mut accessions);
                        if !new.is_empty() {
                            debug!(count = new.len(), "New submissions via RSS");
                            yield new;
                        }
                    }
                    Err(e) => tracing::error!("RSS poll error: {e}"),
                }
                last_poll = Instant::now();
            }

            // Sleep until next scheduled event
            let next = [
                use_rss.then_some(last_poll + poll_interval),
                use_efts.then_some(last_val + val_interval),
            ]
            .into_iter()
            .flatten()
            .min();

            if let Some(wake) = next {
                let now = Instant::now();
                if wake > now {
                    tokio::time::sleep(wake - now).await;
                }
            }
        }
    }
}