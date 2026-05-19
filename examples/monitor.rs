use futures::StreamExt;
use secinfra::Monitor;

#[tokio::main]
async fn main() {
    // optionally enable tracing to see debug logs:
    tracing_subscriber::fmt::init();
    tracing::info!("Starting monitor example");

    let mut stream = Box::pin(
        Monitor::new()
            .polling_interval_ms(200)
            .use_efts(true)
            .use_rss(true)
            .build(),
    );
    while let Some(batch) = stream.next().await {
        for submission in batch {
            tracing::info!(
                filing_date = %submission.filing_date,
                submission_type = %submission.submission_type,
                ciks = ?submission.ciks,
                source = ?submission.source,
                detected_time = %submission.detected_time,
                "New submission"
            );
        }
    }
}
