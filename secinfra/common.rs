/// A single SEC filing.
#[derive(Debug, Clone)]
pub struct Submission {
    pub accession: u64,
    pub submission_type: String,
    pub ciks: Vec<u64>,
    pub filing_date: String,
}