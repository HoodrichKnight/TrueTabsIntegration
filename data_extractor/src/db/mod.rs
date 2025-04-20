pub mod sql;
pub mod nosql;

#[derive(Debug)]
pub struct ExtractedData {
    pub headers: Vec<String>,
    pub rows: Vec<Vec<String>>,
}