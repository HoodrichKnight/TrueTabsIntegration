pub mod sql;
pub mod nosql;
pub mod truetabs;

#[derive(Debug)]
pub struct ExtractedData {
    pub headers: Vec<String>,
    pub rows: Vec<Vec<String>>,
}