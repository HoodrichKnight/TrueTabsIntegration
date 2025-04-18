use anyhow::{Result, anyhow};
use std::{fs::File, path::Path};
use csv::ReaderBuilder;
use calamine::{open_workbook_auto, Reader, Data};

use crate::db::ExtractedData;

pub fn read_csv(file_path: &str) -> Result<ExtractedData> {
    println!("Чтение данных из CSV файла: {}", file_path);
    let file = File::open(file_path)?;

    let mut reader = ReaderBuilder::new()
        .has_headers(true)
        .from_reader(file);

    let mut headers: Vec<String> = Vec::new();
    if let Some(header_result) = reader.headers().ok() {
        headers = header_result.iter().map(|h| h.to_string()).collect();
    } else {
        eprintln!("Предупреждение: Не удалось прочитать заголовки из CSV файла.");
    }

    let mut data_rows: Vec<Vec<String>> = Vec::new();
    for result in reader.records() {
        let record = result?;
        let row: Vec<String> = record.iter().map(|field| field.to_string()).collect();
        data_rows.push(row);
    }

    if headers.is_empty() && !data_rows.is_empty() {
        if let Some(first_row) = data_rows.first() {
            headers = (0..first_row.len()).map(|i| format!("Column{}", i + 1)).collect();
        }
    }

    println!("Успешно прочитано {} строк данных из CSV.", data_rows.len());

    Ok(ExtractedData { headers, rows: data_rows })
}

pub fn read_excel(file_path: &str) -> Result<ExtractedData> {
    println!("Чтение данных из Excel файла: {}", file_path);
    let path = Path::new(file_path);
    let mut workbook = open_workbook_auto(path)?;

    let sheet_name = workbook.sheet_names().get(0).ok_or_else(|| anyhow::anyhow!("В Excel файле нет листов"))?.clone();

    let range = workbook.worksheet_range(&sheet_name)
        .ok_or_else(|| anyhow::anyhow!("Не удалось прочитать лист '{}'", sheet_name))?
        .map_err(|e| anyhow!("Ошибка чтения данных из листа '{}': {}", sheet_name, e))?;

    let mut headers: Vec<String> = Vec::new();
    let mut data_rows: Vec<Vec<String>> = Vec::new();
    let mut is_first_row = true;

    for row in range.rows() {
        let row_values: Vec<String> = row.iter().map(|cell| {
            match cell {
                Data::Empty => "".to_string(),
                Data::String(s) => s.clone(),
                Data::Int(i) => i.to_string(),
                Data::Float(f) => f.to_string(),
                Data::Bool(b) => b.to_string(),
                Data::Error(e) => format!("ERROR: {:?}", e),
                Data::DateTime(d) => d.to_string(),
                Data::Duration(d) => d.to_string(),
            }
        }).collect();

        if is_first_row {
            headers = row_values;
            is_first_row = false;
        } else {
            data_rows.push(row_values);
        }
    }

    if headers.is_empty() && !data_rows.is_empty() {
        if let Some(first_data_row) = data_rows.first() {
            headers = (0..first_data_row.len()).map(|i| format!("Column{}", i + 1)).collect();
        }
    }

    println!("Успешно прочитано {} строк данных из Excel.", data_rows.len());

    Ok(ExtractedData { headers, rows: data_rows })
}