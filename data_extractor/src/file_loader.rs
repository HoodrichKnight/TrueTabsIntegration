use anyhow::{Result, anyhow};
use std::path::Path;
use rust_xlsxwriter::{Workbook, XlsxError};
use crate::db::ExtractedData;

pub fn read_csv<P: AsRef<Path>>(file_path: P, expected_headers: Option<Vec<String>>) -> Result<ExtractedData> {
    println!("Чтение CSV файла: {}", file_path.as_ref().display());
    let mut reader = csv::Reader::from_path(file_path)?;

    let actual_headers: Vec<String> = reader.headers()?.iter().map(|h| h.to_string()).collect();

    if let Some(mut expected) = expected_headers {
        expected.sort();
        let mut actual_sorted = actual_headers.clone();
        actual_sorted.sort();

        if actual_sorted != expected {
            let expected_str = expected.join(", ");
            let actual_str = actual_headers.join(", ");
            let error_msg = format!("Column mismatch: Expected [{}], Got [{}]", expected_str, actual_str);
            return Err(anyhow!(error_msg));
        }
    }

    let headers = actual_headers;
    let mut data_rows: Vec<Vec<String>> = Vec::new();

    for result in reader.records() {
        let record = result?;
        let row: Vec<String> = record.iter().map(|field| field.to_string()).collect();
        data_rows.push(row);
    }

    println!("Извлечено {} строк из CSV файла.", data_rows.len());

    Ok(ExtractedData { headers, rows: data_rows })
}

pub fn write_excel<P: AsRef<Path>>(data: &ExtractedData, file_path: P) -> Result<(), XlsxError> {
    println!("Сохранение в XLSX файл: {}", file_path.as_ref().display());
    let mut workbook = Workbook::new();
    let worksheet = workbook.add_worksheet();

    for (col_num, header) in data.headers.iter().enumerate() {
        worksheet.write_string(0, col_num as u16, header)?;
    }

    for (row_num, row_data) in data.rows.iter().enumerate() {
        for (col_num, cell_data) in row_data.iter().enumerate() {
            worksheet.write(row_num as u32 + 1, col_num as u16, cell_data)?;
        }
    }

    workbook.save(file_path)?;
    println!("XLSX файл успешно сохранен.");

    Ok(())
}