use anyhow::{Result, anyhow};
use std::path::Path;
// // Импорты для Calamine (Excel/ODS) - временно отключены
// use calamine::{open_workbook_auto, Reader, Data, DataType};
use rust_xlsxwriter::{Workbook, Format, XlsxError}; // Убедитесь, что Format используется, иначе удалите

use crate::db::ExtractedData;

pub fn read_csv<P: AsRef<Path>>(file_path: P) -> Result<ExtractedData> {
    println!("Чтение CSV файла: {}", file_path.as_ref().display());
    let mut reader = csv::Reader::from_path(file_path)?;

    let headers: Vec<String> = reader.headers()?.iter().map(|h| h.to_string()).collect();
    let mut data_rows: Vec<Vec<String>> = Vec::new();

    for result in reader.records() {
        let record = result?;
        let row: Vec<String> = record.iter().map(|field| field.to_string()).collect();
        data_rows.push(row);
    }

    println!("Извлечено {} строк из CSV файла.", data_rows.len());

    Ok(ExtractedData { headers, rows: data_rows })
}

// Запись в Excel файл - Оставляем этот код активным (он использует rust_xlsxwriter)
pub fn write_excel<P: AsRef<Path>>(data: &ExtractedData, file_path: P) -> Result<(), XlsxError> {
    println!("Сохранение в XLSX файл: {}", file_path.as_ref().display());
    let mut workbook = Workbook::new();
    let worksheet = workbook.add_worksheet();

    // Записываем заголовки
    for (col_num, header) in data.headers.iter().enumerate() {
        worksheet.write_string(0, col_num as u16, header)?;
    }

    // Записываем данные строк
    for (row_num, row_data) in data.rows.iter().enumerate() {
        for (col_num, cell_data) in row_data.iter().enumerate() {
            worksheet.write(row_num as u32 + 1, col_num as u16, cell_data)?;
        }
    }

    workbook.save(file_path)?;
    println!("XLSX файл успешно сохранен.");

    Ok(())
}