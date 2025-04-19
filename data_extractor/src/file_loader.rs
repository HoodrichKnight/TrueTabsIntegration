use anyhow::{Result, anyhow}; // Исправлена опечатка anyry на anyhow
use std::path::Path;
use calamine::{open_workbook_auto, Reader, Data, DataType}; // Исправлен импорт Data и добавлен DataType
use rust_xlsxwriter::{Workbook, Format, XlsxError}; // Убрана неиспользуемая FormatAlign

use super::ExtractedData;

// Чтение из Excel или ODS файла
pub fn read_excel<P: AsRef<Path>>(file_path: P) -> Result<ExtractedData> {
    println!("Чтение Excel/ODS файла: {}", file_path.as_ref().display());
    let mut workbook = open_workbook_auto(file_path)?;

    // Читаем первый лист
    let sheet_name = workbook.sheet_names().get(0)
        .ok_or_else(|| anyhow!("Файл не содержит листов"))?.clone();

    let range = workbook.worksheet_range(&sheet_name)
        .ok_or_else(|| anyhow!("Не удалось прочитать лист '{}'", sheet_name))??; // Исправлена опечатка anyry!

    let mut rows = range.rows();

    let headers: Vec<String> = rows.next()
        .ok_or_else(|| anyhow!("Лист '{}' пустой или не содержит заголовков", sheet_name))?
        .iter()
        .map(|cell| cell.to_string())
        .collect();

    let data_rows: Vec<Vec<String>> = rows
        .map(|row| {
            row.iter()
                .map(|cell| match cell {
                    // calamine::Data (или DataType) может быть разных вариантов
                    Data::Empty => "".to_string(),
                    Data::String(s) => s.clone(),
                    Data::Int(i) => i.to_string(),
                    Data::Float(f) => f.to_string(),
                    Data::Bool(b) => b.to_string(),
                    Data::DateTime(d) => d.to_string(), // Или форматировать дату иначе
                    Data::Duration(dur) => dur.to_string(),
                    Data::Error(e) => format!("ERROR: {:?}", e),
                    Data::DateTimeIso(d) => d.clone(),
                    Data::DurationIso(d) => d.clone(),
                    Data::Time(t) => t.to_string(), // Или форматировать время иначе
                    Data::TimeIso(t) => t.clone(),
                    // Добавьте другие варианты DataType по мере необходимости
                    // Data::Decimal(d) => d.to_string(), // Пример для Decimal, если Calamine поддерживает
                    _ => cell.to_string(), // Fallback к общему to_string() для других типов
                })
                .collect()
        })
        .collect();

    println!("Извлечено {} строк из листа '{}'.", data_rows.len(), sheet_name);

    Ok(ExtractedData { headers, rows: data_rows })
}

// Чтение из CSV файла
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

// Запись в Excel файл
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
            // rust_xlsxwriter пытается определить тип данных
            // Если нужно явное управление типами, можно использовать write_number, write_string и т.д.
            worksheet.write(row_num as u32 + 1, col_num as u16, cell_data)?;
        }
    }

    workbook.save(file_path)?;
    println!("XLSX файл успешно сохранен.");

    Ok(())
}