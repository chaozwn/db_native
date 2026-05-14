use std::{
    fs::{self, File, OpenOptions},
    io::{BufWriter, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::Arc,
    thread,
};

use arrow_save::{
    array::{ArrayRef, BooleanBuilder, Float64Builder, Int64Builder, StringBuilder},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use csv::WriterBuilder;
use napi_derive::napi;
use orc_rust::ArrowWriterBuilder as OrcArrowWriterBuilder;
use parquet_save::arrow::ArrowWriter as ParquetArrowWriter;
use rust_xlsxwriter::Workbook;
use serde_json::Value;
use tokio::sync::{mpsc, oneshot};

use crate::{
    error::{into_napi_error, state_error, DbNativeResult},
    ColumnMeta, QueryBatch, QueryResult,
};

#[napi(object)]
pub struct SaveResult {
    pub path: String,
    pub file_type: String,
    pub row_count: i64,
}

#[derive(Clone, Copy)]
enum SaveFormat {
    Csv,
    Excel,
    Json,
    Orc,
    Parquet,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum SaveMode {
    Overwrite,
    Append,
}

#[derive(Clone, Copy)]
enum InferredColumnType {
    Bool,
    Float64,
    Int64,
    Utf8,
}

pub struct StreamingSaveWriter {
    output_path: PathBuf,
    format: SaveFormat,
    row_count: i64,
    columns: Option<Vec<ColumnMeta>>,
    inner: StreamingSaveWriterInner,
}

enum StreamingSaveWriterInner {
    Csv(CsvStreamingWriter),
    Excel(ExcelStreamingWriter),
    Json(JsonStreamingWriter),
    Orc(OrcStreamingWriter),
    Parquet(ParquetStreamingWriter),
}

struct CsvStreamingWriter {
    writer: csv::Writer<BufWriter<File>>,
    headers_written: bool,
}

struct JsonStreamingWriter {
    writer: BufWriter<File>,
    wrote_any_row: bool,
}

struct ExcelStreamingWriter {
    workbook: Workbook,
    next_row_index: u32,
    headers_written: bool,
}

struct ParquetStreamingWriter {
    file: Option<File>,
    writer: Option<ParquetArrowWriter<File>>,
    inferred_types: Option<Vec<InferredColumnType>>,
}

struct OrcStreamingWriter {
    file: Option<File>,
    writer: Option<orc_rust::ArrowWriter<File>>,
    inferred_types: Option<Vec<InferredColumnType>>,
}

impl SaveFormat {
    fn parse(file_type: &str) -> DbNativeResult<Self> {
        match file_type.trim().to_ascii_lowercase().as_str() {
            "csv" => Ok(Self::Csv),
            "excel" | "xlsx" => Ok(Self::Excel),
            "json" => Ok(Self::Json),
            "orc" => Ok(Self::Orc),
            "parquet" => Ok(Self::Parquet),
            other => Err(state_error(format!(
                "Unsupported save file type: {other}, please use csv, excel, json, orc, or parquet",
            ))),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            SaveFormat::Csv => "csv",
            SaveFormat::Excel => "excel",
            SaveFormat::Json => "json",
            SaveFormat::Orc => "orc",
            SaveFormat::Parquet => "parquet",
        }
    }
}

impl SaveMode {
    fn parse(mode: Option<&str>) -> DbNativeResult<Self> {
        match mode
            .map(|value| value.trim().to_ascii_lowercase())
            .as_deref()
            .unwrap_or("overwrite")
        {
            "" | "overwrite" => Ok(Self::Overwrite),
            "append" => Ok(Self::Append),
            other => Err(state_error(format!(
                "Unsupported save mode: {other}, please use overwrite or append",
            ))),
        }
    }
}

#[allow(dead_code)]
pub fn save_query_result(
    query_result: &QueryResult,
    file_type: &str,
    path: &str,
) -> DbNativeResult<SaveResult> {
    let format = SaveFormat::parse(file_type)?;
    let output_path = PathBuf::from(path);
    ensure_parent_directory(&output_path)?;

    match format {
        SaveFormat::Csv => save_csv(query_result, &output_path)?,
        SaveFormat::Excel => save_excel(query_result, &output_path)?,
        SaveFormat::Json => save_json(query_result, &output_path)?,
        SaveFormat::Orc => save_orc(query_result, &output_path)?,
        SaveFormat::Parquet => save_parquet(query_result, &output_path)?,
    }

    Ok(SaveResult {
        path: output_path.to_string_lossy().into_owned(),
        file_type: format.as_str().to_string(),
        row_count: query_result.rows.len() as i64,
    })
}

pub fn spawn_streaming_save_worker(
    file_type: String,
    path: String,
    mode: Option<String>,
) -> (
    mpsc::Sender<DbNativeResult<QueryBatch>>,
    oneshot::Receiver<DbNativeResult<SaveResult>>,
) {
    let (batch_sender, mut batch_receiver) = mpsc::channel::<DbNativeResult<QueryBatch>>(2);
    let (result_sender, result_receiver) = oneshot::channel();

    thread::spawn(move || {
        let result = (|| -> DbNativeResult<SaveResult> {
            let mut writer = StreamingSaveWriter::new(&file_type, &path, mode.as_deref())?;

            while let Some(batch) = batch_receiver.blocking_recv() {
                let batch = batch?;
                writer.write_batch(&batch.columns, &batch.rows)?;
            }

            writer.finish()
        })();

        let _ = result_sender.send(result);
    });

    (batch_sender, result_receiver)
}

impl StreamingSaveWriter {
    pub fn new(file_type: &str, path: &str, mode: Option<&str>) -> DbNativeResult<Self> {
        let format = SaveFormat::parse(file_type)?;
        let mode = SaveMode::parse(mode)?;
        let output_path = PathBuf::from(path);
        ensure_parent_directory(&output_path)?;

        let inner = match format {
            SaveFormat::Csv => {
                StreamingSaveWriterInner::Csv(open_csv_streaming_writer(&output_path, mode)?)
            }
            SaveFormat::Excel => {
                ensure_existing_append_target_supported(&output_path, mode, "excel/xlsx")?;
                let mut workbook = Workbook::new();
                workbook.add_worksheet_with_low_memory();
                StreamingSaveWriterInner::Excel(ExcelStreamingWriter {
                    workbook,
                    next_row_index: 1,
                    headers_written: false,
                })
            }
            SaveFormat::Json => {
                StreamingSaveWriterInner::Json(open_json_streaming_writer(&output_path, mode)?)
            }
            SaveFormat::Orc => {
                ensure_existing_append_target_supported(&output_path, mode, "orc")?;
                StreamingSaveWriterInner::Orc(OrcStreamingWriter {
                    file: Some(File::create(&output_path).map_err(into_napi_error)?),
                    writer: None,
                    inferred_types: None,
                })
            }
            SaveFormat::Parquet => {
                ensure_existing_append_target_supported(&output_path, mode, "parquet")?;
                StreamingSaveWriterInner::Parquet(ParquetStreamingWriter {
                    file: Some(File::create(&output_path).map_err(into_napi_error)?),
                    writer: None,
                    inferred_types: None,
                })
            }
        };

        Ok(Self {
            output_path,
            format,
            row_count: 0,
            columns: None,
            inner,
        })
    }

    pub fn write_batch(&mut self, columns: &[ColumnMeta], rows: &[Value]) -> DbNativeResult<()> {
        self.ensure_columns(columns)?;
        let columns = self.columns.as_deref().unwrap_or(columns);

        match &mut self.inner {
            StreamingSaveWriterInner::Csv(writer) => writer.write_batch(columns, rows)?,
            StreamingSaveWriterInner::Excel(writer) => writer.write_batch(columns, rows)?,
            StreamingSaveWriterInner::Json(writer) => writer.write_batch(rows)?,
            StreamingSaveWriterInner::Orc(writer) => writer.write_batch(columns, rows)?,
            StreamingSaveWriterInner::Parquet(writer) => writer.write_batch(columns, rows)?,
        }

        self.row_count += rows.len() as i64;
        Ok(())
    }

    pub fn finish(mut self) -> DbNativeResult<SaveResult> {
        let columns = self.columns.clone().unwrap_or_default();

        match &mut self.inner {
            StreamingSaveWriterInner::Csv(writer) => writer.finish(&columns)?,
            StreamingSaveWriterInner::Excel(writer) => {
                writer.finish(&columns, &self.output_path)?
            }
            StreamingSaveWriterInner::Json(writer) => writer.finish()?,
            StreamingSaveWriterInner::Orc(writer) => writer.finish(&columns)?,
            StreamingSaveWriterInner::Parquet(writer) => writer.finish(&columns)?,
        }

        Ok(SaveResult {
            path: self.output_path.to_string_lossy().into_owned(),
            file_type: self.format.as_str().to_string(),
            row_count: self.row_count,
        })
    }

    fn ensure_columns(&mut self, columns: &[ColumnMeta]) -> DbNativeResult<()> {
        match &self.columns {
            Some(existing) => {
                if columns.is_empty() || same_columns(existing, columns) {
                    Ok(())
                } else {
                    Err(state_error(
                        "Streaming save received inconsistent column metadata",
                    ))
                }
            }
            None => {
                self.columns = Some(columns.to_vec());
                Ok(())
            }
        }
    }
}

impl CsvStreamingWriter {
    fn write_batch(&mut self, columns: &[ColumnMeta], rows: &[Value]) -> DbNativeResult<()> {
        if !self.headers_written {
            self.writer
                .write_record(columns.iter().map(|column| column.name.as_str()))
                .map_err(into_napi_error)?;
            self.headers_written = true;
        }

        for row in rows {
            let object = row.as_object();
            let record = columns
                .iter()
                .map(|column| stringify_cell(object.and_then(|value| value.get(&column.name))))
                .collect::<Vec<_>>();

            self.writer.write_record(record).map_err(into_napi_error)?;
        }

        Ok(())
    }

    fn finish(&mut self, columns: &[ColumnMeta]) -> DbNativeResult<()> {
        if !self.headers_written {
            self.writer
                .write_record(columns.iter().map(|column| column.name.as_str()))
                .map_err(into_napi_error)?;
            self.headers_written = true;
        }

        self.writer.flush().map_err(into_napi_error)
    }
}

impl JsonStreamingWriter {
    fn write_batch(&mut self, rows: &[Value]) -> DbNativeResult<()> {
        for row in rows {
            if self.wrote_any_row {
                self.writer.write_all(b",").map_err(into_napi_error)?;
            }
            serde_json::to_writer(&mut self.writer, row).map_err(into_napi_error)?;
            self.wrote_any_row = true;
        }

        Ok(())
    }

    fn finish(&mut self) -> DbNativeResult<()> {
        self.writer.write_all(b"]").map_err(into_napi_error)?;
        self.writer.flush().map_err(into_napi_error)
    }
}

impl ExcelStreamingWriter {
    fn write_batch(&mut self, columns: &[ColumnMeta], rows: &[Value]) -> DbNativeResult<()> {
        let mut next_row_index = self.next_row_index;
        let mut headers_written = self.headers_written;
        let worksheet = self
            .workbook
            .worksheet_from_index(0)
            .map_err(into_napi_error)?;

        if !headers_written {
            for (column_index, column) in columns.iter().enumerate() {
                let column_index = u16::try_from(column_index)
                    .map_err(|_| state_error("Excel column count exceeds worksheet limits"))?;
                worksheet
                    .write(0, column_index, column.name.as_str())
                    .map_err(into_napi_error)?;
            }
            headers_written = true;
        }

        for row in rows {
            let object = row.as_object();
            let row_index = next_row_index;

            for (column_index, column) in columns.iter().enumerate() {
                let column_index = u16::try_from(column_index)
                    .map_err(|_| state_error("Excel column count exceeds worksheet limits"))?;
                write_excel_cell(
                    worksheet,
                    row_index,
                    column_index,
                    object.and_then(|value| value.get(&column.name)),
                )?;
            }

            next_row_index = next_row_index
                .checked_add(1)
                .ok_or_else(|| state_error("Excel row count exceeds worksheet limits"))?;
        }

        self.headers_written = headers_written;
        self.next_row_index = next_row_index;
        Ok(())
    }

    fn finish(&mut self, columns: &[ColumnMeta], path: &Path) -> DbNativeResult<()> {
        if !self.headers_written && !columns.is_empty() {
            self.write_batch(columns, &[])?;
        }

        self.workbook.save(path).map_err(into_napi_error)
    }
}

impl ParquetStreamingWriter {
    fn write_batch(&mut self, columns: &[ColumnMeta], rows: &[Value]) -> DbNativeResult<()> {
        self.ensure_initialized(columns, rows)?;

        if rows.is_empty() {
            return Ok(());
        }

        let inferred_types = self.inferred_types.clone().unwrap_or_default();
        let batch = build_record_batch_from_parts(columns, rows, &inferred_types)?;
        self.writer
            .as_mut()
            .ok_or_else(|| state_error("Parquet writer is not initialized"))?
            .write(&batch)
            .map_err(into_napi_error)
    }

    fn finish(&mut self, columns: &[ColumnMeta]) -> DbNativeResult<()> {
        self.ensure_initialized(columns, &[])?;
        self.writer
            .take()
            .ok_or_else(|| state_error("Parquet writer is not initialized"))?
            .close()
            .map_err(into_napi_error)?;
        Ok(())
    }

    fn ensure_initialized(&mut self, columns: &[ColumnMeta], rows: &[Value]) -> DbNativeResult<()> {
        if self.writer.is_some() {
            return Ok(());
        }

        let inferred_types = infer_column_types(columns, rows);
        let schema = build_schema(columns, &inferred_types);
        let writer = ParquetArrowWriter::try_new(
            self.file
                .take()
                .ok_or_else(|| state_error("Parquet output file is unavailable"))?,
            schema,
            None,
        )
        .map_err(into_napi_error)?;

        self.inferred_types = Some(inferred_types);
        self.writer = Some(writer);
        Ok(())
    }
}

impl OrcStreamingWriter {
    fn write_batch(&mut self, columns: &[ColumnMeta], rows: &[Value]) -> DbNativeResult<()> {
        self.ensure_initialized(columns, rows)?;

        if rows.is_empty() {
            return Ok(());
        }

        let inferred_types = self.inferred_types.clone().unwrap_or_default();
        let batch = build_record_batch_from_parts(columns, rows, &inferred_types)?;
        self.writer
            .as_mut()
            .ok_or_else(|| state_error("ORC writer is not initialized"))?
            .write(&batch)
            .map_err(into_napi_error)
    }

    fn finish(&mut self, columns: &[ColumnMeta]) -> DbNativeResult<()> {
        self.ensure_initialized(columns, &[])?;
        self.writer
            .take()
            .ok_or_else(|| state_error("ORC writer is not initialized"))?
            .close()
            .map_err(into_napi_error)?;
        Ok(())
    }

    fn ensure_initialized(&mut self, columns: &[ColumnMeta], rows: &[Value]) -> DbNativeResult<()> {
        if self.writer.is_some() {
            return Ok(());
        }

        let inferred_types = infer_column_types(columns, rows);
        let schema = build_schema(columns, &inferred_types);
        let writer = OrcArrowWriterBuilder::new(
            self.file
                .take()
                .ok_or_else(|| state_error("ORC output file is unavailable"))?,
            schema,
        )
        .try_build()
        .map_err(into_napi_error)?;

        self.inferred_types = Some(inferred_types);
        self.writer = Some(writer);
        Ok(())
    }
}

fn same_columns(left: &[ColumnMeta], right: &[ColumnMeta]) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .zip(right.iter())
            .all(|(lhs, rhs)| lhs.name == rhs.name && lhs.data_type == rhs.data_type)
}

fn ensure_parent_directory(path: &Path) -> DbNativeResult<()> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent).map_err(into_napi_error)?;
        }
    }

    Ok(())
}

fn ensure_existing_append_target_supported(
    path: &Path,
    mode: SaveMode,
    format_label: &str,
) -> DbNativeResult<()> {
    if mode != SaveMode::Append || !path.exists() {
        return Ok(());
    }

    let metadata = fs::metadata(path).map_err(into_napi_error)?;
    if metadata.len() == 0 {
        return Ok(());
    }

    Err(state_error(format!(
        "Append mode is not supported for {format_label} exports. Please use overwrite, or choose csv/json when appending to an existing file.",
    )))
}

fn open_csv_streaming_writer(path: &Path, mode: SaveMode) -> DbNativeResult<CsvStreamingWriter> {
    let headers_written = if mode == SaveMode::Append && path.exists() {
        fs::metadata(path).map_err(into_napi_error)?.len() > 0
    } else {
        false
    };

    let file = match mode {
        SaveMode::Overwrite => File::create(path).map_err(into_napi_error)?,
        SaveMode::Append => OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .map_err(into_napi_error)?,
    };

    Ok(CsvStreamingWriter {
        writer: WriterBuilder::new()
            .has_headers(false)
            .from_writer(BufWriter::new(file)),
        headers_written,
    })
}

fn open_json_streaming_writer(path: &Path, mode: SaveMode) -> DbNativeResult<JsonStreamingWriter> {
    match mode {
        SaveMode::Overwrite => {
            let mut writer = BufWriter::new(File::create(path).map_err(into_napi_error)?);
            writer.write_all(b"[").map_err(into_napi_error)?;
            Ok(JsonStreamingWriter {
                writer,
                wrote_any_row: false,
            })
        }
        SaveMode::Append => {
            let mut file = OpenOptions::new()
                .create(true)
                .read(true)
                .write(true)
                .open(path)
                .map_err(into_napi_error)?;
            let file_len = file.metadata().map_err(into_napi_error)?.len();

            if file_len == 0 {
                file.write_all(b"[").map_err(into_napi_error)?;
                return Ok(JsonStreamingWriter {
                    writer: BufWriter::new(file),
                    wrote_any_row: false,
                });
            }

            let append_state = inspect_json_array_for_append(&mut file)?;
            file.set_len(append_state.closing_bracket_pos)
                .map_err(into_napi_error)?;
            file.seek(SeekFrom::Start(append_state.closing_bracket_pos))
                .map_err(into_napi_error)?;

            Ok(JsonStreamingWriter {
                writer: BufWriter::new(file),
                wrote_any_row: append_state.has_rows,
            })
        }
    }
}

struct JsonArrayAppendState {
    closing_bracket_pos: u64,
    has_rows: bool,
}

fn inspect_json_array_for_append(file: &mut File) -> DbNativeResult<JsonArrayAppendState> {
    let file_len = file.metadata().map_err(into_napi_error)?.len();
    let Some((_, first_non_whitespace)) = find_first_non_whitespace_byte(file, file_len)? else {
        return Err(state_error(
            "Cannot append to an empty JSON file. Use overwrite mode or recreate the file.",
        ));
    };

    if first_non_whitespace != b'[' {
        return Err(state_error(
            "Append mode for JSON requires an existing JSON array file.",
        ));
    }

    let Some((closing_bracket_pos, last_non_whitespace)) =
        find_last_non_whitespace_byte(file, file_len)?
    else {
        return Err(state_error(
            "Append mode for JSON requires an existing JSON array file.",
        ));
    };

    if last_non_whitespace != b']' {
        return Err(state_error(
            "Append mode for JSON requires an existing JSON array file.",
        ));
    }

    let has_rows = matches!(
        find_last_non_whitespace_byte(file, closing_bracket_pos)?,
        Some((_, byte)) if byte != b'['
    );

    Ok(JsonArrayAppendState {
        closing_bracket_pos,
        has_rows,
    })
}

fn find_first_non_whitespace_byte(
    file: &mut File,
    file_len: u64,
) -> DbNativeResult<Option<(u64, u8)>> {
    let mut position = 0u64;
    let mut byte = [0u8; 1];

    while position < file_len {
        file.seek(SeekFrom::Start(position))
            .map_err(into_napi_error)?;
        file.read_exact(&mut byte).map_err(into_napi_error)?;

        if !byte[0].is_ascii_whitespace() {
            return Ok(Some((position, byte[0])));
        }

        position += 1;
    }

    Ok(None)
}

fn find_last_non_whitespace_byte(
    file: &mut File,
    upper_bound_exclusive: u64,
) -> DbNativeResult<Option<(u64, u8)>> {
    if upper_bound_exclusive == 0 {
        return Ok(None);
    }

    let mut position = upper_bound_exclusive;
    let mut byte = [0u8; 1];

    while position > 0 {
        position -= 1;
        file.seek(SeekFrom::Start(position))
            .map_err(into_napi_error)?;
        file.read_exact(&mut byte).map_err(into_napi_error)?;

        if !byte[0].is_ascii_whitespace() {
            return Ok(Some((position, byte[0])));
        }
    }

    Ok(None)
}

#[allow(dead_code)]
fn save_csv(query_result: &QueryResult, path: &Path) -> DbNativeResult<()> {
    let file = File::create(path).map_err(into_napi_error)?;
    let writer = BufWriter::new(file);
    let mut csv_writer = WriterBuilder::new().has_headers(true).from_writer(writer);

    csv_writer
        .write_record(
            query_result
                .columns
                .iter()
                .map(|column| column.name.as_str()),
        )
        .map_err(into_napi_error)?;

    for row in &query_result.rows {
        let object = row.as_object();
        let record = query_result
            .columns
            .iter()
            .map(|column| stringify_cell(object.and_then(|value| value.get(&column.name))))
            .collect::<Vec<_>>();

        csv_writer.write_record(record).map_err(into_napi_error)?;
    }

    csv_writer.flush().map_err(into_napi_error)
}

#[allow(dead_code)]
fn save_json(query_result: &QueryResult, path: &Path) -> DbNativeResult<()> {
    let file = File::create(path).map_err(into_napi_error)?;
    serde_json::to_writer_pretty(BufWriter::new(file), &query_result.rows).map_err(into_napi_error)
}

#[allow(dead_code)]
fn save_excel(query_result: &QueryResult, path: &Path) -> DbNativeResult<()> {
    let mut workbook = Workbook::new();
    let worksheet = workbook.add_worksheet_with_low_memory();

    for (column_index, column) in query_result.columns.iter().enumerate() {
        let column_index = u16::try_from(column_index)
            .map_err(|_| state_error("Excel column count exceeds worksheet limits"))?;
        worksheet
            .write(0, column_index, column.name.as_str())
            .map_err(into_napi_error)?;
    }

    for (row_index, row) in query_result.rows.iter().enumerate() {
        let row_index = u32::try_from(row_index + 1)
            .map_err(|_| state_error("Excel row count exceeds worksheet limits"))?;
        let object = row.as_object();

        for (column_index, column) in query_result.columns.iter().enumerate() {
            let column_index = u16::try_from(column_index)
                .map_err(|_| state_error("Excel column count exceeds worksheet limits"))?;
            write_excel_cell(
                worksheet,
                row_index,
                column_index,
                object.and_then(|value| value.get(&column.name)),
            )?;
        }
    }

    workbook.save(path).map_err(into_napi_error)
}

#[allow(dead_code)]
fn save_parquet(query_result: &QueryResult, path: &Path) -> DbNativeResult<()> {
    let batch = build_record_batch(query_result)?;
    let file = File::create(path).map_err(into_napi_error)?;
    let mut writer =
        ParquetArrowWriter::try_new(file, batch.schema(), None).map_err(into_napi_error)?;

    writer.write(&batch).map_err(into_napi_error)?;
    writer.close().map_err(into_napi_error)?;
    Ok(())
}

#[allow(dead_code)]
fn save_orc(query_result: &QueryResult, path: &Path) -> DbNativeResult<()> {
    let batch = build_record_batch(query_result)?;
    let file = File::create(path).map_err(into_napi_error)?;
    let mut writer = OrcArrowWriterBuilder::new(file, batch.schema())
        .try_build()
        .map_err(into_napi_error)?;

    writer.write(&batch).map_err(into_napi_error)?;
    writer.close().map_err(into_napi_error)?;
    Ok(())
}

#[allow(dead_code)]
fn build_record_batch(query_result: &QueryResult) -> DbNativeResult<RecordBatch> {
    let inferred_types = infer_column_types(&query_result.columns, &query_result.rows);
    build_record_batch_from_parts(&query_result.columns, &query_result.rows, &inferred_types)
}

fn infer_column_types(columns: &[ColumnMeta], rows: &[Value]) -> Vec<InferredColumnType> {
    columns
        .iter()
        .map(|column| infer_column_type(rows, &column.name))
        .collect()
}

fn infer_column_type(rows: &[Value], column_name: &str) -> InferredColumnType {
    let mut inferred = None;

    for row in rows {
        let Some(value) = row.as_object().and_then(|object| object.get(column_name)) else {
            continue;
        };

        if value.is_null() {
            continue;
        }

        inferred = Some(match (inferred, value) {
            (None, Value::Bool(_)) => InferredColumnType::Bool,
            (None, Value::Number(number)) if number.is_i64() => InferredColumnType::Int64,
            (None, Value::Number(number)) if number.is_u64() => {
                match number.as_u64().filter(|value| *value <= i64::MAX as u64) {
                    Some(_) => InferredColumnType::Int64,
                    None => InferredColumnType::Utf8,
                }
            }
            (None, Value::Number(_)) => InferredColumnType::Float64,
            (None, Value::String(_)) | (None, Value::Array(_)) | (None, Value::Object(_)) => {
                InferredColumnType::Utf8
            }
            (Some(InferredColumnType::Bool), Value::Bool(_)) => InferredColumnType::Bool,
            (Some(InferredColumnType::Int64), Value::Number(number))
                if number.is_i64()
                    || number
                        .as_u64()
                        .filter(|value| *value <= i64::MAX as u64)
                        .is_some() =>
            {
                InferredColumnType::Int64
            }
            (Some(InferredColumnType::Float64), Value::Number(_)) => InferredColumnType::Float64,
            (Some(InferredColumnType::Int64), Value::Number(_)) => InferredColumnType::Float64,
            _ => InferredColumnType::Utf8,
        });

        if matches!(inferred, Some(InferredColumnType::Utf8)) {
            break;
        }
    }

    inferred.unwrap_or(InferredColumnType::Utf8)
}

fn build_schema(columns: &[ColumnMeta], inferred_types: &[InferredColumnType]) -> Arc<Schema> {
    Arc::new(Schema::new(
        columns
            .iter()
            .zip(inferred_types.iter())
            .map(|(column, inferred_type)| {
                Field::new(
                    &column.name,
                    match inferred_type {
                        InferredColumnType::Bool => DataType::Boolean,
                        InferredColumnType::Float64 => DataType::Float64,
                        InferredColumnType::Int64 => DataType::Int64,
                        InferredColumnType::Utf8 => DataType::Utf8,
                    },
                    true,
                )
            })
            .collect::<Vec<_>>(),
    ))
}

fn build_record_batch_from_parts(
    columns: &[ColumnMeta],
    rows: &[Value],
    inferred_types: &[InferredColumnType],
) -> DbNativeResult<RecordBatch> {
    let schema = build_schema(columns, inferred_types);
    let arrays = columns
        .iter()
        .zip(inferred_types.iter())
        .map(|(column, inferred_type)| build_array(rows, &column.name, *inferred_type))
        .collect::<DbNativeResult<Vec<_>>>()?;

    RecordBatch::try_new(schema, arrays).map_err(into_napi_error)
}

fn build_array(
    rows: &[Value],
    column_name: &str,
    inferred_type: InferredColumnType,
) -> DbNativeResult<ArrayRef> {
    match inferred_type {
        InferredColumnType::Bool => {
            let mut builder = BooleanBuilder::new();

            for row in rows {
                match row.as_object().and_then(|object| object.get(column_name)) {
                    Some(Value::Bool(value)) => builder.append_value(*value),
                    _ => builder.append_null(),
                }
            }

            Ok(Arc::new(builder.finish()) as ArrayRef)
        }
        InferredColumnType::Int64 => {
            let mut builder = Int64Builder::new();

            for row in rows {
                match row.as_object().and_then(|object| object.get(column_name)) {
                    Some(Value::Number(value)) => {
                        if let Some(number) = value.as_i64() {
                            builder.append_value(number);
                        } else if let Some(number) =
                            value.as_u64().filter(|item| *item <= i64::MAX as u64)
                        {
                            builder.append_value(number as i64);
                        } else {
                            builder.append_null();
                        }
                    }
                    _ => builder.append_null(),
                }
            }

            Ok(Arc::new(builder.finish()) as ArrayRef)
        }
        InferredColumnType::Float64 => {
            let mut builder = Float64Builder::new();

            for row in rows {
                match row.as_object().and_then(|object| object.get(column_name)) {
                    Some(Value::Number(value)) => {
                        let number = value
                            .as_f64()
                            .or_else(|| value.as_i64().map(|item| item as f64))
                            .or_else(|| value.as_u64().map(|item| item as f64));

                        if let Some(number) = number {
                            builder.append_value(number);
                        } else {
                            builder.append_null();
                        }
                    }
                    _ => builder.append_null(),
                }
            }

            Ok(Arc::new(builder.finish()) as ArrayRef)
        }
        InferredColumnType::Utf8 => {
            let mut builder = StringBuilder::new();

            for row in rows {
                match row.as_object().and_then(|object| object.get(column_name)) {
                    None | Some(Value::Null) => builder.append_null(),
                    Some(value) => builder.append_value(stringify_cell(Some(value))),
                }
            }

            Ok(Arc::new(builder.finish()) as ArrayRef)
        }
    }
}

fn write_excel_cell(
    worksheet: &mut rust_xlsxwriter::Worksheet,
    row_index: u32,
    column_index: u16,
    value: Option<&Value>,
) -> DbNativeResult<()> {
    match value {
        None | Some(Value::Null) => Ok(()),
        Some(Value::Bool(value)) => worksheet
            .write(row_index, column_index, *value)
            .map(|_| ())
            .map_err(into_napi_error),
        Some(Value::Number(value)) => {
            let number = value
                .as_f64()
                .or_else(|| value.as_i64().map(|item| item as f64))
                .or_else(|| value.as_u64().map(|item| item as f64))
                .ok_or_else(|| state_error("Unable to convert JSON number to Excel value"))?;
            worksheet
                .write(row_index, column_index, number)
                .map(|_| ())
                .map_err(into_napi_error)
        }
        Some(Value::String(value)) => worksheet
            .write(row_index, column_index, value.as_str())
            .map(|_| ())
            .map_err(into_napi_error),
        Some(other) => {
            let value = serde_json::to_string(other).map_err(into_napi_error)?;
            worksheet
                .write(row_index, column_index, value.as_str())
                .map(|_| ())
                .map_err(into_napi_error)
        }
    }
}

fn stringify_cell(value: Option<&Value>) -> String {
    match value {
        None | Some(Value::Null) => String::new(),
        Some(Value::Bool(value)) => value.to_string(),
        Some(Value::Number(value)) => value.to_string(),
        Some(Value::String(value)) => value.clone(),
        Some(other) => serde_json::to_string(other).unwrap_or_default(),
    }
}
