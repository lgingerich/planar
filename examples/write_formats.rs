//! Writes the same small Arrow table to Parquet, Lance, and Vortex files.
//!
//! Run from repo root: cargo run --example write_formats

use std::path::Path;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};
use arrow_array::{Float64Array, Int64Array, RecordBatch, StringArray};
use futures::{stream, TryStreamExt};
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use planar::storage::file_format::{
    lance::LanceWriter,
    parquet::{ParquetReadOptions, ParquetReader, ParquetWriter},
    vortex::VortexWriter,
};
use planar::storage::RecordBatchStream;
use vortex::file::VortexWriteOptions;
use vortex::session::VortexSession;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out_dir = Path::new("examples/output");
    let _ = std::fs::remove_dir_all(out_dir);
    std::fs::create_dir_all(out_dir)?;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["a", "b", "c"])),
            Arc::new(Float64Array::from(vec![10.0, 20.0, 30.0])),
        ],
    )?;

    let lance_path = out_dir.join("data.lance");
    LanceWriter::new()
        .write_with_options(&batch, &lance_path, &Default::default())
        .await?;
    println!("Wrote {}", lance_path.display());

    let parquet_path = out_dir.join("data.parquet");
    let parquet_options = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();
    ParquetWriter::new()
        .write_with_options(&batch, &parquet_path, &parquet_options)
        .await?;
    println!("Wrote {}", parquet_path.display());

    let parquet_stream_path = out_dir.join("data_stream.parquet");
    let stream: RecordBatchStream =
        Box::pin(stream::iter(vec![Ok(batch.clone()), Ok(batch.clone())]));
    ParquetWriter::new()
        .write_stream(stream, &parquet_stream_path, &parquet_options)
        .await?;
    println!("Wrote {}", parquet_stream_path.display());

    let vortex_path = out_dir.join("data.vortex");
    let vortex_options = VortexWriteOptions::new(VortexSession::default());
    VortexWriter::new()
        .write_with_options(&batch, &vortex_path, &vortex_options)
        .await?;
    println!("Wrote {}", vortex_path.display());

    let reader = ParquetReader::new();
    let read_stream = reader
        .read_stream(&parquet_path, &ParquetReadOptions::default())
        .await?;
    let batches: Vec<RecordBatch> = read_stream.try_collect().await?;
    println!("Read {} batches from {}", batches.len(), parquet_path.display());

    println!("Done.");
    Ok(())
}
