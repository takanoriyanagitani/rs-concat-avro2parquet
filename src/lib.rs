pub use parquet;

use std::io;

use io::BufReader;
use io::Read;
use io::Seek;

use io::Write;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;

use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use parquet::file::properties::WriterProperties;

use datafusion_datasource_avro::avro_to_arrow::ReaderBuilder;

pub fn rdr2avro2batch<R>(
    rdr: R,
) -> Result<
    (
        SchemaRef,
        impl Iterator<Item = Result<RecordBatch, io::Error>>,
    ),
    io::Error,
>
where
    R: Read + Seek,
{
    let rdr = ReaderBuilder::new().build(rdr)?;
    let sch: SchemaRef = rdr.schema();
    let mapd = rdr.map(|rslt| rslt.map_err(io::Error::other));
    Ok((sch, mapd))
}

pub fn file2parquet2batch(
    f: std::fs::File,
) -> Result<impl Iterator<Item = Result<RecordBatch, io::Error>>, io::Error> {
    let bldr = ParquetRecordBatchReaderBuilder::try_new(f)?;
    let rdr = bldr.build()?;
    let mapd = rdr.map(|rslt| rslt.map_err(io::Error::other));
    Ok(mapd)
}

pub fn concat_iter<I, J>(i1: I, i2: J) -> impl Iterator<Item = Result<RecordBatch, io::Error>>
where
    I: Iterator<Item = Result<RecordBatch, io::Error>>,
    J: Iterator<Item = Result<RecordBatch, io::Error>>,
{
    i1.chain(i2)
}

pub fn batch2parquet<I, W>(
    batch: I,
    sch: SchemaRef,
    wtr: W,
    opts: Option<WriterProperties>,
) -> Result<W, io::Error>
where
    I: Iterator<Item = Result<RecordBatch, io::Error>>,
    W: Write + Send,
{
    let mut wtr = ArrowWriter::try_new(wtr, sch, opts)?;
    for rb in batch {
        let bat: RecordBatch = rb?;
        wtr.write(&bat)?;
    }
    wtr.into_inner().map_err(io::Error::other)
}

pub fn concat_avro2parquet<W>(
    avro_file: std::fs::File,
    original_parquet: std::fs::File,
    wtr: W,
    opts: Option<WriterProperties>,
) -> Result<W, io::Error>
where
    W: Write + Send,
{
    let bardr = BufReader::new(avro_file);
    let (sch, i1_avro) = rdr2avro2batch(bardr)?;
    let i2_parquet = file2parquet2batch(original_parquet)?;
    let concat = concat_iter(i1_avro, i2_parquet);
    let mut wrote: W = batch2parquet(concat, sch, wtr, opts)?;
    wrote.flush()?;
    Ok(wrote)
}
