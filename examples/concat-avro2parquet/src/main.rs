use std::fs::File;
use std::io;

use std::process::ExitCode;

use rs_concat_avro2parquet::parquet;

use parquet::file::properties::WriterProperties;

use rs_concat_avro2parquet::concat_avro2parquet;

fn env2avro_filename() -> Result<String, io::Error> {
    std::env::var("ENV_IN_AVRO_FILENAME").map_err(io::Error::other)
}

fn env2parquet_filename() -> Result<String, io::Error> {
    std::env::var("ENV_IN_PARQUET_FILENAME").map_err(io::Error::other)
}

fn env2output_parquet_filename() -> Result<String, io::Error> {
    std::env::var("ENV_OUT_PARQUET_FILENAME").map_err(io::Error::other)
}

fn sub() -> Result<(), io::Error> {
    let props: Option<WriterProperties> = None;

    let name_ia: String = env2avro_filename()?;
    let name_ip: String = env2parquet_filename()?;
    let name_op: String = env2output_parquet_filename()?;

    let file_ia: File = File::open(name_ia)?;
    let file_ip: File = File::open(name_ip)?;
    let file_op: File = File::create(name_op)?;

    let bw = io::BufWriter::new(file_op);
    let wrote = concat_avro2parquet(file_ia, file_ip, bw, props)?;
    let wrote_file: File = wrote.into_inner()?;
    wrote_file.sync_data()?;

    Ok(())
}

fn main() -> ExitCode {
    match sub() {
        Ok(_) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("{e}");
            ExitCode::FAILURE
        }
    }
}
