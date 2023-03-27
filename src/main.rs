use std::{
    cmp::min,
    fs::{File, OpenOptions},
    io::{BufReader, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

use bson::Document;
use clap::Parser;
use flate2::write::{ZlibDecoder, ZlibEncoder};
use flate2::Compression;
use lua_engine::LuaEngine;
use neoncore::streams::{read::read_pattern, SeekRead};
use rayon::{
    prelude::{IntoParallelRefIterator, ParallelIterator},
    ThreadPoolBuilder,
};
use serde::{Deserialize, Serialize};

mod lua_engine;

/// Tool to dissect a bson file into json files for each document
///
/// this tool can handle very large bson files with millions of documents
/// and gigabytes of data.
#[derive(Debug, Parser)]
#[clap(version=env!("CARGO_PKG_VERSION"), author="Matheus Xavier <mxavier@neonimp.com>", about)]
pub struct Args {
    /// The input file to read
    pub input: PathBuf,

    /// The output directory to write to
    #[clap(short, long, default_value = "output")]
    pub output: PathBuf,

    /// The number of threads to use
    #[clap(short, long, default_value = "4")]
    pub threads: usize,

    /// Only inspect the file and do not write any output
    #[clap(long)]
    pub inspect: bool,

    /// pretty json output
    #[clap(long)]
    pub pretty: bool,

    /// Limit using a rust slice expression
    #[clap(short, long)]
    pub slice: Option<String>,

    /// Lua script to run on each document
    #[clap(short = 'S', long)]
    pub script: Option<PathBuf>,
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
struct DocOffset {
    offset: usize,
    size: usize,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("---------------------------------------");
    println!("BSON Dissector v{}", env!("CARGO_PKG_VERSION"));
    println!("Copyright (c) 2023 Neon Imp");
    println!("Licensed under the BSD-3-Clause License");
    println!("---------------------------------------\n");

    let args = Args::parse();
    let path = args.input.as_path();
    let out_dir = args.output.as_path();

    if !out_dir.exists() {
        std::fs::create_dir(out_dir)?;
    }

    let idx = if args.input.with_extension("idx.dat").exists() && !args.inspect {
        println!("Found index file, skipping inspection...");
        load_index_data(path.with_extension("idx.dat"))?
    } else {
        println!("Inspecting file: {}", path.display());
        let offsets = inspect_bson(path)?;
        let mut offsets_checkpoint = File::create(path.with_extension("idx.dat"))?;
        let ser = postcard::to_allocvec_cobs(&offsets)?;
        let mut enc = ZlibEncoder::new(&mut offsets_checkpoint, Compression::default());
        enc.write_all(&ser)?;
        enc.finish()?;
        offsets
    };

    let idx = if let Some(slice) = args.slice {
        let (start, end) = parse_slice(&slice)?;
        idx[start as usize..min(end as usize, idx.len())].to_vec()
    } else {
        idx
    };

    // progress bar
    let pb = indicatif::ProgressBar::new(idx.len() as u64);
    pb.set_style(indicatif::ProgressStyle::default_bar().template(
        "{spinner:.green} [{elapsed_precise}] [{bar:40.red/blue}] {pos:>7}/{len:7} \n {msg}",
    )?);

    println!("Loaded index data, containing {} entries", idx.len());
    let thread_pool = ThreadPoolBuilder::new().num_threads(args.threads).build()?;
    thread_pool.install(|| {
        idx.par_iter().for_each(|offset| {
            if let Some(script) = &args.script {
                let doc = apply_script(path, script, *offset).unwrap();
                
                
                let json = if args.pretty {
                    serde_json::to_string_pretty(&doc).unwrap()
                } else {
                    serde_json::to_string(&doc).unwrap()
                };

                let mut out_file = File::create(out_dir.join(format!("{}.json", offset.offset))).unwrap();
                out_file.write_all(json.as_bytes()).unwrap();
            } else {
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap()
                    .block_on(export_doc(path, out_dir, *offset, args.pretty))
                    .unwrap();
            }
            pb.inc(1);
        });
    });
    pb.finish_with_message("");
    println!("Exported {} documents to {}", idx.len(), out_dir.display());

    Ok(())
}

fn load_index_data<P: AsRef<Path>>(path: P) -> Result<Vec<DocOffset>, Box<dyn std::error::Error>> {
    let path = path.as_ref();

    let mut file = OpenOptions::new().read(true).open(path)?;
    let mut dat = Vec::new();
    let mut reader = BufReader::new(&mut file);
    let mut dec = ZlibDecoder::new(&mut dat);
    let mut buf = [0u8; 8192];
    while let Ok(n) = reader.read(&mut buf[..]) {
        if n == 0 {
            break;
        }
        dec.write_all(&buf[..n])?;
    }
    dec.finish()?;

    let offsets = postcard::from_bytes_cobs::<Vec<DocOffset>>(&mut dat)?;

    Ok(offsets)
}

fn inspect_bson<P: AsRef<Path>>(
    bson_file: P,
) -> Result<Vec<DocOffset>, Box<dyn std::error::Error>> {
    let path = bson_file.as_ref();
    let mut file = OpenOptions::new().read(true).open(path)?;
    let mut reader = BufReader::new(&mut file);
    let (offsets, _) = count_documents(&mut reader)?;
    Ok(offsets)
}

fn count_documents<R: SeekRead>(
    mut reader: R,
) -> Result<(Vec<DocOffset>, usize), Box<dyn std::error::Error>> {
    let mut count = 0;
    // little endian 4 byte int
    let pat = "@W";
    let mut offsets = Vec::new();

    let mut buf = [0u8; 4];

    loop {
        let n = reader.read(&mut buf)?;
        if n == 0 {
            break;
        }
        count += 1;
        let size: i32 = read_pattern(&buf[..], pat)?[0].try_into()?;
        offsets.push(DocOffset {
            offset: reader.stream_position()? as usize - 4,
            size: size as usize,
        });
        // seek to the end of the document minus the 4 bytes that were just read
        reader.seek(SeekFrom::Current((size - 4) as i64))?;
    }
    reader.rewind()?;
    Ok((offsets, count))
}

/// Split a string in the form of `start..end` into a tuple of `start` and `end`
fn parse_slice(slice: &str) -> Result<(u64, u64), Box<dyn std::error::Error>> {
    let slice = slice.trim();
    let slice = slice.trim_matches(|c| c == '[' || c == ']');
    let mut parts = slice.split("..").collect::<Vec<_>>();
    if parts.len() != 2 {
        return Err("Invalid slice format".into());
    }
    let start = parts.remove(0).parse::<u64>().unwrap_or(0);
    let end = parts.remove(0).parse::<u64>().unwrap_or(!0);
    Ok((start, end))
}

fn apply_script<P: AsRef<Path>>(
    input: P,
    script: P,
    offset: DocOffset,
) -> Result<Document, Box<dyn std::error::Error>> {
    let script = script.as_ref();
    let script = std::fs::read_to_string(script)?;

    let path = input.as_ref();
    let mut file = OpenOptions::new().read(true).open(path)?;
    file.seek(SeekFrom::Start(offset.offset as u64))?;
    let mut buf = vec![0u8; offset.size];
    file.read_exact(&mut buf)?;
    let doc = Document::from_reader(&mut buf.as_slice())?;

    let lctx = LuaEngine::new()?;
    lctx.load_document(doc)?;
    lctx.load_script(&script)?;
    let res = lctx.get_document()?;
    Ok(res)
}

async fn export_doc<P: AsRef<Path>>(
    input: P,
    out_dir: P,
    offset: DocOffset,
    pretty: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let path = input.as_ref();
    let out_dir = out_dir.as_ref();
    let mut file = OpenOptions::new().read(true).open(path)?;
    file.seek(SeekFrom::Start(offset.offset as u64))?;
    let mut buf = vec![0u8; offset.size];
    file.read_exact(&mut buf)?;
    let doc = Document::from_reader(&mut buf.as_slice())?;

    let json = if pretty {
        serde_json::to_string_pretty(&doc)?
    } else {
        serde_json::to_string(&doc)?
    };

    let mut out_file = File::create(out_dir.join(format!("{}.json", offset.offset)))?;
    out_file.write_all(json.as_bytes())?;
    Ok(())
}
