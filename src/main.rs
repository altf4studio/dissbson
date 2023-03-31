use bson::Document;
use clap::Parser;
use flate2::write::{ZlibDecoder, ZlibEncoder};
use flate2::Compression;
use lua_engine::LuaEngine;
use neoncore::streams::{read::read_pattern, SeekRead};
use parking_lot::RwLock;
use rayon::prelude::IndexedParallelIterator;
use rayon::{
    prelude::{IntoParallelRefIterator, ParallelIterator},
    ThreadPoolBuilder,
};
use serde::{ser::SerializeSeq, Deserialize, Serialize, Serializer};
use std::sync::Arc;
use std::{
    fs::{File, OpenOptions},
    io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    ops::Bound,
    path::{Path, PathBuf},
};
use thiserror::Error;

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
    pub output: PathBuf,

    /// The number of threads to use
    #[clap(short, long, default_value = "4")]
    pub threads: usize,

    /// How many documents to work with in RAM at a time
    /// this options controls memory usage, the higher the value the more memory
    /// will be used but io will be faster
    #[clap(short, long, default_value = "100")]
    pub batch: usize,

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

    /// Single file output
    /// write all documents to a single file as a json array
    #[clap(long)]
    pub single: bool,
}

#[derive(Debug, Error)]
enum DissectError {
    #[error("IO Error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Serde Error: {0}")]
    Postcard(#[from] postcard::Error),
    #[error("Json Error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("Bson Error: {0}")]
    Bson(#[from] bson::de::Error),
    #[error("Lua Error: {0}")]
    LuaError(#[from] rlua::Error),
    #[error("Thread Pool Error: {0}")]
    ThreadPool(#[from] rayon::ThreadPoolBuildError),
    #[error("Parse Error: {0}")]
    Parse(String),
    #[error("Unexpected Error: {0}")]
    Unexpected(String),
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
struct DocOffset {
    offset: usize,
    size: usize,
}

fn main() -> Result<(), DissectError> {
    println!("---------------------------------------");
    println!("BSON Dissector v{}", env!("CARGO_PKG_VERSION"));
    println!("Copyright (c) 2023 Neon Imp");
    println!("Licensed under the BSD-3-Clause License");
    println!("---------------------------------------\n");

    let args = Args::parse();
    let path = args.input.as_path();
    let output = args.output.as_path();

    if args.single && output.is_dir() {
        return Err(DissectError::Io(std::io::Error::new(
            std::io::ErrorKind::Other,
            "Output path must be a file when using --single",
        )));
    }

    if !output.exists() && !args.single {
        std::fs::create_dir(output)?;
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
        idx[parse_slice(&slice)?].to_vec()
    } else {
        idx
    };

    // progress bar
    let pb = indicatif::ProgressBar::new(idx.len() as u64);
    pb.set_style(indicatif::ProgressStyle::default_bar().template(
        "{spinner:.green} [{elapsed_precise}] [{eta_precise}] [{bar:40.red/blue}] {pos:>7}/{len:7} \n {msg}",
    ).unwrap());

    let thread_pool = ThreadPoolBuilder::new().num_threads(args.threads).build()?;

    if args.single {
        let mut file = File::create(output).unwrap();
        let mut bufwriter = BufWriter::new(&mut file);
        let mut ser = serde_json::Serializer::new(&mut bufwriter);
        let writer = Arc::new(RwLock::new(ser.serialize_seq(Some(idx.len())).unwrap()));

        thread_pool.install(|| {
            let chunk_ct = Arc::new(RwLock::new(0));
            idx.par_iter().chunks(args.batch).for_each(|offsets| {
                let docs = if let Some(script) = &args.script {
                    apply_script(path, script, offsets).unwrap()
                } else {
                    load_docs(path, offsets).unwrap()
                };

                let mut writer_lock = writer.write();
                for doc in docs {
                    writer_lock.serialize_element(&doc).unwrap();
                }

                pb.inc(args.batch as u64);
                *chunk_ct.write() += 1
            });
        });
        match Arc::try_unwrap(writer) {
            Ok(l) => {
                let l = l.into_inner();
                l.end().unwrap();
            }
            Err(_) => {
                panic!("Failed to unwrap writer");
            }
        };
    } else {
        thread_pool.install(|| {
            let chunk_ct = Arc::new(RwLock::new(0));
            idx.par_iter().chunks(args.batch).for_each(|offsets| {
                let docs = if let Some(script) = &args.script {
                    apply_script(path, script, offsets).unwrap()
                } else {
                    load_docs(path, offsets).unwrap()
                };

                for (nth, doc) in docs.into_iter().enumerate() {
                    save_single_doc(
                        doc,
                        output,
                        format!("{}-{}", chunk_ct.read(), nth),
                        args.pretty,
                    )
                    .unwrap();
                }

                pb.inc(args.batch as u64);
                *chunk_ct.write() += 1
            });
        });
    }

    pb.finish_with_message("");
    println!("Exported {} documents to {}", idx.len(), output.display());

    Ok(())
}

fn load_index_data<P: AsRef<Path>>(path: P) -> Result<Vec<DocOffset>, DissectError> {
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

fn inspect_bson<P: AsRef<Path>>(bson_file: P) -> Result<Vec<DocOffset>, DissectError> {
    let path = bson_file.as_ref();
    let mut file = OpenOptions::new().read(true).open(path)?;
    let mut reader = BufReader::new(&mut file);
    let (offsets, _) = index_file(&mut reader)?;
    Ok(offsets)
}

fn index_file<R: SeekRead>(mut reader: R) -> Result<(Vec<DocOffset>, usize), DissectError> {
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
fn parse_slice(slice: &str) -> Result<(Bound<usize>, Bound<usize>), DissectError> {
    let slice = slice.trim();
    let slice = slice.trim_matches(|c| c == '[' || c == ']');
    let mut parts = slice.split("..").collect::<Vec<_>>();
    if parts.len() != 2 {
        return Err(DissectError::Parse("Invalid slice format".into()));
    }
    let start = parts.remove(0).parse::<usize>().unwrap_or(0);
    let end = parts.remove(0).parse::<usize>().unwrap_or(!0);
    if start > end {
        return Err(DissectError::Parse("Invalid slice format".into()));
    }

    if start != 0 && end != !0 {
        Ok((Bound::Included(start), Bound::Excluded(end)))
    } else if start != 0 {
        Ok((Bound::Included(start), Bound::Unbounded))
    } else if end != !0 {
        Ok((Bound::Unbounded, Bound::Excluded(end)))
    } else {
        Ok((Bound::Unbounded, Bound::Unbounded))
    }
    // Ok((start, end))
}

fn apply_script<P: AsRef<Path>>(
    input: P,
    script: P,
    offsets: Vec<&DocOffset>,
) -> Result<Vec<Document>, DissectError> {
    let script = script.as_ref();
    let script = std::fs::read_to_string(script)?;

    let docs = load_docs(input, offsets)?;
    let mut res = Vec::with_capacity(docs.len());
    let lctx = LuaEngine::new()
        .map_err(|e| DissectError::Unexpected(format!("Failed to create Lua context: {}", e)))?;
    for doc in docs {
        lctx.load_document(doc)?;
        lctx.load_script(&script)?;
        res.push(lctx.get_document()?);
    }
    Ok(res)
}

fn load_docs<P: AsRef<Path>>(
    input: P,
    offsets: Vec<&DocOffset>,
) -> Result<Vec<Document>, DissectError> {
    let path = input.as_ref();
    let mut file = OpenOptions::new().read(true).open(path)?;
    let mut docs = Vec::new();
    for offset in offsets {
        file.seek(SeekFrom::Start(offset.offset as u64))?;
        let mut buf = vec![0u8; offset.size];
        file.read_exact(&mut buf)?;
        docs.push(Document::from_reader(&mut buf.as_slice())?);
    }
    Ok(docs)
}

fn save_single_doc<P: AsRef<Path>>(
    doc: Document,
    out_dir: P,
    idx: String,
    pretty: bool,
) -> Result<(), DissectError> {
    let out_dir = out_dir.as_ref();
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(out_dir.join(format!("{}.json", idx)))?;
    let writer = BufWriter::new(&mut file);
    if pretty {
        let mut ser = serde_json::Serializer::pretty(writer);
        doc.serialize(&mut ser)?;
    } else {
        let mut ser = serde_json::Serializer::new(writer);
        doc.serialize(&mut ser)?;
    }
    Ok(())
}
