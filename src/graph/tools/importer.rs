///! Graph Importer
use std::{
    borrow::BorrowMut,
    collections::VecDeque,
    fmt::DebugList,
    fs::File,
    io::{BufRead, BufReader, Read, Seek, SeekFrom, Write},
    ops::Deref,
    path::Path,
    sync::{Arc, Mutex},
};

use clap::Parser;
use rayon::prelude::*;
use rocksdb::{DBCompressionType, Options};

use super::segment::SegmentReader;
use super::shuffle::{Shuffle, ShuffleOptions};
use crate::graph::graph::Graph;
use crate::graph::r#type::*;
use crate::graph::tools::file_sharder::shard_file_to_parts;

/// Stage 1 : get vertex statistics, decide how many segments, and segment range.
/// Make sure each segment holds a maximum of given vertices, so the segment can
/// be kept in memory and do in-memory sorting.

/// Stage 2 : write file to segments
/// read worker ( read files and parse file to (from, to) pairs )
/// with range partition, send pair to segments
///    |
///    V
/// write worker ( recv pair from channel and write to segments),
/// each worker in charge of some segment sorting and writing.
///    |
///    V
/// segment ( each segment has an underlying file as durable storage )

/// Stage 3 : read segments and make kv pairs and write to sst files.
///    read segment file  (read worker (single))
///          |
///          V
///       parallel sort ( sort worker (many) )
///          |
///          V
///      write sst file ( write worker (many) )
///          

#[derive(Debug, Parser, Clone)]
pub struct ImportOptions {
    #[clap(flatten)]
    shuffle_opts: ShuffleOptions,

    /// threads for read shuffle files
    #[clap(long, default_value = "1")]
    read_threads: usize,

    /// threads for data sorting
    #[clap(long, default_value = "32")]
    sort_threads: usize,

    /// threads for writing sst files
    #[clap(long, default_value = "1")]
    write_threads: usize,
}

impl Default for ImportOptions {
    fn default() -> Self {
        Self {
            shuffle_opts: ShuffleOptions::default(),
            read_threads: 4,
            sort_threads: 32,
            write_threads: 4,
        }
    }
}

pub struct GraphImporter {
    opts: ImportOptions,
    input_dir: String,
    output_dir: String,
    shuffle_dir: String,
    delimiter: char,
    graph: Arc<Graph>,
}

impl GraphImporter {
    pub fn new(
        input_dir: String,
        output_dir: String,
        shuffle_dir: String,
        opts: ImportOptions,
        delimiter: char,
        graph: Arc<Graph>,
    ) -> Self {
        Self {
            opts,
            input_dir,
            output_dir,
            shuffle_dir,
            delimiter,
            graph: graph.clone(),
        }
    }

    fn write_sst(&mut self, fpath: String, out_path: String) {
        // read file
        let mut reader = SegmentReader::open(fpath.clone());
        let mut data = vec![];
        for item in reader {
            data.push(item);
        }

        // parallel sort
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(self.opts.sort_threads)
            .build()
            .unwrap();

        data.par_sort_by(|a, b| a.0.cmp(&b.0));

        // write sst file
        if data.len() == 0 {
            return;
        }

        // open sst file
        let sst_fpath = Path::new(&out_path).join(Path::new(&fpath).file_name().unwrap());
        let mut db_opts = Options::default();
        let mut blk_opts = rocksdb::BlockBasedOptions::default();
        blk_opts.set_data_block_index_type(rocksdb::DataBlockIndexType::BinaryAndHash);
        blk_opts.set_data_block_hash_ratio(0.75);
        db_opts.set_block_based_table_factory(&blk_opts);

        db_opts.set_compression_type(DBCompressionType::Lz4);

        let mut sst_writer = rocksdb::SstFileWriter::create(&db_opts);
        sst_writer.open(sst_fpath.clone()).unwrap();

        // form kv and write sst
        let mut prev = data[0].0;
        let mut buf = vec![];
        for item in data {
            if item.0 == prev {
                buf.push(item.1);
            } else {
                let mut key = vec![];
                let mut value = vec![];
                key.write(prev.to_be_bytes().as_ref()).unwrap();
                for nei in buf.iter() {
                    value.write(nei.to_be_bytes().as_ref());
                }
                buf.clear();

                // write to sst
                sst_writer.put(key, value).unwrap();
                prev = item.0;
                buf.push(item.1);
            }
        }
        if buf.len() > 0 {
            let mut key = vec![];
            let mut value = vec![];
            key.write(prev.to_be_bytes().as_ref()).unwrap();
            for nei in buf.iter() {
                value.write(nei.to_be_bytes().as_ref());
            }
            buf.clear();

            // write to sst
            sst_writer.put(key, value).unwrap();
        }

        sst_writer.finish().unwrap();
        println!("write sst file {:?} done", sst_fpath.clone());
    }

    pub async fn start(&mut self) {
        // shuffle inputs
        let mut shuffle = Shuffle::new(
            self.opts.shuffle_opts.clone(),
            self.shuffle_dir.clone(),
            self.input_dir.clone(),
            self.delimiter,
        );

        shuffle.do_shuffle().await;

        // list segments
        let mut files = vec![];
        let dir = std::fs::read_dir(self.shuffle_dir.clone()).unwrap();
        for fpath in dir {
            files.push(fpath.unwrap().path().to_str().unwrap().to_string());
        }

        // create sst folder
        std::fs::create_dir_all(self.output_dir.clone()).unwrap();

        for file in files {
            self.write_sst(file, self.output_dir.clone());
        }
    }
}
