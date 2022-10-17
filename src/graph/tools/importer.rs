///! Graph Importer
use std::{
    borrow::BorrowMut,
    collections::VecDeque,
    fmt::DebugList,
    fs::File,
    io::{BufRead, BufReader, Read, Seek, SeekFrom},
    ops::Deref,
    sync::{Arc, Mutex},
};

use crate::graph::r#type::*;
use crate::graph::tools::file_sharder::shard_file_to_parts;

/// Stage 1 : get vertex statistics, decide how many buckets, and bucket range.
/// Make sure each bucket holds a maximum of given vertices, so the bucket can
/// be kept in memory and do in-memory sorting.

/// Stage 2 : write file to buckets
/// read worker ( read files and parse file to (from, to) pairs )
/// with range partition, send pair to buckets
///    |
///    V
/// write worker ( recv pair from channel and write to buckets),
/// each worker in charge of some bucket sorting and writing.
///    |
///    V
/// bucket ( each bucket has an underlying file as durable storage )

/// Stage 3 : read buckets and make kv pairs and write to sst files.

struct MapTask {
    pub fpath: String,
    pub range: (u64, u64),
}

fn gen_map_tasks(input_dir: String, shard_num: usize) -> Mutex<VecDeque<MapTask>> {
    let res = Mutex::new(VecDeque::new());
    let mut files = vec![];
    {
        let dir = std::fs::read_dir(input_dir).unwrap();
        for fpath in dir {
            files.push(fpath.unwrap().path().to_str().unwrap().to_string());
        }
    }

    for fpath in files {
        let parts = shard_file_to_parts(fpath.clone(), shard_num as u64).unwrap();
        for range in parts {
            let mut guard = res.lock().unwrap();
            guard.push_back(MapTask {
                fpath: fpath.clone(),
                range,
            })
        }
    }

    res
}

struct MapWorker {
    tasks: Arc<Mutex<VecDeque<MapTask>>>,
    shuffle_dir: String,
    shuffle_id: usize,
    buffer_bytes: usize,
    task_id: usize,
    delimiter: char,

    buffer: Vec<(u64, u64)>,
    used_bytes: usize,
}

impl MapWorker {
    pub fn new(
        task_id: usize,
        tasks: Arc<Mutex<VecDeque<MapTask>>>,
        shuffle_dir: String,
        buffer_bytes: usize,
        delimiter: char,
    ) -> Self {
        Self {
            task_id,
            tasks,
            shuffle_dir,
            shuffle_id: 0usize,
            buffer_bytes,
            buffer: Vec::new(),
            used_bytes: 0usize,
            delimiter,
        }
    }

    fn flush(&mut self) {}

    fn do_task(&mut self) {
        loop {
            let task = self.tasks.lock().unwrap().pop_back();
            match task {
                Some(task) => {
                    let file = File::open(task.fpath).unwrap();
                    let range = task.range;
                    let task_bytes = range.1 - range.0;
                    let mut bytes_readed = 0usize;
                    let mut reader = BufReader::new(file);
                    reader.seek_relative(range.0 as i64).unwrap();
                    loop {
                        let mut line = String::new();
                        let line_bytes = reader.read_line(&mut line).unwrap();
                        bytes_readed += line_bytes;
                        if line_bytes > 0 {
                            // process line
                            let ids: Vec<_> = line
                                .split(self.delimiter)
                                .map(|s| s.parse::<VertexId>().unwrap())
                                .collect();
                            assert_eq!(ids.len(), 2);
                        }

                        if line_bytes == 0 || bytes_readed >= task_bytes as usize {
                            break;
                        }
                    }
                }
                None => {
                    self.flush();
                    break;
                }
            }
        }
    }
}

pub struct ImportOptions {
    parallelism: usize,
}

pub struct GraphImporter {
    opts: ImportOptions,
    input_dir: String,
    output_dir: String,
}

impl GraphImporter {
    pub fn new(input_dir: String, output_dir: String, opts: ImportOptions) -> Self {
        Self {
            opts,
            input_dir,
            output_dir,
        }
    }

    async fn start_map_task(&self) -> std::result::Result<(), std::io::Error> {
        let tasks = gen_map_tasks(self.input_dir.clone(), self.opts.parallelism);

        todo!()
    }
}
