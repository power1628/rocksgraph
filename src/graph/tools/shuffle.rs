use std::collections::{HashMap, VecDeque};
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Mutex};

use super::file_sharder::{shard_input, InputFileShard};
use super::segment;
use crate::graph::r#type::*;
use crate::graph::tools::file_sharder;
use crate::graph::tools::segment::*;

pub struct ShuffleOptions {
    read_threads: usize,
    write_threads: usize,
}

pub fn make_exchange_channel(
    receiver_num: usize,
) -> (
    Vec<Sender<(VertexId, VertexId)>>,
    Vec<Receiver<(VertexId, VertexId)>>,
) {
    let mut txs = vec![];
    let mut rxs = vec![];

    for _ in 0..receiver_num {
        let (tx, rx) = mpsc::channel::<(VertexId, VertexId)>();
        txs.push(tx);
        rxs.push(rx);
    }
    (txs, rxs)
}

#[derive(Clone)]
pub(crate) struct ReadTaskContext {
    txs: Vec<Sender<(VertexId, VertexId)>>,
    input_shards: Arc<Mutex<VecDeque<InputFileShard>>>,
    segments: Vec<(VertexId, VertexId)>,
}

impl ReadTaskContext {
    pub fn new(
        txs: Vec<Sender<(VertexId, VertexId)>>,
        input_shards: Arc<Mutex<VecDeque<InputFileShard>>>,
        segments: Vec<(VertexId, VertexId)>,
    ) -> Self {
        Self {
            txs,
            input_shards,
            segments,
        }
    }

    pub fn next_input_shard(&mut self) -> Option<InputFileShard> {
        self.input_shards.lock().unwrap().pop_back()
    }

    pub fn push(&mut self, item: (VertexId, VertexId)) {
        let shard = match self.segments.binary_search_by(|probe| probe.0.cmp(&item.0)) {
            Ok(index) => index % self.txs.len(),
            Err(index) => index % self.txs.len(),
        };
        self.txs[shard].send(item).unwrap();
    }
}

// TODO(power): use visitor pattern.
pub struct Shuffle {
    opts: ShuffleOptions,
    shuffle_dir: String,
    input_dir: String,
    delimiter: char,
}

impl Shuffle {
    pub fn new(
        opts: ShuffleOptions,
        shuffle_dir: String,
        input_dir: String,
        delimiter: char,
    ) -> Self {
        Self {
            opts,
            shuffle_dir,
            input_dir,
            delimiter,
        }
    }

    async fn init_segments(&mut self) -> Vec<(VertexId, VertexId)> {
        let mut statis_job = StatisJob::new(
            self.opts.read_threads + self.opts.write_threads,
            self.input_dir.clone(),
            self.delimiter,
        );
        let vertex_per_segment = 1_000_000;
        let segments = statis_job.cut_to_segment(vertex_per_segment).await;
        segments
    }

    pub async fn do_shuffle(&mut self) {
        let mut read_tasks = vec![];
        let mut write_tasks = vec![];

        let input_shards = shard_input(self.input_dir.clone(), self.opts.read_threads).unwrap();

        let mut segments = self.init_segments().await;
        segments.sort_by(|a, b| a.0.cmp(&b.0));

        let mut senders = vec![];

        // start write task
        for task_id in 0..self.opts.write_threads {
            // make channel
            let (tx, rx) = mpsc::channel::<(VertexId, VertexId)>();
            senders.push(tx);
            let task_id = task_id;
            let num_task = self.opts.write_threads;
            let segments = segments.clone();
            let shuffle_dir = self.shuffle_dir.clone();

            let task = tokio::spawn(async move {
                // create segment writers
                let mut writers: HashMap<usize, SegmentWriter> = HashMap::new();
                for segid in 0..segments.len() {
                    if segid % num_task == task_id {
                        let fpath = std::path::Path::new(shuffle_dir.as_str())
                            .join(format!("{}.bin", segid).as_str())
                            .to_str()
                            .unwrap()
                            .to_string();

                        let meta = SegmentMeta {
                            fpath,
                            range: segments[segid],
                        };
                        let writer = SegmentWriter::open(&meta, 4096);
                        writers.insert(segid, writer);
                    }
                }

                // loop
                loop {
                    match rx.recv() {
                        Ok(item) => {
                            let shard =
                                match segments.binary_search_by(|probe| probe.0.cmp(&item.0)) {
                                    Ok(index) => writers
                                        .get_mut(&(index % num_task))
                                        .unwrap()
                                        .append_edge(item),
                                    Err(index) => writers
                                        .get_mut(&(index % num_task))
                                        .unwrap()
                                        .append_edge(item),
                                };
                        }
                        _ => {
                            break;
                        }
                    }
                }
            });
            write_tasks.push(task);
        }

        // start read task
        let read_context =
            ReadTaskContext::new(senders, Arc::new(Mutex::new(input_shards)), segments);
        for task_id in 0..self.opts.read_threads {
            let mut ctx = read_context.clone();
            let delimiter = self.delimiter;
            let task = tokio::spawn(async move {
                loop {
                    let task = ctx.next_input_shard();
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
                                    let ids: Vec<VertexId> = line
                                        .split(delimiter)
                                        .map(|s| s.parse::<VertexId>().unwrap())
                                        .collect();
                                    assert_eq!(ids.len(), 2);
                                    ctx.push((ids[0], ids[1]));
                                }

                                if line_bytes == 0 || bytes_readed >= task_bytes as usize {
                                    break;
                                }
                            }
                        }
                        None => {
                            break;
                        }
                    }
                }
            });
            read_tasks.push(task);
        }

        for task in read_tasks {
            task.await.unwrap();
        }

        for task in write_tasks {
            task.await.unwrap();
        }

        println!("shuffle done");
    }
}
