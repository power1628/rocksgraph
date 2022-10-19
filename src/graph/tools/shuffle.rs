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

#[derive(Clone)]
pub struct ShuffleOptions {
    read_threads: usize,
    write_threads: usize,
}

impl Default for ShuffleOptions {
    fn default() -> Self {
        Self {
            read_threads: 4,
            write_threads: 4,
        }
    }
}

#[derive(Clone)]
pub(crate) struct ReadTaskContext {
    txs: Vec<Sender<(VertexId, VertexId)>>,
    input_shards: Arc<Mutex<VecDeque<InputFileShard>>>,
    segments: Vec<(VertexId, VertexId)>,
}

fn find_segment_id(segments: &Vec<(u64, u64)>, q: (u64, u64)) -> usize {
    match segments.binary_search_by(|probe| probe.0.cmp(&q.0)) {
        Ok(index) => index,
        Err(index) => index - 1,
    }
}

fn find_writer_id(num_writer: usize, segments: &Vec<(u64, u64)>, q: (u64, u64)) -> usize {
    find_segment_id(segments, q) % num_writer
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
        let wid = find_writer_id(self.txs.len(), &self.segments, item);
        self.txs[wid].send(item).unwrap();
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

        // create shuffle dir
        std::fs::create_dir_all(self.shuffle_dir.clone()).unwrap();

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
                println!("write-task {} start", task_id);
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
                            let segid = find_segment_id(&segments, item);
                            writers.get_mut(&segid).unwrap().append_edge(item);
                        }
                        _ => {
                            break;
                        }
                    }
                }
                println!("write-task {} finish", task_id);
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
                println!("read-task {} start", task_id);
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
                                        .trim()
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
                println!("read-task {} finish", task_id);
            });
            read_tasks.push(task);
        }

        for task in read_tasks {
            task.await.unwrap();
        }

        drop(read_context);

        for task in write_tasks {
            task.await.unwrap();
        }

        println!("shuffle done");
    }
}

#[cfg(test)]
mod tests {
    use super::{Shuffle, ShuffleOptions};

    fn binary_search(segments: &Vec<(u64, u64)>, q: (u64, u64)) -> usize {
        match segments.binary_search_by(|probe| probe.0.cmp(&q.0)) {
            Ok(index) => index,
            Err(index) => index - 1,
        }
    }

    #[test]
    fn test_find_segment() {
        let segments: Vec<(u64, u64)> = vec![
            (0, 973603),
            (973603, 5737808),
            (5737808, 7990150),
            (7990150, 11928601),
            (11928601, 14227080),
            (14227080, 14596178),
            (14596178, 15082718),
            (15082718, 15535703),
            (15535703, 16005465),
            (16005465, 16458449),
            (16458449, 16978543),
            (16978543, 17498637),
            (17498637, 18085839),
            (18085839, 18538824),
            (18538824, 19058918),
            (19058918, 19545457),
            (19545457, 19998442),
            (19998442, 20669531),
            (20669531, 21374174),
            (21374174, 22162703),
            (22162703, 23051895),
            (23051895, 24024974),
            (24024974, 25048384),
            (25048384, 26206012),
            (26206012, 27380417),
            (27380417, 28605154),
            (28605154, 30031217),
            (30031217, 31541167),
            (31541167, 33755759),
            (33755759, 35567698),
            (35567698, 37245420),
            (37245420, 39023805),
            (39023805, 40936408),
            (40936408, 42916119),
            (42916119, 44862276),
            (44862276, 46674215),
            (46674215, 48989471),
            (48989471, 51975816),
            (51975816, 55801021),
            (55801021, 61639492),
        ];

        let q0 = (0u64, 1u64);
        assert_eq!(0, binary_search(&segments, q0));

        let q1: (u64, u64) = (973602, 1);
        assert_eq!(0, binary_search(&segments, q1));

        let q2: (u64, u64) = (55801021, 1);
        assert_eq!(segments.len() - 1, binary_search(&segments, q2));

        let q3: (u64, u64) = (55801022, 1);
        assert_eq!(segments.len() - 1, binary_search(&segments, q3));
    }

    #[test]
    fn test_lj() {
        let opts = ShuffleOptions {
            read_threads: 2,
            write_threads: 2,
        };
        let input_dir = String::from("/Users/gaopin/dataset/lj");
        let shuffle_dir = String::from("/Users/gaopin/dataset/shuffle");
        let delimiter = '\t';

        let mut shuffle = Shuffle::new(opts, shuffle_dir, input_dir, delimiter);

        let rt = tokio::runtime::Runtime::new().unwrap();

        let handle = shuffle.do_shuffle();
        rt.block_on(handle);
    }
}
