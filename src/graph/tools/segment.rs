use core::num;
use histogram::Histogram;
use roaring::RoaringTreemap;
use std::{
    collections::VecDeque,
    fmt::DebugList,
    fs::File,
    io::{BufRead, BufReader},
    sync::{Arc, Mutex},
};

use crate::graph::tools::file_sharder::shard_file_to_parts;
use crate::graph::{r#type::*, tools::segment};

struct StatisTask {
    fpath: String,
    range: (u64, u64),
}

#[derive(Clone)]
struct StatisWorker {
    worker_id: usize,
    tasks: Arc<Mutex<VecDeque<StatisTask>>>,
    delimiter: char,
    // states
    histo: Histogram,
    vids: RoaringTreemap,
}

impl StatisWorker {
    pub fn new(worker_id: usize, tasks: Arc<Mutex<VecDeque<StatisTask>>>, delimiter: char) -> Self {
        Self {
            worker_id,
            tasks,
            delimiter,
            histo: Histogram::new(),
            vids: RoaringTreemap::new(),
        }
    }

    pub fn do_work(&mut self) {
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
                            self.histo.increment(ids[0]).unwrap();
                            self.vids.insert(ids[0]);
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
    }

    fn histo(&mut self) -> Histogram {
        self.histo.clone()
    }

    fn vids(&mut self) -> RoaringTreemap {
        self.vids.clone()
    }
}

struct StatisJob {
    worker_num: usize,
    input_dir: String,
    delimiter: char,
}

impl StatisJob {
    pub fn new(worker_num: usize, input_dir: String, delimiter: char) -> Self {
        Self {
            worker_num,
            input_dir,
            delimiter,
        }
    }

    fn gen_statis_tasks(&mut self) -> Mutex<VecDeque<StatisTask>> {
        let res = Mutex::new(VecDeque::new());
        let mut files = vec![];
        {
            let dir = std::fs::read_dir(self.input_dir.clone()).unwrap();
            for fpath in dir {
                files.push(fpath.unwrap().path().to_str().unwrap().to_string());
            }
        }

        for fpath in files {
            let parts = shard_file_to_parts(fpath.clone(), self.worker_num as u64).unwrap();
            for range in parts {
                let mut guard = res.lock().unwrap();
                guard.push_back(StatisTask {
                    fpath: fpath.clone(),
                    range,
                })
            }
        }

        res
    }

    pub async fn cut_to_segment(&mut self, vertex_per_segment: usize) -> Vec<(VertexId, VertexId)> {
        let tasks = Arc::new(self.gen_statis_tasks());

        // spawn workers
        let mut workers = vec![];
        for wid in 0..self.worker_num {
            let tasks = tasks.clone();
            let delimiter = self.delimiter.clone();
            let handle = tokio::task::spawn_blocking(move || {
                let mut worker = StatisWorker::new(wid, tasks, delimiter);
                worker.do_work();
                (worker.histo, worker.vids)
            });
            workers.push(handle);
        }

        // join worker
        let mut histo = Histogram::new();
        let mut vids = RoaringTreemap::new();
        for w in workers {
            let other = w.await.unwrap();
            histo.merge(&other.0);
            vids |= other.1;
        }

        // decide segment range
        let num_segment = std::cmp::max(1, vids.len() as usize / vertex_per_segment);
        let mut segments = vec![];
        let p = 1 as f64 / num_segment as f64;
        let mut prev = 0u64;
        for segid in 1..(num_segment + 1) {
            let high;
            if segid == num_segment {
                high = histo.percentile(1f64).unwrap();
            } else {
                high = histo.percentile(segid as f64 * p).unwrap();
            }
            if high < prev {
                break;
            }
            segments.push((prev, high));
            prev = high;
        }

        println!("cut to segments {:?}", segments);

        segments
    }
}

#[derive(Clone)]
pub struct SegmentMeta {
    fpath: String,
    range: (VertexId, VertexId),
}

pub struct SegmentWriter {
    meta: SegmentMeta,
    file: File,
}

impl SegmentWriter {
    pub fn open(meta: &SegmentMeta) -> Self {
        let file = File::open(meta.fpath.clone()).unwrap();
        Self {
            meta: meta.clone(),
            file,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::StatisJob;
    use tokio::runtime::Runtime;

    #[test]
    fn test_cut_to_segment() {
        let worker_num = 64;
        let fpath = String::from("/home/gaopin/dataset/twitter/edges");
        let vertex_per_segment = 1_000_000;

        let rt = Runtime::new().unwrap();

        let mut job = StatisJob::new(worker_num, fpath, ',');

        let handle = job.cut_to_segment(vertex_per_segment);
        let segment = rt.handle().block_on(handle);
        println!("{:?}", segment);
    }
}
