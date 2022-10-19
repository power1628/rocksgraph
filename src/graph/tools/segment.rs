use core::num;
use std::collections::VecDeque;
use std::fmt::DebugList;
use std::fs::File;
use std::io::{BufRead, BufReader, Write};
use std::sync::{Arc, Mutex};

use histogram::Histogram;
use roaring::RoaringTreemap;

use crate::graph::r#type::*;
use crate::graph::tools::file_sharder::shard_file_to_parts;
use crate::graph::tools::segment;

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
                            let splits = line.trim().split(self.delimiter);
                            let mut ids = vec![];
                            for s in splits {
                                ids.push(s.parse::<VertexId>().expect(
                                    format!("parse line failed {:?} {:?}", line, s).as_str(),
                                ));
                            }
                            assert_eq!(ids.len(), 2);
                            self.histo.increment(ids[0]).unwrap();
                            self.vids.insert(ids[0]);
                            self.vids.insert(ids[1]);
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

pub struct StatisJob {
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
            let (min, max) = (other.0.minimum().unwrap(), other.0.maximum().unwrap());
            histo.merge(&other.0);
            let (min2, max2) = (histo.minimum().unwrap(), histo.maximum().unwrap());
            println!("{} {} | {} {}", min, max, min2, max2);
            vids |= other.1;
        }
        println!("num_v {}", vids.len());

        // decide segment range
        let num_segment = std::cmp::max(1, vids.len() as usize / vertex_per_segment);
        let mut segments = vec![];
        let p = 100 as f64 / num_segment as f64;
        let mut prev = 0u64;
        for segid in 1..(num_segment + 1) {
            let high;
            if segid == num_segment {
                high = histo.percentile(100f64).unwrap();
            } else {
                high = histo.percentile(segid as f64 * p).unwrap();
            }
            if high < prev {
                break;
            }
            println!("p {:?} {:?}", segid as f64 * p, high);
            segments.push((prev, high));
            prev = high;
        }

        println!("cut to segments {:?}", segments);

        segments
    }
}

#[derive(Clone)]
pub struct SegmentMeta {
    pub fpath: String,
    pub range: (VertexId, VertexId),
}

pub struct SegmentWriter {
    meta: SegmentMeta,
    file: File,
    buffer: Vec<(VertexId, VertexId)>,
    buffer_cap: usize,
}

///
/// segment file format:
/// <u64><u64>[<from_id><to_id>]
/// the first two u64 are the range of this segment.
impl SegmentWriter {
    pub fn open(meta: &SegmentMeta, buffer_cap: usize) -> Self {
        let mut file = File::create(meta.fpath.clone()).unwrap();
        let mut buf = vec![];
        buf.write(&meta.range.0.to_be_bytes()).unwrap();
        buf.write(&meta.range.1.to_be_bytes()).unwrap();
        file.write(&buf).unwrap();
        Self {
            meta: meta.clone(),
            file,
            buffer: Vec::with_capacity(buffer_cap),
            buffer_cap,
        }
    }

    pub fn append_edges<I>(&mut self, edges: I)
    where
        I: IntoIterator<Item = (VertexId, VertexId)>,
    {
        for item in edges {
            self.buffer.push(item);
            if self.buffer.len() > self.buffer_cap {
                self.flush();
            }
        }
    }

    pub fn append_edge(&mut self, edge: (VertexId, VertexId)) {
        self.buffer.push(edge);
        if self.buffer.len() > self.buffer_cap {
            self.flush();
        }
    }

    pub fn flush(&mut self) {
        let mut buf = vec![];
        for item in self.buffer.iter() {
            buf.write(&(item.0.to_be_bytes())).unwrap();
            buf.write(&(item.1.to_be_bytes())).unwrap();
            //buf.write(format!("{},{}\n", item.0, item.1).as_bytes())
            //    .unwrap();
        }
        self.file.write(&buf).unwrap();
        self.buffer.clear();
    }
}

impl Drop for SegmentWriter {
    fn drop(&mut self) {
        self.flush();
        self.file.sync_all().unwrap();
    }
}

#[cfg(test)]
mod tests {
    use tokio::runtime::Runtime;

    use super::StatisJob;

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
