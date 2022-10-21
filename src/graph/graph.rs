use std::io::Write;
use std::sync::Arc;
use std::time::Instant;

use anyhow::Result;
use bytes::Buf;
use crossbeam::channel::{unbounded, Receiver, Sender};
use roaring::RoaringTreemap;
use rocksdb::{
    self, BlockBasedOptions, CompactOptions, DBAccess, DBCompressionType, DataBlockIndexType,
    IngestExternalFileOptions, Options,
};

use super::r#type::{EdgeType, VertexId};

pub const CF_VERTEX: &'static str = "vertex_cf";
pub const CF_EDGE: &'static str = "edge_cf";
type MTDB = rocksdb::DBWithThreadMode<rocksdb::MultiThreaded>;

#[derive(Clone)]
pub struct Graph {
    pub db_opts: Options,
    pub db_path: String,
    pub db: Arc<MTDB>,
}

type EdgeIter = Vec<VertexId>;

fn raw_to_edge_iter(raw: Vec<u8>) -> EdgeIter {
    let mut data = vec![];
    let mut buf: &[u8] = raw.as_ref();
    let size = data.len() / std::mem::size_of::<VertexId>();
    for _ in 0..size {
        data.push(buf.get_u64());
    }
    data
}

impl Graph {
    pub fn open(db_path: String, cache_cap: Option<usize>, use_block_index: bool) -> Self {
        let mut env = rocksdb::Env::default().unwrap();
        env.set_background_threads(8);
        let mut db_opts = Options::default();
        db_opts.create_if_missing(true);
        db_opts.create_missing_column_families(true);
        db_opts.set_env(&env);
        db_opts.set_compression_type(DBCompressionType::Lz4);
        db_opts.enable_statistics();

        // block cache opts
        {
            let mut block_opts = BlockBasedOptions::default();
            match cache_cap {
                Some(cap) => {
                    let cache = rocksdb::Cache::new_lru_cache(cap * 1_000_000_000).unwrap();
                    block_opts.set_block_cache(&cache);
                    println!("Setting block cache to {:?} gb", cap);
                }
                _ => {}
            };
            if use_block_index {
                // should also set to gen sst files config
                block_opts.set_data_block_index_type(DataBlockIndexType::BinaryAndHash);
                block_opts.set_data_block_hash_ratio(0.75);
                println!(
                    "Setting data_block_index_type {:?}",
                    DataBlockIndexType::BinaryAndHash as i32
                );
            }
            db_opts.set_block_based_table_factory(&block_opts);
        }

        let cfs = vec![CF_VERTEX, CF_EDGE];

        //let db = MTDB::open_cf(&db_opts, &db_path, &cfs).unwrap();

        let cf_descriptors = cfs
            .clone()
            .into_iter()
            .map(|cfn| Ok(rocksdb::ColumnFamilyDescriptor::new(cfn, db_opts.clone())))
            .collect::<Result<Vec<_>>>()
            .unwrap();
        let db = MTDB::open_cf_descriptors(&db_opts, &db_path, cf_descriptors).unwrap();

        Self {
            db_opts,
            db_path,
            db: Arc::new(db),
        }
    }

    pub fn destory(opts: &Options, db_path: String) -> std::result::Result<(), rocksdb::Error> {
        MTDB::destroy(opts, db_path)
    }

    pub fn get_statistics(&self) -> Option<String> {
        self.db_opts.get_statistics()
    }

    pub fn compact(&self) -> anyhow::Result<()> {
        let mut opts = CompactOptions::default();
        opts.set_bottommost_level_compaction(rocksdb::BottommostLevelCompaction::ForceOptimized);
        opts.set_change_level(true);
        let cf = self.db.cf_handle(CF_EDGE).unwrap();
        let now = std::time::Instant::now();

        self.db
            .compact_range_cf_opt(&cf, Option::<String>::None, Option::<String>::None, &opts);
        println!("compaction cost(sec) : {:?}", now.elapsed().as_secs());
        Ok(())
    }

    pub fn ingest_sst(&self, sst_path: &str) -> anyhow::Result<()> {
        let now = Instant::now();
        println!("start ingest {:?}", sst_path);
        let dir = std::fs::read_dir(sst_path)?;
        let mut files = vec![];
        for f in dir {
            files.push(f.unwrap().path());
        }

        let mut ingest_opts = IngestExternalFileOptions::default();
        ingest_opts.set_move_files(true);

        let cf = self.db.cf_handle(CF_EDGE).unwrap();
        //TODO(power): ingest options
        self.db
            .ingest_external_file_cf_opts(&cf, &ingest_opts, files)?;
        println!("ingest done cost {:?}", now.elapsed().as_secs_f32());
        self.compact()
    }

    pub fn seek_out_edge(&self, etype: EdgeType, from: VertexId) -> anyhow::Result<EdgeIter> {
        let cf = self.db.cf_handle(CF_EDGE).unwrap();
        let mut key = vec![];
        key.write(from.to_be_bytes().as_ref());
        let value = self.db.get_cf(&cf, key)?;
        match value {
            Some(raw) => Ok(raw_to_edge_iter(raw)),
            None => Ok(EdgeIter::new()),
        }
    }

    pub fn multi_get_out_edge(
        &self,
        etype: EdgeType,
        froms: &Vec<VertexId>,
    ) -> Vec<std::result::Result<Option<Vec<u8>>, rocksdb::Error>> {
        let cf = self.db.cf_handle(CF_EDGE).unwrap();
        let mut keys = vec![];
        for from in froms {
            let mut buf = vec![];
            buf.write(from.to_be_bytes().as_ref());
            keys.push((&cf, buf));
        }

        let values = self.db.multi_get_cf(keys);

        values
    }

    pub fn decide_batch_size(data_size: usize) -> usize {
        if data_size < 100 {
            10
        } else if data_size < 20480 {
            128
        } else {
            128
        }
    }

    pub async fn kstep(
        &self,
        etype: EdgeType,
        from: VertexId,
        step: usize,
        num_tasks: usize,
    ) -> std::result::Result<usize, rocksdb::Error> {
        let mut frontier = vec![from];
        for cur_step in 0..step {
            let prev_front_size = frontier.len();
            let _tt1 = std::time::Instant::now();
            let mut prepare_cost = 0f64;

            let now = std::time::Instant::now();

            let (sx, rx) = unbounded();

            let batch_size = Graph::decide_batch_size(frontier.len());

            // spawn tasks
            let mut tasks = Vec::new();
            for _ in 0..num_tasks {
                let rx: Receiver<Vec<VertexId>> = rx.clone();
                let self_clone = self.clone();

                let task = tokio::spawn(async move {
                    let mut next_rtm = RoaringTreemap::new();
                    let mut esize = 0usize;
                    let mut cost_get = 0u128;
                    let mut cost_comp = 0u128;

                    loop {
                        match rx.recv() {
                            Ok(srcs) => {
                                let tt = Instant::now();
                                let neis_set = self_clone.multi_get_out_edge(etype, &srcs);
                                cost_get += tt.elapsed().as_micros();

                                let tt = Instant::now();
                                for neis in neis_set {
                                    match neis {
                                        Ok(value) => {
                                            let raw = value.unwrap_or_default();
                                            let mut buf: &[u8] = raw.as_ref();
                                            let size = raw.len() / std::mem::size_of::<VertexId>();
                                            for _ in 0..size {
                                                let dst = buf.get_u64();
                                                next_rtm.insert(dst);
                                                //if !next_rtm.contains(dst) {
                                                //    next_rtm.insert(dst);
                                                //}
                                                esize += 1;
                                            }
                                        }
                                        _ => {}
                                    }
                                }
                                cost_comp += tt.elapsed().as_micros();
                            }
                            _ => break,
                        }
                    }

                    println!(
                        "worker step cost(us) get {:?} comp {:?}",
                        cost_get, cost_comp
                    );
                    next_rtm
                });
                tasks.push(task);
            }

            // start sending
            for batch in frontier.chunks(batch_size) {
                let srcs = batch.to_vec();
                let _ = sx.send(srcs).unwrap();
            }
            drop(sx);

            // join results
            let mut rtm = RoaringTreemap::new();
            let mut join_cost = 0f64;
            let mut convert_cost = 0f64;

            for task in tasks {
                let task_res = task.await.unwrap();
                let tt = std::time::Instant::now();
                rtm |= task_res;
                join_cost += tt.elapsed().as_micros() as f64;
            }
            let tt = std::time::Instant::now();
            // convert rtm to frontier
            frontier.clear();
            for datum in rtm.iter() {
                frontier.push(datum);
            }
            convert_cost += tt.elapsed().as_micros() as f64;
            println!("step {:?} batch_size {:?} frontier {:?} successor {:?} cost(micro) {:?} join {:?} convert {:?} prepare_cost {:?}",
                                        cur_step,
                                        batch_size,
                                        prev_front_size,
                                        frontier.len(),
                                        now.elapsed().as_micros(),
                                        join_cost,
                                        convert_cost,
                                        prepare_cost
                                    );
        }
        Ok(frontier.len())
    }
}
