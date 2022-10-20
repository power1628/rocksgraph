use std::sync::Arc;
use std::time::Instant;

use anyhow::Result;
use rocksdb::{self, BlockBasedOptions, DataBlockIndexType, IngestExternalFileOptions, Options};

pub const CF_VERTEX: &'static str = "vertex_cf";
pub const CF_EDGE: &'static str = "edge_cf";
type MTDB = rocksdb::DBWithThreadMode<rocksdb::MultiThreaded>;

pub struct Graph {
    pub db_opts: Options,
    pub db_path: String,
    pub db: Arc<MTDB>,
}

impl Graph {
    pub fn open(db_path: String, cache_cap: Option<usize>, use_block_index: bool) -> Self {
        let mut env = rocksdb::Env::default().unwrap();
        env.set_background_threads(8);
        let mut db_opts = Options::default();
        db_opts.create_if_missing(true);
        db_opts.create_missing_column_families(true);
        db_opts.set_env(&env);

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
        let db = MTDB::open_cf(&db_opts, &db_path, &cfs).unwrap();

        Self {
            db_opts,
            db_path,
            db: Arc::new(db),
        }
    }

    pub fn destory(opts: &Options, db_path: String) -> std::result::Result<(), rocksdb::Error> {
        MTDB::destroy(opts, db_path)
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
        Ok(())
    }
}
