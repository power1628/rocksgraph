use std::sync::Arc;

use clap::Parser;
use rocksgraph::graph::graph::Graph;
use rocksgraph::graph::r#type::EdgeType;
use rocksgraph::graph::tools::importer::{GraphImporter, ImportOptions};
use {anyhow, tokio};

/// opts
#[derive(Parser, Debug)]
struct Cmd {
    /// cmd
    #[clap(long, possible_values = &["gensst", "ingest", "kstep"])]
    cmd: String,

    /// rocksdb path
    #[clap(long, default_value = "./db")]
    db_path: String,

    /// options for importer
    #[clap(flatten)]
    import_opts: ImportOptions,

    /// input dir
    #[clap(long, default_value = "./")]
    input_dir: String,

    /// output dir
    #[clap(long, default_value = "./")]
    output_dir: String,

    /// shuffle dir
    #[clap(long, default_value = "./")]
    shuffle_dir: String,

    /// delimiter
    #[clap(long, default_value = ",")]
    delimiter: char,

    /// from vertex id
    #[clap(long, default_value = "0")]
    vid: u64,

    /// kstep
    #[clap(long, default_value = "1")]
    kstep: usize,

    /// query threads
    #[clap(long, default_value = "10")]
    thread: usize,
}

async fn gensst(graph: Arc<Graph>, cmd: &Cmd) {
    let mut importer = GraphImporter::new(
        cmd.input_dir.clone(),
        cmd.output_dir.clone(),
        cmd.shuffle_dir.clone(),
        cmd.import_opts.clone(),
        cmd.delimiter,
        graph,
    );

    importer.start().await;
}

async fn ingest(graph: Arc<Graph>, cmd: &Cmd) -> anyhow::Result<()> {
    graph.ingest_sst(cmd.output_dir.as_str())
}

async fn kstep(graph: Arc<Graph>, cmd: &Cmd) -> anyhow::Result<()> {
    let etype = 1 as EdgeType;
    let kstep_size = graph.kstep(etype, cmd.vid, cmd.kstep, cmd.thread).await?;
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cmd: Cmd = Cmd::parse();
    println!("{:?}", cmd);

    let graph = Arc::new(Graph::open(cmd.db_path.clone(), None, true));

    if cmd.cmd == "gensst" {
        gensst(graph, &cmd).await;
    } else if cmd.cmd == "ingest" {
        ingest(graph, &cmd).await?;
    } else if cmd.cmd == "kstep" {
        kstep(graph, &cmd).await?;
    } else {
        println!("invalid cmd {:?}", cmd.cmd);
    }

    Ok(())
}
