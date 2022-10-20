use std::sync::Arc;

use clap::Parser;
use rocksgraph::graph::graph::Graph;
use rocksgraph::graph::tools::importer::{GraphImporter, ImportOptions};
use {anyhow, tokio};
/// opts
#[derive(Parser, Debug)]
struct Cmd {
    /// cmd: gensst, loadsst
    #[clap(long, possible_values = &["gensst", "loadsst"])]
    cmd: String,

    /// rocksdb path
    #[clap(long, default_value = "./db")]
    db_path: String,

    /// options for importer
    #[clap(flatten)]
    import_opts: ImportOptions,

    /// input dir
    #[clap(long)]
    input_dir: String,

    /// output dir
    #[clap(long)]
    output_dir: String,

    /// shuffle dir
    #[clap(long)]
    shuffle_dir: String,

    /// delimiter
    #[clap(long, default_value = ",")]
    delimiter: char,
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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cmd: Cmd = Cmd::parse();

    let graph = Arc::new(Graph::open(cmd.db_path.clone(), None, true));

    if cmd.cmd == "gensst" {
        gensst(graph, &cmd).await;
    } else {
        println!("invalid cmd {:?}", cmd.cmd);
    }

    Ok(())
}
