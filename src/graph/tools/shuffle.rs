pub struct ShuffleOptions {
    read_threads: usize,
    write_threads: usize,
}

pub struct Shuffle {
    opts: ShuffleOptions,
    shuffle_dir: String,
}
