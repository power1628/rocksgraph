### About
Experimental graph store based on rocksdb.
Storage model:
```
Key: VertexId
Value: [VertexId]
```
VertexId are encoded to big endian string.

### Importing Data
Step 1: Generate SST file
```
./rocksgraph --cmd gensst --input-dir ~/dataset/twitter/edges/ --output-dir ~/sst2 --shuffle-dir ./shuffle --sort-threads=60
```

Step 2: Ingest SST file to RocksDB.
```
./rocksgraph --cmd ingest --input-dir ~/dataset/twitter/edges/ --output-dir ~/sst2 --shuffle-dir ./shuffle
```

### Query
Currently only support KStep Query.
```
./rocksgraph --cmd=kstep --vid 19058681 --thread 64 --kstep 7
```

### Some Results
```
➜  release git:(main) ✗ ./rocksgraph --cmd=kstep --input-dir ./ --output-dir ./ --shuffle-dir ./ --vid 19058681 --thread 64 --kstep 7
Cmd { cmd: "kstep", db_path: "./db", import_opts: ImportOptions { shuffle_opts: ShuffleOptions { shuffle_read_threads: 16, shuffle_write_threads: 16 }, read_threads: 1, sort_threads: 32, write_threads: 1 }, input_dir: "./", output_dir: "./", shuffle_dir: "./", delimiter: ',', vid: 19058681, kstep: 7, thread: 64 }
Setting data_block_index_type 1
step 0 batch_size 10 frontier 1 successor 2997469 cost(micro) 340310 join 153.0 convert 107360.0 prepare_cost 0.0
step 1 batch_size 128 frontier 2997469 successor 22244464 cost(micro) 4802158 join 303998.0 convert 292296.0 prepare_cost 0.0
step 2 batch_size 128 frontier 22244464 successor 33231294 cost(micro) 12202333 join 279199.0 convert 548819.0 prepare_cost 0.0
step 3 batch_size 128 frontier 33231294 successor 34819992 cost(micro) 14347093 join 289826.0 convert 397841.0 prepare_cost 0.0
step 4 batch_size 128 frontier 34819992 successor 34997181 cost(micro) 14783989 join 307310.0 convert 405858.0 prepare_cost 0.0
step 5 batch_size 128 frontier 34997181 successor 35013961 cost(micro) 14875379 join 296591.0 convert 402085.0 prepare_cost 0.0
step 6 batch_size 128 frontier 35013961 successor 35015863 cost(micro) 14622117 join 306284.0 convert 404419.0 prepare_cost 0.0
```
