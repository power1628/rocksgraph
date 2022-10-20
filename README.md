

### import
generate sst
```
./rocksgraph --cmd gensst --input-dir ~/dataset/twitter/edges/ --output-dir ~/sst2 --shuffle-dir ./shuffle --sort-threads=60
```

ingest sst
```
./rocksgraph --cmd ingest --input-dir ~/dataset/twitter/edges/ --output-dir ~/sst2 --shuffle-dir ./shuffle
```

query
```
./rocksgraph --cmd=kstep --input-dir ./ --output-dir ./ --shuffle-dir ./ --vid 19058681 --thread 64 --kstep 7
```