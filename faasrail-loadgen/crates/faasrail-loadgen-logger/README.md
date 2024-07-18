# faasrail-loadgen-logger

All options:
```console
$ target/release/faasrail-loadgen-logger --help
```

Example usage:
```console
$ RUST_LOG='trace,faasrail_loadgen=trace' target/release/faasrail-loadgen-logger --minio-address 'icy1.cslab.ece.ntua.gr:59000' -o /tmp/tmpfs/sink.out --requests /tmp/tmpfs/wreqs.json --invoc-id 10000 --csv artifacts/azure_spec_15rps_5min.csv --seed 0
```
