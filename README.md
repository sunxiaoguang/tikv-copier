### Build
```shell
make
```

### Help
```shell
tikv-copier --help

Usage of ./tikv-copier:
  -batch-size int
    	batch size (default 10000)
  -checkpoint-size int
    	checkpoint size (default 1000000)
  -source-mode string
    	source tikv mode: txn or raw (default "txn")
  -source-tikv string
    	source tikv address
  -state-file string
    	file to record copy state for resume (default "state.json")
  -target-mode string
    	target tikv mode: txn or raw (default "txn")
  -target-tikv string
    	target tikv address
```

### What is state file
State file saves start key for the next scan and the number of records that have been copied to target cluster. When copying is terminated for any reason, tikv-copier can resume from the state of last checkpoint and copy the left over keys.
