# Simple Sort

First put the input file into `hdfs`
```bash
hadoop fs -put <input-file-local-location> <input-file-hdfs-location>
```

Then run `SimpleSort.py`.  The spark environment is pre-configured
```bash
sh run.sh <master-node-spark-address> <input-file-hdfs-location> <output-file-location>
```
it is likely that both the input file location and output file location will be in `hdfs`.

Pulling into the local environment from `hdfs` can be done with
```bash
hadoop fs -get <output-file-location> <local-download-location>
```
