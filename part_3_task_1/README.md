# Page Rank

First load the input file(s) into `hdfs`
```bash
hadoop fs -put <local-input-file-location> /<hdfs-upload-location>
```

Then run the script on the main `spark` node
```bash
sh run.sh <master-node-spark-address> <input-file-location> <OPTIONAL: output-file-location>
```
it is likely that both the input file location and output file location will be in `hdfs`.
