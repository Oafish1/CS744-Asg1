# Simple Sort

First put the input file into `hdfs`
```bash
hadoop fs -put export.csv /
```

Then run `SimpleSort.py`.  The spark environment is pre-configured
```bash
spark-submit SimpleSort.py export.csv output
```

The output can be found in the `hdfs` directory `output`.  Pulling into the local environment can be done with
```bash
hadoop fs -get /output .
```