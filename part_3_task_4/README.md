# Page Rank

First put the input file(s) into `hdfs`.

Then run the script on the main `spark` node
```bash
sh run.sh
```

# Kill a Worker

On one of the worker nodes, the following command can be run to simulate worker failure
```bash
sudo sh -c "sync; echo 3 > /proc/sys/vm/drop_caches"
```
