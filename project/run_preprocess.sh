#!/usr/bin/env bash
# Clean + join the raw Movies_and_TV JSONL on the Spark cluster -> Parquet on HDFS.
#   ./run_preprocess.sh            full run  -> hdfs:///user/wilvalen/final_project/clean
#   ./run_preprocess.sh --quick    1% sample -> ..._quick
# Extra options pass through to preprocess.py (e.g. --data / --out / --sample 0.1).
# Override the cluster with SPARK_MASTER=..., the Python with PY=...
set -euo pipefail
cd "$(dirname "$0")"

PY=${PY:-/usr/local/python-env/py313/bin/python3}
MASTER=${SPARK_MASTER:-spark://rubens.cs.colostate.edu:30156}
LOG=preprocess.log; [[ " $* " == *" --quick "* ]] && LOG=preprocess_quick.log

PYSPARK_PYTHON=$PY PYSPARK_DRIVER_PYTHON=$PY \
spark-submit \
  --master "$MASTER" \
  --driver-memory 4g \
  --executor-memory 1800m \
  --conf spark.pyspark.python="$PY" \
  --conf spark.sql.shuffle.partitions=200 \
  --conf spark.sql.files.maxPartitionBytes=64m \
  preprocess.py "$@" 2>&1 | tee "$LOG"
