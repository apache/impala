#!/bin/bash
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.

# If first argument is set, reformat the cluster, else try to load
# what was already there
HDFS_FORMAT_CLUSTER=""
if [ "x$1" != "x" ]; then
  echo "Formatting cluster"
  HDFS_FORMAT_CLUSTER="-format"
fi

# Kill and clean data for a clean start.
$IMPALA_HOME/testdata/bin/kill-all.sh

# Start up DFS, then Hbase
$IMPALA_HOME/testdata/bin/run-mini-dfs.sh ${HDFS_FORMAT_CLUSTER}
$IMPALA_HOME/testdata/bin/run-hbase.sh

$IMPALA_HOME/testdata/bin/run-hive-server.sh
