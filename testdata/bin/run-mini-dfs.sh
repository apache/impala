#!/bin/bash
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.

# Kill and clean data for a clean start.
$IMPALA_HOME/testdata/bin/kill-mini-dfs.sh

# Starts up a three-node single-process cluster; the NN listens on port 20500.
pushd ${HADOOP_HOME}
hadoop org.apache.hadoop.test.MiniDFSClusterManager -datanodes 3 -nnport=20500 $@ &
popd
sleep 10
