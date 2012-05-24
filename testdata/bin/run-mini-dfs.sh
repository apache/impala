#!/bin/bash
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.

# Kill and clean data for a clean start.
$IMPALA_HOME/testdata/bin/kill-mini-dfs.sh

# Starts up a three-node single-process cluster; the NN listens on port 20500.
CLASSPATH=
for jar in `find ${IMPALA_HOME}/thirdparty/hadoop-2.0.0-cdh4.1.0-SNAPSHOT/ -name "*.jar"`; do
  CLASSPATH=${CLASSPATH}:${jar}
done;
pushd ${HADOOP_HOME}
java org.apache.hadoop.test.MiniDFSClusterManager -datanodes 3 -nnport=20500 $@ &
popd
sleep 10
