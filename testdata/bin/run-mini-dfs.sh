#!/bin/bash
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.

# Starts up a three-node single-process cluster; the NN listens on port 20500.

./kill-mini-dfs.sh > /dev/null 2>&1
CLASSPATH=
for jar in `find ${IMPALA_HOME}/thirdparty/hadoop-0.23.0-cdh4b2-SNAPSHOT/ -name "*.jar"`; do
  CLASSPATH=${CLASSPATH}:${jar}
done;


pushd ${HADOOP_HOME}
java org.apache.hadoop.test.MiniHadoopClusterManager -datanodes 3 -nomr -nnport=20500  &
popd
sleep 10
