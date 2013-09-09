#!/bin/bash
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.

set -u

# Kill and clean data for a clean start.
$IMPALA_HOME/testdata/bin/kill-mini-dfs.sh

# Starts up a three-node single-process cluster; the NN listens on port 20500.
pushd ${HADOOP_HOME}
CLASSPATH=`hadoop classpath`
java -Dtest.build.data="$MINI_DFS_BASE_DATA_DIR" -Djava.library.path="$HADOOP_HOME/lib/native" org.apache.hadoop.test.MiniDFSClusterManager \
  -datanodes 3 -nnport=20500 -writeConfig=${IMPALA_HOME}/fe/target/test-classes/minicluster-conf.xml $@  &
popd
sleep 10
