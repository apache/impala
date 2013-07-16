#!/bin/bash
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.

set -u

# Kill and clean data for a clean start.
$IMPALA_HOME/testdata/bin/kill-mini-llama.sh

# Starts up a MiniLlama cluster which includes:
# - HDFS with a given number of DNs
# - One Yarn ResourceManager
# - Multiple Yarn NodeManagers, exactly one per HDFS DN
# - Single Llama service
CLASSPATH=`hadoop classpath`
export MINI_LLAMA_OPTS="-Dtest.build.data=$MINI_DFS_BASE_DATA_DIR -Djava.library.path=$HADOOP_HOME/lib/native"
#export JETTY_ARGS=OPTIONS=Server,jsp
pushd ${LLAMA_HOME}
bin/minillama --hadoop-conf=$IMPALA_HOME/fe/src/test/resources/ --hadoop-nodes=3 $@
sleep 10
popd
