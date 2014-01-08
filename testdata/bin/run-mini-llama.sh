#!/bin/bash
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.

export HADOOP_CLIENT_OPTS="${HADOOP_CLIENT_OPTS} -Dllama.server.log.dir=${IMPALA_HOME}/cluster_logs"
set -u

# Kill and clean data for a clean start.
$IMPALA_HOME/testdata/bin/kill-mini-llama.sh

# Starts up a MiniLlama cluster which includes:
# - HDFS with a given number of DNs
# - One Yarn ResourceManager
# - Multiple Yarn NodeManagers, exactly one per HDFS DN
# - Single Llama service
CLASSPATH=`hadoop classpath`
export MINI_LLAMA_OPTS="-Dtest.build.data=$MINI_DFS_BASE_DATA_DIR -Djava.library.path=${HADOOP_HOME}/lib/native"
pushd ${LLAMA_HOME}

echo "Running mini llama"
bin/minillama minicluster -nodes 3 -hdfswriteconf ${HADOOP_CONF_DIR}/minicluster-conf.xml $@ &
sleep 10
popd
