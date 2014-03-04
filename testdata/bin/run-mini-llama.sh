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
# Workaround for https://jira.cloudera.com/browse/CDH-17751
# A core-site.xml got introduced in the YARN test jar upstream. This jar was prefixed to
# the classpath before starting minillama, causing settings in /fe/src/test/resources to
# be ignored. The workaround can be removed when the next thirdparty update is made in
# CDH5.
export MINI_LLAMA_CLASSPATH="${IMPALA_HOME}/fe/src/test/resources"
pushd ${LLAMA_HOME}

echo "Running mini llama"
bin/minillama minicluster -nodes 3 -hdfswriteconf ${HADOOP_CONF_DIR}/minicluster-conf.xml $@ &
sleep 10
popd
