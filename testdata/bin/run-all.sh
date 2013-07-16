#!/bin/bash
# Copyright 2012 Cloudera Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Starts up a mini-dfs test cluster and related services

# If -format is passed, format the mini-dfs cluster.

HDFS_FORMAT_CLUSTER="--no-format"
if [ "$1" == "-format" ]; then
  echo "Formatting cluster"
  HDFS_FORMAT_CLUSTER=""
elif [[ $1 ]]; then
  echo "Usage: run-all.sh [-format]"
  echo "[-format] : Format the mini-dfs cluster before starting"
  exit 1
fi

set -u

# Kill and clean data for a clean start.
echo "Killing running services..."
$IMPALA_HOME/testdata/bin/kill-all.sh &>${IMPALA_TEST_CLUSTER_LOG_DIR}/kill-all.log

# Starts up a MiniLlama cluster which includes:
# - HDFS with a given number of DNs
# - One Yarn ResourceManager
# - Multiple Yarn NodeManagers, exactly one per HDFS DN
# - Single Llama service
echo "Starting all cluster services..."
echo " --> Starting mini-Llama cluster"
$IMPALA_HOME/testdata/bin/run-mini-llama.sh ${HDFS_FORMAT_CLUSTER}\
    &>${IMPALA_TEST_CLUSTER_LOG_DIR}/run-mini-llama.log

echo " --> Starting HBase"
$IMPALA_HOME/testdata/bin/run-hbase.sh &>${IMPALA_TEST_CLUSTER_LOG_DIR}/run-hbase.log

echo " --> Starting Hive Server and Metastore Service"
$IMPALA_HOME/testdata/bin/run-hive-server.sh\
    &>${IMPALA_TEST_CLUSTER_LOG_DIR}/run-hive-server.log
