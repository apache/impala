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
HDFS_FORMAT_CLUSTER=""
if [ "$1" == "-format" ]; then
  echo "Formatting cluster"
  HDFS_FORMAT_CLUSTER="-format"
elif [[ $1 ]]; then
  echo "Usage: run-all.sh [-format]"
  echo "[-format] : Format the mini-dfs cluster before starting"
  exit 1
fi

# Kill and clean data for a clean start.
$IMPALA_HOME/testdata/bin/kill-all.sh

# Start up DFS, then Hbase
echo "Starting all cluster services..."
$IMPALA_HOME/testdata/bin/run-mini-dfs.sh ${HDFS_FORMAT_CLUSTER}
$IMPALA_HOME/testdata/bin/run-hbase.sh

$IMPALA_HOME/testdata/bin/run-hive-server.sh
