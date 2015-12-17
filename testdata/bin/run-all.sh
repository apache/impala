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

set -euo pipefail
trap 'echo Error in $0 at line $LINENO: $(cd "'$PWD'" && awk "NR == $LINENO" $0)' ERR

# If -format is passed, format the mini-dfs cluster.

if [[ $# -eq 1 && "$1" == "-format" ]]; then
  echo "Formatting cluster"
  HDFS_FORMAT_CLUSTER="-format"
elif [[ $# -ne 0 ]]; then
  echo "Usage: run-all.sh [-format]"
  echo "[-format] : Format the mini-dfs cluster before starting"
  exit 1
fi

# Kill and clean data for a clean start.
echo "Killing running services..."
$IMPALA_HOME/testdata/bin/kill-all.sh &>${IMPALA_TEST_CLUSTER_LOG_DIR}/kill-all.log

# Starts up a mini-cluster which includes:
# - HDFS with 3 DNs
# - One Yarn ResourceManager
# - Multiple Yarn NodeManagers, exactly one per HDFS DN
if [[ ${DEFAULT_FS} == "hdfs://localhost:20500" ]]; then
  echo "Starting all cluster services..."
  echo " --> Starting mini-DFS cluster"
  $IMPALA_HOME/testdata/bin/run-mini-dfs.sh ${HDFS_FORMAT_CLUSTER-} 2>&1 | \
      tee ${IMPALA_TEST_CLUSTER_LOG_DIR}/run-mini-dfs.log

  echo " --> Starting HBase"
  $IMPALA_HOME/testdata/bin/run-hbase.sh 2>&1 | \
      tee ${IMPALA_TEST_CLUSTER_LOG_DIR}/run-hbase.log

  echo " --> Starting Hive Server and Metastore Service"
  $IMPALA_HOME/testdata/bin/run-hive-server.sh 2>&1 | \
      tee ${IMPALA_TEST_CLUSTER_LOG_DIR}/run-hive-server.log

  echo " --> Starting the Sentry Policy Server"
  $IMPALA_HOME/testdata/bin/run-sentry-service.sh > \
      ${IMPALA_TEST_CLUSTER_LOG_DIR}/run-sentry-service.log 2>&1

elif [[ ${DEFAULT_FS} == "${LOCAL_FS}" ]]; then
  # When the local file system is used as default, we only start the Hive metastore.
  # Impala can run locally without additional services.
  $IMPALA_HOME/testdata/bin/run-hive-server.sh -only_metastore 2>&1 | \
      tee ${IMPALA_TEST_CLUSTER_LOG_DIR}/run-hive-server.log
else
  # With Isilon, we only start the Hive metastore and Sentry Policy Server.
  #   - HDFS is not started becuase Isilon is used as the defaultFs in core-site
  #   - HBase is irrelevent for Impala testing with Isilon.
  #   - We don't yet have a good way to start YARN using a different defaultFS. Moreoever,
  #     we currently don't run hive queries against Isilon for testing.
  #   - LLAMA is avoided because we cannot start YARN.
  #   - KMS is used for encryption testing, which is not available on Isilon.
  #   - Hive needs YARN, and we don't run Hive queries.
  # TODO: Figure out how to start YARN, LLAMA and Hive with a different defaultFs.
  echo " --> Starting Hive Metastore Service"
  $IMPALA_HOME/testdata/bin/run-hive-server.sh -only_metastore 2>&1 | \
      tee ${IMPALA_TEST_CLUSTER_LOG_DIR}/run-hive-server.log

  echo " --> Starting the Sentry Policy Server"
  $IMPALA_HOME/testdata/bin/run-sentry-service.sh > \
      ${IMPALA_TEST_CLUSTER_LOG_DIR}/run-sentry-service.log 2>&1
fi
