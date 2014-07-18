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
# Loads a test-warehouse snapshot file into HDFS. Test-warehouse snapshot files
# are produced as an artifact of each successful master Jenkins build and can be
# downloaded from the Jenkins job webpage.
#
# NOTE: Running this script will remove your existing test-warehouse directory. Be sure
# to backup any data you need before running this script.

. ${IMPALA_HOME}/bin/impala-config.sh
TEST_WAREHOUSE_HDFS_DIR=/test-warehouse

if [[ ! $1 ]]; then
  echo "Usage: load-test-warehouse-snapshot.sh [test-warehouse-SNAPSHOT.tar.gz]"
  exit 1
fi

set -u
SNAPSHOT_FILE=$1
if [ ! -f ${SNAPSHOT_FILE} ]; then
  echo "Snapshot tarball file '${SNAPSHOT_FILE}' not found"
  exit 1
fi

echo "Your existing HDFS warehouse directory (${TEST_WAREHOUSE_HDFS_DIR}) will be"\
     "removed."
read -p "Continue (y/n)? "
if [[ "$REPLY" =~ ^[Yy]$ ]]; then
  # Create a new warehouse directory. If one already exist, remove it first.
  hadoop fs -test -d ${TEST_WAREHOUSE_HDFS_DIR}
  if [ $? -eq 0 ]; then
    echo "Removing existing test-warehouse directory"
    hadoop fs -rm -r ${TEST_WAREHOUSE_HDFS_DIR}
  fi
  echo "Creating test-warehouse directory"
  hadoop fs -mkdir ${TEST_WAREHOUSE_HDFS_DIR}
else
  echo -e "\nAborting."
  exit 1
fi

set -e
echo "Loading hive builtins"
${IMPALA_HOME}/testdata/bin/load-hive-builtins.sh

echo "Loading snapshot file: ${SNAPSHOT_FILE}"
SNAPSHOT_STAGING_DIR=`dirname ${SNAPSHOT_FILE}`/hdfs-staging-tmp
rm -rf ${SNAPSHOT_STAGING_DIR}
mkdir ${SNAPSHOT_STAGING_DIR}

echo "Extracting tarball"
tar -C ${SNAPSHOT_STAGING_DIR} -xzf ${SNAPSHOT_FILE}

echo "Copying data to HDFS"
hadoop fs -put ${SNAPSHOT_STAGING_DIR}/test-warehouse/* ${TEST_WAREHOUSE_HDFS_DIR}

echo "Cleaning up external hbase tables"
${IMPALA_HOME}/bin/create_testdata.sh
hadoop fs -rm -r -f ${TEST_WAREHOUSE_HDFS_DIR}/functional_hbase.db

echo "Cleaning up workspace"
rm -rf ${SNAPSHOT_STAGING_DIR}
