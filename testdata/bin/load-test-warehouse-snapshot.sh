#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
# Loads a test-warehouse snapshot file into HDFS. Test-warehouse snapshot files
# are produced as an artifact of each successful master Jenkins build and can be
# downloaded from the Jenkins job webpage.
#
# NOTE: Running this script will remove your existing test-warehouse directory. Be sure
# to backup any data you need before running this script.

set -euo pipefail
. $IMPALA_HOME/bin/report_build_error.sh
setup_report_build_error

. ${IMPALA_HOME}/bin/impala-config.sh > /dev/null 2>&1
: ${REMOTE_LOAD:=}

if [[ $# -ne 1 ]]; then
  echo "Usage: load-test-warehouse-snapshot.sh [test-warehouse-SNAPSHOT.tar.gz]"
  exit 1
fi

: ${TEST_WAREHOUSE_DIR=/test-warehouse}

SNAPSHOT_FILE=$1
if [ ! -f ${SNAPSHOT_FILE} ]; then
  echo "Snapshot tarball file '${SNAPSHOT_FILE}' not found"
  exit 1
fi

if [[ -z "$REMOTE_LOAD" ]]; then
  echo "Your existing ${TARGET_FILESYSTEM} warehouse directory " \
    "(${FILESYSTEM_PREFIX}${TEST_WAREHOUSE_DIR} will be removed."
  read -p "Continue (y/n)? "
else
  REPLY=y
fi
if [[ "$REPLY" =~ ^[Yy]$ ]]; then
  # Create a new warehouse directory. If one already exist, remove it first.
  if [ "${TARGET_FILESYSTEM}" = "s3" ]; then
    # TODO: The aws cli emits a lot of spew, redirect /dev/null once it's deemed stable.
    if ! aws s3 rm --recursive s3://${S3_BUCKET}${TEST_WAREHOUSE_DIR}; then
      echo "Deleting pre-existing data in s3 failed, aborting."
      exit 1
    fi
    if [[ "${S3GUARD_ENABLED}" = "true" ]]; then
      # Initialize the s3guard dynamodb table and clear it out. This is valid even if
      # the table already exists.
      hadoop s3guard init -meta "dynamodb://${S3GUARD_DYNAMODB_TABLE}" \
        -region "${S3GUARD_DYNAMODB_REGION}"
      hadoop s3guard prune -seconds 1 -meta "dynamodb://${S3GUARD_DYNAMODB_TABLE}" \
        -region "${S3GUARD_DYNAMODB_REGION}"
    fi
  else
    # Either isilon or hdfs, no change in procedure.
    if hadoop fs -test -d ${FILESYSTEM_PREFIX}${TEST_WAREHOUSE_DIR}; then
      echo "Removing existing ${TEST_WAREHOUSE_DIR} directory contents"
      # For filesystems that don't allow 'rm' without 'x', chmod to 777 for the
      # subsequent 'rm -r'.
      if [ "${TARGET_FILESYSTEM}" = "isilon" ] || \
          [ "${TARGET_FILESYSTEM}" = "local" ]; then
        hadoop fs -chmod -R 777 ${FILESYSTEM_PREFIX}${TEST_WAREHOUSE_DIR}
      fi
      hadoop fs -rm -f -r -skipTrash "${FILESYSTEM_PREFIX}${TEST_WAREHOUSE_DIR}/*"
    else
      # Only create the directory if it doesn't exist. Some filesystems - such as Ozone -
      # are created with extra properties.
      echo "Creating ${TEST_WAREHOUSE_DIR} directory"
      hadoop fs -mkdir -p ${FILESYSTEM_PREFIX}${TEST_WAREHOUSE_DIR}
    fi
    if [[ -n "${HDFS_ERASURECODE_POLICY:-}" ]]; then
      hdfs ec -enablePolicy -policy "${HDFS_ERASURECODE_POLICY}"
      hdfs ec -setPolicy -policy "${HDFS_ERASURECODE_POLICY}" \
        -path "${HDFS_ERASURECODE_PATH:=/test-warehouse}"
    fi

    # TODO: commented out because of regressions in local end-to-end testing. See
    # IMPALA-4345
    #
    # hdfs dfs -chmod 1777 ${FILESYSTEM_PREFIX}${TEST_WAREHOUSE_DIR}
  fi
else
  echo -e "\nAborting."
  exit 1
fi

echo "Loading snapshot file: ${SNAPSHOT_FILE}"
SNAPSHOT_STAGING_DIR=`dirname ${SNAPSHOT_FILE}`/hdfs-staging-tmp
rm -rf ${SNAPSHOT_STAGING_DIR}
mkdir ${SNAPSHOT_STAGING_DIR}

echo "Extracting tarball"
tar -C ${SNAPSHOT_STAGING_DIR} -xzf ${SNAPSHOT_FILE}

if [ ! -f ${SNAPSHOT_STAGING_DIR}${TEST_WAREHOUSE_DIR}/githash.txt ]; then
  echo "The test-warehouse snapshot does not contain a githash.txt file, aborting load"
  exit 1
fi

if [ "${TARGET_FILESYSTEM}" != "hdfs" ]; then
  # Need to rewrite test metadata regardless of ${WAREHOUSE_LOCATION_PREFIX} because
  # paths can have "hdfs://" scheme
  echo "Updating Iceberg locations with warehouse prefix ${WAREHOUSE_LOCATION_PREFIX}"
  ${IMPALA_HOME}/testdata/bin/rewrite-iceberg-metadata.py "${WAREHOUSE_LOCATION_PREFIX}" \
      $(find ${SNAPSHOT_STAGING_DIR}${TEST_WAREHOUSE_DIR}/ -name "metadata")
fi

echo "Copying data to ${TARGET_FILESYSTEM}"
if [ "${TARGET_FILESYSTEM}" = "s3" ]; then
  # hive does not yet work well with s3, so we won't need hive builtins.
  # TODO: The aws cli emits a lot of spew, redirect /dev/null once it's deemed stable.
  if ! aws s3 cp --recursive ${SNAPSHOT_STAGING_DIR}${TEST_WAREHOUSE_DIR} \
      s3://${S3_BUCKET}${TEST_WAREHOUSE_DIR}; then
    echo "Copying the test-warehouse to s3 failed, aborting."
    exit 1
  fi
elif [ "${TARGET_FILESYSTEM}" = "gs" ]; then
  # Authenticate with the service account before using gsutil
  gcloud auth activate-service-account --key-file "$GOOGLE_APPLICATION_CREDENTIALS"
  # Parallelly(-m) upload files
  if ! gsutil -m -q cp -r ${SNAPSHOT_STAGING_DIR}${TEST_WAREHOUSE_DIR} \
      gs://${GCS_BUCKET}; then
    echo "Copying the test-warehouse to GCS failed, aborting."
    exit 1
  fi
else
    hadoop fs -put ${SNAPSHOT_STAGING_DIR}${TEST_WAREHOUSE_DIR}/* ${FILESYSTEM_PREFIX}${TEST_WAREHOUSE_DIR}
fi

${IMPALA_HOME}/bin/create_testdata.sh
echo "Cleaning up external hbase tables"
hadoop fs -rm -r -f ${FILESYSTEM_PREFIX}${TEST_WAREHOUSE_DIR}/functional_hbase.db

echo "Cleaning up workspace"
rm -rf ${SNAPSHOT_STAGING_DIR}
