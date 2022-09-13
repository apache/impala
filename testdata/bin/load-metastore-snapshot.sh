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
# Loads a hive metastore snapshot file to re-create its postgres database.
# A metastore snapshot file is produced as an artifact of a successful
# full data load build.

set -euo pipefail
. $IMPALA_HOME/bin/report_build_error.sh
setup_report_build_error

. ${IMPALA_HOME}/bin/impala-config.sh > /dev/null 2>&1

if [[ $# -ne 1 ]]; then
  echo "Usage: load-metastore-snapshot.sh [<metastore_snapshot_file>]"
  exit 1
fi

: ${TEST_WAREHOUSE_DIR=/test-warehouse}

SNAPSHOT_FILE=$1
if [ ! -f ${SNAPSHOT_FILE} ]; then
  echo "Metastore Snapshot file '${SNAPSHOT_FILE}' not found"
  exit 1
fi

# Copy the snapshot time to a temporary location
TMP_SNAPSHOT_FILE=/tmp/tmp-hive-metastore-snapshot.txt
rm -f ${TMP_SNAPSHOT_FILE}
cp ${SNAPSHOT_FILE} ${TMP_SNAPSHOT_FILE}

# The snapshot file has jenkins as the default user, search and replace with the current
# user (this is only useful for local environments).
# TODO: While this is safe at the moment, there is no guarentee that it will remain so.
# We're at risk is a table/column name has the string 'jenkins' in it. Find a robust way
# to do the transformation.
if [ ${USER} != "jenkins" ]; then
  echo "Searching and replacing jenkins with ${USER}"
  sed -i "s/jenkins/${USER}/g" ${TMP_SNAPSHOT_FILE}
fi


# When the tests are run on a filesystem other than hdfs, we need to change the location
# of the tables in the metastore. The location change breaks down into two cases:
#   - We use the other filesystem as a secondary filesystem. In this case, the
#     core-site.xml still point to hdfs. We need to use the FILESYSTEM_PREFIX environment
#     variable to determine the table location.
#   - We use the other filesystem as the default filesystem. In this case, we use the
#     DEFAULT_FS environment variable to determine the table locations.
if [[ "${FILESYSTEM_PREFIX}" != "" ]]; then
  echo "Changing table metadata to point to ${FILESYSTEM_PREFIX}"
  sed -i "s|hdfs://localhost:20500|${FILESYSTEM_PREFIX}|g" ${TMP_SNAPSHOT_FILE}
elif [[ "${DEFAULT_FS}" != "hdfs://localhost:20500" ]]; then
  echo "Changing table metadata to point to ${DEFAULT_FS}"
  sed -i "s|hdfs://localhost:20500|${DEFAULT_FS}|g" ${TMP_SNAPSHOT_FILE}
fi

if [[ "${WAREHOUSE_LOCATION_PREFIX}" != "" ]]; then
  echo "Adding prefix ${WAREHOUSE_LOCATION_PREFIX} to iceberg.catalog_location"
  cloc='iceberg\.catalog_location\t'
  sed -i "s|\(${cloc}\)\(${TEST_WAREHOUSE_DIR}\)|\1${WAREHOUSE_LOCATION_PREFIX}\2|g" \
      ${TMP_SNAPSHOT_FILE}
fi

# Drop and re-create the hive metastore database
dropdb -U hiveuser ${METASTORE_DB} 2> /dev/null || true
createdb -U hiveuser ${METASTORE_DB}

# Copy the contents of the SNAPSHOT_FILE
psql -q -U hiveuser ${METASTORE_DB} < ${TMP_SNAPSHOT_FILE}
# Two tables (tpch.nation and functional.alltypestiny) have cache_directive_id set in
# their metadata. These directives are now stale, and will cause any query that attempts
# to cache the data in the tables to fail.
psql -q -U hiveuser -d ${METASTORE_DB} -c \
  "delete from \"TABLE_PARAMS\" where \"PARAM_KEY\"='cache_directive_id'"
psql -q -U hiveuser -d ${METASTORE_DB} -c \
  "delete from \"PARTITION_PARAMS\" where \"PARAM_KEY\"='cache_directive_id'"
