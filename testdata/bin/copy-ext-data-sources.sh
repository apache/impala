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
# This script copies the test data source library into hdfs.

set -euo pipefail
. $IMPALA_HOME/bin/report_build_error.sh
setup_report_build_error

. ${IMPALA_HOME}/bin/impala-config.sh > /dev/null 2>&1

EXT_DATA_SOURCE_SRC_PATH=${IMPALA_HOME}/java/ext-data-source
EXT_DATA_SOURCES_HDFS_PATH=${FILESYSTEM_PREFIX}/test-warehouse/data-sources
JDBC_DRIVERS_HDFS_PATH=${EXT_DATA_SOURCES_HDFS_PATH}/jdbc-drivers

hadoop fs -mkdir -p ${EXT_DATA_SOURCES_HDFS_PATH}
hadoop fs -mkdir -p ${JDBC_DRIVERS_HDFS_PATH}

# Copy libraries of external data sources to HDFS
hadoop fs -put -f \
  ${EXT_DATA_SOURCE_SRC_PATH}/test/target/impala-data-source-test-*.jar \
  ${EXT_DATA_SOURCES_HDFS_PATH}/test-data-source.jar

echo "Copied" ${EXT_DATA_SOURCE_SRC_PATH}/test/target/impala-data-source-test-*.jar \
  "into HDFS" ${EXT_DATA_SOURCES_HDFS_PATH}

# Copy Postgres JDBC driver to HDFS
hadoop fs -put -f \
  ${IMPALA_HOME}/fe/target/dependency/postgresql-*.jar \
  ${JDBC_DRIVERS_HDFS_PATH}/postgresql-jdbc.jar

echo "Copied" ${IMPALA_HOME}/fe/target/dependency/postgresql-*.jar \
  "into HDFS" ${JDBC_DRIVERS_HDFS_PATH}

# Extract URI scheme from DEFAULT_FS environment variable.
FILESYSTEM_URI_SCHEME="hdfs"
if [ ! -z "$DEFAULT_FS" ] && [[ $DEFAULT_FS =~ ":" ]]; then
  FILESYSTEM_URI_SCHEME=`echo $DEFAULT_FS | cut -d \: -f 1`
fi
# Delete hivuser if it already exists in the jceks file
if [ $(hadoop credential list -provider jceks://${FILESYSTEM_URI_SCHEME}/test-warehouse/\
data-sources/test.jceks | grep -c 'hiveuser') -eq 1 ]; then
  hadoop credential delete hiveuser -f -provider jceks://${FILESYSTEM_URI_SCHEME}/\
test-warehouse/data-sources/test.jceks > /dev/null 2>&1
fi

# Store password in a Java keystore file on HDFS
hadoop credential create hiveuser -provider \
  jceks://${FILESYSTEM_URI_SCHEME}/test-warehouse/data-sources/test.jceks -v password\
  > /dev/null 2>&1

# Download Impala JDBC driver
${IMPALA_HOME}/testdata/bin/download-impala-jdbc-driver.sh
