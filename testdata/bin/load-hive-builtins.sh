#!/bin/bash

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

set -euo pipefail
. $IMPALA_HOME/bin/report_build_error.sh
setup_report_build_error

. ${IMPALA_HOME}/bin/impala-config.sh > /dev/null 2>&1

# TODO: remove this once we understand why Hive looks in HDFS for many of its jars

# Remove all directories in one command for efficiency
${HADOOP_HOME}/bin/hadoop fs -rm -skipTrash -r -f ${FILESYSTEM_PREFIX}${HIVE_HOME}/lib/ \
  ${FILESYSTEM_PREFIX}${HBASE_HOME}/lib/ \
  ${FILESYSTEM_PREFIX}${HADOOP_HOME}/share/hadoop/common/ \
  ${FILESYSTEM_PREFIX}${HADOOP_HOME}/share/hadoop/mapreduce/ \
  ${FILESYSTEM_PREFIX}${HADOOP_HOME}/share/hadoop/tools/lib \
  ${FILESYSTEM_PREFIX}${HADOOP_LZO}/build \
  ${FILESYSTEM_PREFIX}${SENTRY_HOME}/lib/ \
  ${FILESYSTEM_PREFIX}${IMPALA_HOME}/thirdparty/postgresql-jdbc/

TMP_DIR=$(mktemp -d)

# Create the directory structure to copy over
mkdir -p ${TMP_DIR}/${HIVE_HOME}/lib \
  ${TMP_DIR}/${HBASE_HOME}/lib \
  ${TMP_DIR}/${HADOOP_HOME}/share/hadoop/common/lib \
  ${TMP_DIR}/${HADOOP_HOME}/share/hadoop/mapreduce \
  ${TMP_DIR}/${HADOOP_HOME}/share/hadoop/tools/lib \
  ${TMP_DIR}/${HADOOP_LZO}/build \
  ${TMP_DIR}/${SENTRY_HOME}/lib \
  ${TMP_DIR}/${IMPALA_HOME}/thirdparty/postgresql-jdbc/

# Add symbolic links to files in the appropriate places
ln -s ${HIVE_HOME}/lib/*.jar ${TMP_DIR}/${HIVE_HOME}/lib
ln -s ${HBASE_HOME}/lib/*.jar ${TMP_DIR}/${HBASE_HOME}/lib
ln -s ${HADOOP_HOME}/share/hadoop/common/*.jar \
  ${TMP_DIR}/${HADOOP_HOME}/share/hadoop/common
ln -s ${HADOOP_HOME}/share/hadoop/common/lib/*.jar \
  ${TMP_DIR}/${HADOOP_HOME}/share/hadoop/common/lib
ln -s ${HADOOP_HOME}/share/hadoop/mapreduce/*.jar \
  ${TMP_DIR}/${HADOOP_HOME}/share/hadoop/mapreduce
ln -s ${HADOOP_HOME}/share/hadoop/tools/lib/*.jar \
  ${TMP_DIR}/${HADOOP_HOME}/share/hadoop/tools/lib
ln -s ${HADOOP_LZO}/build/hadoop-lzo*.jar ${TMP_DIR}/${HADOOP_LZO}/build
ln -s ${SENTRY_HOME}/lib/*.jar ${TMP_DIR}/${SENTRY_HOME}/lib
# This is the only item that uses a different path
# TODO: why is this path different?
ln -s ${POSTGRES_JDBC_DRIVER} ${TMP_DIR}/${IMPALA_HOME}/thirdparty/postgresql-jdbc

${HADOOP_HOME}/bin/hadoop fs -put ${TMP_DIR}/* ${FILESYSTEM_PREFIX}/

rm -r ${TMP_DIR}
