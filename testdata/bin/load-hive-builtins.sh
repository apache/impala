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
trap 'echo Error in $0 at line $LINENO: $(cd "'$PWD'" && awk "NR == $LINENO" $0)' ERR

. ${IMPALA_HOME}/bin/impala-config.sh > /dev/null 2>&1

# TODO: remove this once we understand why Hive looks in HDFS for many of its jars
${HADOOP_HOME}/bin/hadoop fs -rm -r -f ${FILESYSTEM_PREFIX}${HIVE_HOME}/lib/
${HADOOP_HOME}/bin/hadoop fs -mkdir -p ${FILESYSTEM_PREFIX}${HIVE_HOME}/lib/
${HADOOP_HOME}/bin/hadoop fs -put ${HIVE_HOME}/lib/*.jar ${FILESYSTEM_PREFIX}${HIVE_HOME}/lib/

${HADOOP_HOME}/bin/hadoop fs -rm -r -f ${FILESYSTEM_PREFIX}${HBASE_HOME}/lib/
${HADOOP_HOME}/bin/hadoop fs -mkdir -p ${FILESYSTEM_PREFIX}${HBASE_HOME}/lib/
${HADOOP_HOME}/bin/hadoop fs -put ${HBASE_HOME}/lib/*.jar ${FILESYSTEM_PREFIX}${HBASE_HOME}/lib/

${HADOOP_HOME}/bin/hadoop fs -rm -r -f ${FILESYSTEM_PREFIX}${HADOOP_HOME}/share/hadoop/common/
${HADOOP_HOME}/bin/hadoop fs -mkdir -p ${FILESYSTEM_PREFIX}${HADOOP_HOME}/share/hadoop/common/
${HADOOP_HOME}/bin/hadoop fs -put ${HADOOP_HOME}/share/hadoop/common/*.jar \
    ${FILESYSTEM_PREFIX}${HADOOP_HOME}/share/hadoop/common/
${HADOOP_HOME}/bin/hadoop fs -rm -r -f ${FILESYSTEM_PREFIX}${HADOOP_HOME}/share/hadoop/common/lib/
${HADOOP_HOME}/bin/hadoop fs -mkdir -p ${FILESYSTEM_PREFIX}${HADOOP_HOME}/share/hadoop/common/lib/
${HADOOP_HOME}/bin/hadoop fs -put ${HADOOP_HOME}/share/hadoop/common/lib/*.jar \
    ${FILESYSTEM_PREFIX}${HADOOP_HOME}/share/hadoop/common/lib/
${HADOOP_HOME}/bin/hadoop fs -rm -r -f ${FILESYSTEM_PREFIX}${HADOOP_HOME}/share/hadoop/mapreduce/
${HADOOP_HOME}/bin/hadoop fs -mkdir -p ${FILESYSTEM_PREFIX}${HADOOP_HOME}/share/hadoop/mapreduce/
${HADOOP_HOME}/bin/hadoop fs -put ${HADOOP_HOME}/share/hadoop/mapreduce/*.jar \
    ${FILESYSTEM_PREFIX}${HADOOP_HOME}/share/hadoop/mapreduce/
${HADOOP_HOME}/bin/hadoop fs -rm -r -f ${FILESYSTEM_PREFIX}${HADOOP_HOME}/share/hadoop/tools/lib
${HADOOP_HOME}/bin/hadoop fs -mkdir -p ${FILESYSTEM_PREFIX}${HADOOP_HOME}/share/hadoop/tools/lib
${HADOOP_HOME}/bin/hadoop fs -put ${HADOOP_HOME}/share/hadoop/tools/lib/*.jar \
    ${FILESYSTEM_PREFIX}${HADOOP_HOME}/share/hadoop/tools/lib/

${HADOOP_HOME}/bin/hadoop fs -rm -r -f ${FILESYSTEM_PREFIX}${HADOOP_LZO}/build
${HADOOP_HOME}/bin/hadoop fs -mkdir -p ${FILESYSTEM_PREFIX}${HADOOP_LZO}/build
${HADOOP_HOME}/bin/hadoop fs -put ${HADOOP_LZO}/build/hadoop-lzo*.jar \
    ${FILESYSTEM_PREFIX}${HADOOP_LZO}/build/

${HADOOP_HOME}/bin/hadoop fs -rm -r -f ${FILESYSTEM_PREFIX}${SENTRY_HOME}/lib/
${HADOOP_HOME}/bin/hadoop fs -mkdir -p ${FILESYSTEM_PREFIX}${SENTRY_HOME}/lib/
${HADOOP_HOME}/bin/hadoop fs -put ${SENTRY_HOME}/lib/*.jar ${FILESYSTEM_PREFIX}${SENTRY_HOME}/lib/

${HADOOP_HOME}/bin/hadoop fs -rm -r -f ${FILESYSTEM_PREFIX}${IMPALA_HOME}/thirdparty/postgresql-jdbc/
${HADOOP_HOME}/bin/hadoop fs -mkdir -p ${FILESYSTEM_PREFIX}${IMPALA_HOME}/thirdparty/postgresql-jdbc/
${HADOOP_HOME}/bin/hadoop fs -put ${POSTGRES_JDBC_DRIVER} \
    ${FILESYSTEM_PREFIX}${IMPALA_HOME}/thirdparty/postgresql-jdbc/
