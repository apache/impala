#!/bin/bash

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
${HADOOP_HOME}/bin/hadoop fs -put ${IMPALA_HOME}/thirdparty/postgresql-jdbc/*.jar \
    ${FILESYSTEM_PREFIX}${IMPALA_HOME}/thirdparty/postgresql-jdbc/
