#!/bin/bash
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
#
# This script copies udf/uda binaries into hdfs.

if [ x${JAVA_HOME} == x ]; then
  echo JAVA_HOME not set
  exit 1
fi
. ${IMPALA_HOME}/bin/impala-config.sh
set -e

# Copy the test UDF/UDA libraries into HDFS
# We copy:
#   libTestUdas.so
#   libTestUdfs.so  -> to libTestUdfs.so and libTestUdfs.SO
#   hive-builtins.jar
#   impala-hive-udfs.jar
#   test-udfs.ll
#   udf/uda samples (.so/.ll)
hadoop fs -put -f ${IMPALA_HOME}/be/build/debug/testutil/libTestUdas.so /test-warehouse
hadoop fs -put -f ${IMPALA_HOME}/be/build/debug/testutil/libTestUdfs.so /test-warehouse
hadoop fs -put -f ${IMPALA_HOME}/be/build/debug/testutil/libTestUdfs.so\
    /test-warehouse/libTestUdfs.SO
hadoop fs -put -f ${HIVE_HOME}/lib/hive-builtins-${IMPALA_HIVE_VERSION}.jar\
    /test-warehouse/hive-builtins.jar
hadoop fs -put -f ${IMPALA_HOME}/fe/target/impala-frontend-0.1-SNAPSHOT-tests.jar\
    /test-warehouse/impala-hive-udfs.jar
hadoop fs -put -f ${IMPALA_HOME}/be/build/debug/testutil/test-udfs.ll /test-warehouse
hadoop fs -put -f ${IMPALA_HOME}/be/build/debug/udf_samples/libudfsample.so\
    /test-warehouse
hadoop fs -put -f ${IMPALA_HOME}/be/build/debug/udf_samples/udf-sample.ll\
    /test-warehouse
hadoop fs -put -f ${IMPALA_HOME}/be/build/debug/udf_samples/libudasample.so\
    /test-warehouse
hadoop fs -put -f ${IMPALA_HOME}/be/build/debug/udf_samples/uda-sample.ll\
    /test-warehouse
echo "Done copying udf/uda libraries."
