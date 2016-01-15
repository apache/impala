#!/bin/bash
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
#
# This script copies udf/uda binaries into hdfs.

set -euo pipefail
trap 'echo Error in $0 at line $LINENO: $(cd "'$PWD'" && awk "NR == $LINENO" $0)' ERR

if [ x${JAVA_HOME} == x ]; then
  echo JAVA_HOME not set
  exit 1
fi
. ${IMPALA_HOME}/bin/impala-config.sh > /dev/null 2>&1

BUILD=0

# parse command line options
for ARG in $*
do
  case "$ARG" in
    -build)
      BUILD=1
      ;;
    -help)
      echo "copy-udfs-udas.sh [-build]"
      echo "[-build] : Builds the files to be copied first."
      exit
      ;;
  esac
done

if [ $BUILD -eq 1 ]
then
  pushd $IMPALA_HOME
  make -j$CORES \
      TestUdas TestUdfs test-udfs-ir udfsample udasample udf-sample-ir uda-sample-ir
  cd $IMPALA_HOME/tests/test-hive-udfs
  ${IMPALA_HOME}/bin/mvn-quiet.sh package
  popd
fi

# Copy the test UDF/UDA libraries into HDFS
# We copy:
#   libTestUdas.so
#   libTestUdfs.so  -> to libTestUdfs.so, libTestUdfs.SO, and test_udf/libTestUdfs.so
#   hive-exec.jar
#   impala-hive-udfs.jar
#   test-udfs.ll
#   udf/uda samples (.so/.ll)
hadoop fs -put -f ${IMPALA_HOME}/be/build/latest/testutil/libTestUdas.so\
    ${FILESYSTEM_PREFIX}/test-warehouse
hadoop fs -put -f ${IMPALA_HOME}/be/build/latest/testutil/libTestUdfs.so\
    ${FILESYSTEM_PREFIX}/test-warehouse
hadoop fs -put -f ${IMPALA_HOME}/be/build/latest/testutil/libTestUdfs.so\
    ${FILESYSTEM_PREFIX}/test-warehouse/libTestUdfs.SO
hadoop fs -mkdir -p ${FILESYSTEM_PREFIX}/test-warehouse/udf_test
hadoop fs -put -f ${IMPALA_HOME}/be/build/latest/testutil/libTestUdfs.so\
    ${FILESYSTEM_PREFIX}/test-warehouse/udf_test/libTestUdfs.so
hadoop fs -put -f ${HIVE_HOME}/lib/hive-exec-${IMPALA_HIVE_VERSION}.jar\
  ${FILESYSTEM_PREFIX}/test-warehouse/hive-exec.jar
hadoop fs -put -f ${IMPALA_HOME}/tests/test-hive-udfs/target/test-hive-udfs-1.0.jar\
    ${FILESYSTEM_PREFIX}/test-warehouse/impala-hive-udfs.jar
hadoop fs -put -f ${IMPALA_HOME}/be/build/latest/testutil/test-udfs.ll\
    ${FILESYSTEM_PREFIX}/test-warehouse
hadoop fs -put -f ${IMPALA_HOME}/be/build/latest/udf_samples/libudfsample.so\
    ${FILESYSTEM_PREFIX}/test-warehouse
hadoop fs -put -f ${IMPALA_HOME}/be/build/latest/udf_samples/udf-sample.ll\
    ${FILESYSTEM_PREFIX}/test-warehouse
hadoop fs -put -f ${IMPALA_HOME}/be/build/latest/udf_samples/libudasample.so\
    ${FILESYSTEM_PREFIX}/test-warehouse
hadoop fs -put -f ${IMPALA_HOME}/be/build/latest/udf_samples/uda-sample.ll\
    ${FILESYSTEM_PREFIX}/test-warehouse
echo "Done copying udf/uda libraries."
