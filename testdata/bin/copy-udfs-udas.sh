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
# This script copies udf/uda binaries into hdfs.

set -euo pipefail
. $IMPALA_HOME/bin/report_build_error.sh
setup_report_build_error

if [ x${JAVA_HOME} == x ]; then
  echo JAVA_HOME not set
  exit 1
fi
. "${IMPALA_HOME}/bin/impala-config.sh" > /dev/null 2>&1

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
  pushd "${IMPALA_HOME}"
  "${MAKE_CMD:-make}" ${IMPALA_MAKE_FLAGS} "-j${IMPALA_BUILD_THREADS:-4}" \
      TestUdas TestUdfs test-udfs-ir udfsample udasample udf-sample-ir uda-sample-ir
  cd "${IMPALA_HOME}/java/test-corrupt-hive-udfs"
  "${IMPALA_HOME}/bin/mvn-quiet.sh" package
  cp target/test-corrupt-hive-udfs-1.0.jar \
      "${IMPALA_HOME}/testdata/udfs/impala-corrupt-hive-udfs.jar"
  cd "${IMPALA_HOME}/java/test-hive-udfs"
  "${IMPALA_HOME}/bin/mvn-quiet.sh" package
  cp target/test-hive-udfs-1.0.jar "${IMPALA_HOME}/testdata/udfs/impala-hive-udfs.jar"
  # Change one of the Java files to make a new jar for testing purposes, then change it
  # back
  find . -type f -name 'TestUpdateUdf.java' -execdir \
       bash -c "sed -i s/'Text(\"Old UDF\")'/'Text(\"New UDF\")'/g '{}'" \;
  # Create a new Java function by copying and renaming an existing Java file for testing
  # purposes. Then remove it.
  find . -type f -name 'ReplaceStringUdf.java' -execdir \
       bash -c "sed s/'ReplaceStringUdf'/'NewReplaceStringUdf'/g '{}'" \; \
       > src/main/java/org/apache/impala/NewReplaceStringUdf.java
  "${IMPALA_HOME}/bin/mvn-quiet.sh" package
  cp target/test-hive-udfs-1.0.jar \
      "${IMPALA_HOME}/testdata/udfs/impala-hive-udfs-modified.jar"
  find . -type f -name 'TestUpdateUdf.java' -execdir \
       bash -c "sed -i s/'Text(\"New UDF\")'/'Text(\"Old UDF\")'/g '{}'" \;
  rm src/main/java/org/apache/impala/NewReplaceStringUdf.java
  # Rebuild with the original version
  "${IMPALA_HOME}/bin/mvn-quiet.sh" package
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

# Using a single HDFS command only works if the files already have the same names
# and directory structure that we want in HDFS. Create directories and symbolic links
# to make that possible.
UDF_TMP_DIR=$(mktemp -d)

ln -s "${IMPALA_HOME}/be/build/latest/testutil/libTestUdas.so" "${UDF_TMP_DIR}"
ln -s "${IMPALA_HOME}/be/build/latest/testutil/libTestUdfs.so" "${UDF_TMP_DIR}"
ln -s "${IMPALA_HOME}/be/build/latest/testutil/libTestUdfs.so" \
  "${UDF_TMP_DIR}/libTestUdfs.SO"
mkdir "${UDF_TMP_DIR}/udf_test"
ln -s "${IMPALA_HOME}/be/build/latest/testutil/libTestUdfs.so" "${UDF_TMP_DIR}/udf_test"
ln -s "${HIVE_HOME}/lib/hive-exec-"*.jar "${UDF_TMP_DIR}/hive-exec.jar"
ln -s "${IMPALA_HOME}/testdata/udfs/impala-hive-udfs.jar" \
  "${UDF_TMP_DIR}/impala-hive-udfs.jar"
ln -s "${IMPALA_HOME}/testdata/udfs/impala-corrupt-hive-udfs.jar" \
  "${UDF_TMP_DIR}/impala-corrupt-hive-udfs.jar"
ln -s "${IMPALA_HOME}/be/build/latest/testutil/test-udfs.ll" "${UDF_TMP_DIR}"
ln -s "${IMPALA_HOME}/be/build/latest/udf_samples/libudfsample.so" "${UDF_TMP_DIR}"
ln -s "${IMPALA_HOME}/be/build/latest/udf_samples/udf-sample.ll" "${UDF_TMP_DIR}"
ln -s "${IMPALA_HOME}/be/build/latest/udf_samples/libudasample.so" "${UDF_TMP_DIR}"
ln -s "${IMPALA_HOME}/be/build/latest/udf_samples/uda-sample.ll" "${UDF_TMP_DIR}"

hadoop fs -put -f "${UDF_TMP_DIR}"/* "${FILESYSTEM_PREFIX}/test-warehouse"

# Remove temporary directory
rm -r ${UDF_TMP_DIR}
echo "Done copying udf/uda libraries."
