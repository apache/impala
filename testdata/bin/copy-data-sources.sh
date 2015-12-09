#!/bin/bash
# Copyright (c) 2014 Cloudera, Inc. All rights reserved.
#
# This script copies the test data source library into hdfs.

set -euo pipefail
trap 'echo Error in $0 at line $LINENO: $(awk "NR == $LINENO" $0)' ERR

. ${IMPALA_HOME}/bin/impala-config.sh > /dev/null 2>&1

hadoop fs -mkdir -p ${FILESYSTEM_PREFIX}/test-warehouse/data-sources/

hadoop fs -put -f \
  ${IMPALA_HOME}/ext-data-source/test/target/impala-data-source-test-*.jar \
  ${FILESYSTEM_PREFIX}/test-warehouse/data-sources/test-data-source.jar
