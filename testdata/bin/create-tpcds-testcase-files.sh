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

# This script generates tesecase files for tpcds queries.
# These testcases are then replayed on an empty Catalog to make
# sure that the import works fine.

set -euo pipefail
. $IMPALA_HOME/bin/report_build_error.sh
setup_report_build_error
set -x

. ${IMPALA_HOME}/bin/impala-config.sh > /dev/null 2>&1
IMPALAD=${IMPALAD:-localhost}

TPCDS_QUERY_HOME=$IMPALA_HOME/testdata/workloads/tpcds/queries/raw
# Target directory containing the testcase data.
TESTCASE_DATA_DIR=${FILESYSTEM_PREFIX}/test-warehouse/tpcds-testcase-data

COPY_TEST_CASE_PREFIX="COPY TESTCASE TO '$TESTCASE_DATA_DIR'"

# Clean-up if the directory already exists.
hadoop fs -rm -r -f $TESTCASE_DATA_DIR
hadoop fs -mkdir $TESTCASE_DATA_DIR

for file in $TPCDS_QUERY_HOME/tpcds-query*.sql
do
  echo "Generating testcase for $file"
  ${IMPALA_HOME}/bin/impala-shell.sh -i ${IMPALAD} \
  -d "tpcds" -q "$COPY_TEST_CASE_PREFIX $(< $file)"
done
