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
# Check if a reference githash exists in hdfs; If it does, diff the directories that
# contain the schemas against the current HEAD.
# This script exits with 0 and 1 as the returncodes
#  - 0 implies that the schema diff is emppty, or that a reference githash was not found.
#  - 1 implies that the schemas have changed.

set -euo pipefail

# We don't want to generate a junit xml report for errors generated here,
# since exit code 1 here denotes something useful. So in the case of this
# script, we don't call setup_report_build_error.
trap 'echo Error in $0 at line $LINENO: $(cd "'$PWD'" && awk "NR == $LINENO" $0)' ERR

. ${IMPALA_HOME}/bin/impala-config.sh > /dev/null 2>&1

DATASET=${1-}
hdfs dfs -test -e  ${WAREHOUSE_LOCATION_PREFIX}/test-warehouse/githash.txt || { exit 0; }
GIT_HASH=$(echo $(hdfs dfs -cat ${WAREHOUSE_LOCATION_PREFIX}/test-warehouse/githash.txt))

# To ensure this script works when executed from anywhere, even outside the Impala repo,
# we specifiy the git-dir and work-tree for all git commands.
if ! git --git-dir ${IMPALA_HOME}/.git --work-tree=${IMPALA_HOME} \
  show $GIT_HASH &>/dev/null; then
  echo The git commit used to create the test warehouse snapshot is not available \
      locally. Fetching the latest commits from remotes.
  git --git-dir ${IMPALA_HOME}/.git --work-tree=${IMPALA_HOME} fetch --all &>/dev/null
fi
# Check whether a non-empty diff exists.
git --git-dir ${IMPALA_HOME}/.git --work-tree=${IMPALA_HOME} \
  diff --exit-code ${GIT_HASH}..HEAD ${IMPALA_HOME}/testdata/datasets/$DATASET
git --git-dir ${IMPALA_HOME}/.git --work-tree=${IMPALA_HOME} \
  diff --exit-code ${GIT_HASH}..HEAD ${IMPALA_HOME}/testdata/avro_schema_resolution
