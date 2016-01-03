#!/bin/bash
# Copyright 2012 Cloudera Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Check if a reference githash exists in hdfs; If it does, diff the directories that
# contain the schemas against the current HEAD.
# This script exits with 0 and 1 as the returncodes
#  - 0 implies that the schema diff is emppty, or that a reference githash was not found.
#  - 1 implies that the schemas have changed.

set -euo pipefail
trap 'echo Error in $0 at line $LINENO: $(awk "NR == $LINENO" $0)' ERR

. ${IMPALA_HOME}/bin/impala-config.sh > /dev/null 2>&1

DATASET=${1-}
hdfs dfs -test -e  ${WAREHOUSE_LOCATION_PREFIX}/test-warehouse/githash.txt || { exit 0; }
GIT_HASH=$(echo $(hdfs dfs -cat ${WAREHOUSE_LOCATION_PREFIX}/test-warehouse/githash.txt))
if ! git show $GIT_HASH &>/dev/null; then
  echo The git commit used to create the test warehouse snapshot is not available \
      locally. Fetching the latest commits from remotes.
  git fetch --all &>/dev/null
fi
# Check whether a non-empty diff exists.
git diff --exit-code ${GIT_HASH}..HEAD ${IMPALA_HOME}/testdata/datasets/$DATASET
git diff --exit-code ${GIT_HASH}..HEAD ${IMPALA_HOME}/testdata/avro_schema_resolution
