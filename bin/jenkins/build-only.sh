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

# Only run an Impala build.

set -euo pipefail
. $IMPALA_HOME/bin/report_build_error.sh
setup_report_build_error

cd "${IMPALA_HOME}"

export IMPALA_MAVEN_OPTIONS="-U"

bin/bootstrap_build.sh

# Sanity check: bootstrap_build.sh should not have modified any of
# the Impala files. This is important for the continued functioning of
# bin/single_node_perf_run.py.
NUM_MODIFIED_FILES=$(git status --porcelain --untracked-files=no | wc -l)
if [[ "${NUM_MODIFIED_FILES}" -ne 0 ]]; then
  echo "ERROR: Impala source files were modified during bin/bootstrap_build.sh"
  echo "Dumping diff:"
  git status --porcelain --untracked-files=no
  git --no-pager diff
  exit 1
fi
