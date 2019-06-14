#!/usr/bin/env bash
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

# Utility script that invokes maven but filters out noisy info logging
set -euo pipefail

LOG_FILE="${IMPALA_MVN_LOGS_DIR}/mvn.log"

mkdir -p "$IMPALA_MVN_LOGS_DIR"

cat << EOF | tee -a "$LOG_FILE"
========================================================================
Running mvn $IMPALA_MAVEN_OPTIONS $@
Directory $(pwd)
========================================================================
EOF

if ! mvn $IMPALA_MAVEN_OPTIONS "$@" | \
  tee -a "$LOG_FILE" | grep -E -e WARNING -e ERROR -e SUCCESS -e FAILURE -e Test; then
  echo "mvn $IMPALA_MAVEN_OPTIONS $@ exited with code $?"
  exit 1
fi
