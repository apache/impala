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

# Do some error checking and generate junit symptoms after running a build.

set -euo pipefail
trap 'echo Error in $0 at line $LINENO: $(cd "'$PWD'" && awk "NR == $LINENO" $0)' ERR

rm -rf "${IMPALA_HOME}"/logs_system
mkdir -p "${IMPALA_HOME}"/logs_system
dmesg > "${IMPALA_HOME}"/logs_system/dmesg

# Check dmesg for OOMs and generate a symptom if present.
if [[ $(grep "Out of memory" "${IMPALA_HOME}"/logs_system/dmesg) ]]; then
  "${IMPALA_HOME}"/bin/generate_junitxml.py --phase finalize --step dmesg \
      --stdout "${IMPALA_HOME}"/logs_system/dmesg --error "Process was OOM killed."
fi
