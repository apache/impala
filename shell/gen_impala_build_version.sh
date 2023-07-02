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

set -euo pipefail
. $IMPALA_HOME/bin/report_build_error.sh
setup_report_build_error

if [ "x${IMPALA_HOME}" == "x" ]; then
  echo "\$IMPALA_HOME must be set"
  exit 1
fi

IMPALA_VERSION_INFO_FILE=${IMPALA_HOME}/bin/version.info

if [ ! -f ${IMPALA_VERSION_INFO_FILE} ]; then
  echo "No version.info file found. Generating new version info"
  ${IMPALA_HOME}/bin/save-version.sh
else
  echo "Using existing version.info file."
fi

VERSION=$(grep "VERSION: " ${IMPALA_VERSION_INFO_FILE} | awk '{print $2}')
GIT_HASH=$(grep "GIT_HASH: " ${IMPALA_VERSION_INFO_FILE} | awk '{print $2}')
BUILD_DATE=$(grep "BUILD_TIME: " ${IMPALA_VERSION_INFO_FILE} | cut -f 2- -d ' ')
cat ${IMPALA_VERSION_INFO_FILE}

SHELL_HOME=${IMPALA_HOME}/shell
THRIFT_GEN_PY_DIR="${SHELL_HOME}/gen-py"

rm -f ${THRIFT_GEN_PY_DIR}/impala_build_version.py
cat > ${THRIFT_GEN_PY_DIR}/impala_build_version.py <<EOF
# -*- coding: utf-8 -*-
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

def get_version():
  return "${VERSION}"

def get_git_hash():
  return "${GIT_HASH}"

def get_build_date():
  return "${BUILD_DATE}"
EOF
