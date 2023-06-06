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

RANGER_LOG_DIR="${IMPALA_CLUSTER_LOGS_DIR}/ranger"
if [[ ! -d "${RANGER_LOG_DIR}" ]]; then
    mkdir -p "${RANGER_LOG_DIR}"
fi

# IMPALA-8815: don't allow additional potentially incompatible jars to get onto
# the ranger classpath. We should only need the test cluster configs on the classpath.
unset CLASSPATH
. $IMPALA_HOME/bin/impala-config.sh > /dev/null 2>&1

# Required to start Ranger with Java 11
if [[ ! -d "${RANGER_HOME}"/ews/logs ]]; then
  mkdir -p "${RANGER_HOME}"/ews/logs
fi

JAVA_DBG_SOCKET="-Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=30130"
JAVA_OPTS="-XX:+IgnoreUnrecognizedVMOptions -Xdebug ${JAVA_DBG_SOCKET}" \
    "${RANGER_HOME}"/ews/ranger-admin-services.sh restart
