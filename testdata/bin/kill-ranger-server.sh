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

# If this directory doesn't exist, then the stop command will immediately error
# trying to write to this directory. The stop won't be issued, and shutdown will
# take 30 seconds longer.
RANGER_LOG_DIR="${IMPALA_CLUSTER_LOGS_DIR}/ranger"
if [[ ! -d "${RANGER_LOG_DIR}" ]]; then
    mkdir -p "${RANGER_LOG_DIR}"
fi

"${RANGER_HOME}"/ews/ranger-admin-services.sh stop
