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

if [[ $# -eq 1 && "$1" == -format ]]; then
  SHOULD_FORMAT=true
elif [[ $# -ne 0 ]]; then
  echo "Usage: $0 [-format]"
  echo "[-format] : Format the kudu cluster before starting"
  exit 1
else
  SHOULD_FORMAT=false
fi

# Kill and clean data for a clean start.
$IMPALA_HOME/testdata/bin/kill-kudu.sh

if $SHOULD_FORMAT; then
  $IMPALA_HOME/testdata/cluster/admin delete_kudu_data
fi

set +e
$IMPALA_HOME/testdata/cluster/admin start_kudu_cluster
