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

# Shutdown Impala if it is alive
${IMPALA_HOME}/bin/start-impala-cluster.py --kill

# Kill HBase, then MiniLlama (which includes a MiniDfs, a Yarn RM several NMs).
$IMPALA_HOME/testdata/bin/kill-sentry-service.sh
$IMPALA_HOME/testdata/bin/kill-hive-server.sh
$IMPALA_HOME/testdata/bin/kill-hbase.sh
$IMPALA_HOME/testdata/bin/kill-kudu.sh
$IMPALA_HOME/testdata/bin/kill-mini-dfs.sh
$IMPALA_HOME/testdata/bin/kill-ranger-server.sh

for BINARY in impalad statestored catalogd; do
  if pgrep -U $USER $BINARY; then
    killall -9 -u $USER -q $BINARY
  fi
done
