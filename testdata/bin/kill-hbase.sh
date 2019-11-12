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

DIR=$(dirname "$0")
echo Stopping Hbase
# Use the stop-hbase.sh script provided by HBase. This does a more graceful shutdown
# that using our kill-java-service.sh script.
${HBASE_HOME}/bin/stop-hbase.sh

# Clear up data so that zookeeper/hbase won't do recovery when it starts.
# TODO: is this still needed when using bin/stop-hbase.sh?
rm -rf /tmp/hbase-*
