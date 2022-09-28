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
KILL_HIVESERVER=1
KILL_METASTORE=1

while [ -n "$*" ]
do
  case $1 in
    -only_hiveserver)
      KILL_METASTORE=0
      ;;
    -only_metastore)
      KILL_HIVESERVER=0
      ;;
    -help|-h|*)
      echo "kill-hive-server.sh : Kills the hive server and the metastore."
      echo "[-only_metastore] : Only kills the hive metastore."
      echo "[-only_hiveserver] : Only kills the hive server."
      exit 1;
      ;;
    esac
  shift;
done

if [[ $KILL_HIVESERVER -eq 1 ]]; then
  echo Stopping Hive server.
  "$DIR"/kill-java-service.sh -c HiveServer
  # The kill-java-service.sh command would fail if it did not succeed in
  # stopping HiveServer2. Remove the pid file so that a reuse of the pid cannot
  # interfere with starting HiveServer2. By default, the pid is written to
  # $HIVE_CONF_DIR.
  rm -f "$HIVE_CONF_DIR"/hiveserver2.pid
fi
if [[ $KILL_METASTORE -eq 1 ]]; then
  echo Stopping Hive metastore.
  "$DIR"/kill-java-service.sh -c HiveMetaStore
fi
