#!/bin/bash
################################################################################
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
################################################################################
#
# This script follows the pattern described in the docker best practices here:
# https://docs.docker.com/develop/develop-images/dockerfile_best-practices/#entrypoint
################################################################################

set -euo pipefail

DATA_DIR="/var/lib/hive"
LOG_DIR="$DATA_DIR/logs"
HIVE_HOME="/opt/hive"
export JAVA_HOME=$(which java | xargs readlink -f | sed "s:/bin/java::")
export HADOOP_HOME=/opt/hadoop

function print_help {
  echo "Supported commands:"
  echo "   hms           - start the hive metastore service"
  echo "   help          - print useful information and exit"
  echo ""
  echo "Other commands can be specified to run shell commands."
}

function run_hive_metastore() {
  # If the derby files do not exist, then initialize the schema.
  if [ ! -d "${DATA_DIR}/metastore/metastore_db" ]; then
    $HIVE_HOME/bin/schematool -dbType derby -initSchema
  fi
  # Start the Hive Metastore.
  exec $HIVE_HOME/bin/hive --service metastore
}

# If no arguments are passed, print the help.
if [[ $# -eq 0 ]]; then
  print_help
  exit 1
fi

mkdir -p $DATA_DIR
mkdir -p $LOG_DIR
if [[ "$1" == "hms" ]]; then
  mkdir -p $DATA_DIR
  mkdir -p $LOG_DIR
  run_hive_metastore
  exit 0
elif [[ "$1" == "help" ]]; then
  print_help
  exit 0
fi
# Support calling anything else in the container.
exec "$@"
