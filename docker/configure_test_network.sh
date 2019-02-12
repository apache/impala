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
#
# Sets up a Docker bridge network with the name provided by the first argument and
# appends the configuration required to use it in a dockerised minicluster to
# bin/impala-config-local.sh. Note that impala-config.sh needs to be re-sourced,
# cluster configurations need to be regenerated, all minicluster processes restarted,
# and data reloaded for the change to be effective and your cluster to be functional.

set -euo pipefail

usage() {
  echo "configure_test_network.sh <docker network name>"
}

if [[ $# != 1 ]]; then
  usage
  exit 1
fi

NETWORK_NAME=$1

# Remove existing network if present.
echo "Removing existing network '$NETWORK_NAME'"
docker network rm "$NETWORK_NAME" || true

echo "Create network '$NETWORK_NAME'"
docker network create -d bridge $NETWORK_NAME
GATEWAY=$(docker network inspect "$NETWORK_NAME" -f '{{(index .IPAM.Config 0).Gateway}}')
echo "Gateway is '${GATEWAY}'"

echo "Updating impala-config-local.sh"
echo "# Configuration to use docker network ${NETWORK_NAME}" \
      >> "$IMPALA_HOME"/bin/impala-config-local.sh
echo "export INTERNAL_LISTEN_HOST=${GATEWAY}" >> "$IMPALA_HOME"/bin/impala-config-local.sh
echo "export DEFAULT_FS=hdfs://\${INTERNAL_LISTEN_HOST}:20500" \
      >> "$IMPALA_HOME"/bin/impala-config-local.sh
echo "export KUDU_MASTER_HOSTS=\${INTERNAL_LISTEN_HOST}" \
      >> "$IMPALA_HOME"/bin/impala-config-local.sh
