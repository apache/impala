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
# Starts a Valkey (Redis-compatible) server in a Docker container for the distributed
# HBO cache tests. Unlike Trino, no custom image is built: the official image is used
# directly and 'docker run' pulls it on first use. --network=host publishes the default
# 6379 port on the host so the Impala minicluster coordinators can reach it at
# localhost:6379.
#
# Optional environment variables (used by the password-auth test to bring up a second,
# isolated instance alongside the default passwordless one):
#   IMPALA_VALKEY_CONTAINER  container name (default impala-minicluster-valkey)
#   IMPALA_VALKEY_PORT       listen port; appended as 'valkey-server --port <port>'
#   IMPALA_VALKEY_PASSWORD   require auth; appended as 'valkey-server --requirepass <pw>'
# Any trailing arguments after the image name are passed straight to valkey-server.

VALKEY_IMAGE="${IMPALA_TEST_VALKEY_IMAGE:-valkey/valkey:8}"
VALKEY_CONTAINER="${IMPALA_VALKEY_CONTAINER:-impala-minicluster-valkey}"

VALKEY_ARGS=()
if [[ -n "${IMPALA_VALKEY_PORT}" ]]; then
  VALKEY_ARGS+=(--port "${IMPALA_VALKEY_PORT}")
fi
if [[ -n "${IMPALA_VALKEY_PASSWORD}" ]]; then
  VALKEY_ARGS+=(--requirepass "${IMPALA_VALKEY_PASSWORD}")
fi

docker run --detach --network=host --name "${VALKEY_CONTAINER}" "${VALKEY_IMAGE}" \
    "${VALKEY_ARGS[@]}"
