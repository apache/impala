#!/bin/bash

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


# This script runs Impala's tests with a dockerised minicluster.
# It has been tested on Ubuntu 16.04.
set -x
set -eu -o pipefail

DOCKER_NETWORK="test-impala-cluster"

# Helper to source impala-config.sh, which may have unbound variables
source_impala_config() {
  set +u
  . ./bin/impala-config.sh > /dev/null 2>&1
  set -u
}

source_impala_config

onexit() {
  # Get the logs from all docker containers
  DOCKER_LOGS_DIR="${IMPALA_HOME}/logs/docker_logs"
  mkdir -p "${DOCKER_LOGS_DIR}"
  for container in $(docker ps -a -q); do
    docker logs ${container} > "${DOCKER_LOGS_DIR}/${container}.log" 2>&1 || true
  done

  # Clean up docker containers and networks that may have been created by
  # these tests.
  docker rm -f $(docker ps -a -q) || true
  docker network rm $DOCKER_NETWORK || true
}
trap onexit EXIT

# Check that docker is running and that our user can interact with it.
docker run hello-world

# Set up the test network.
./docker/configure_test_network.sh $DOCKER_NETWORK

# Pick up the new variables.
source_impala_config

# Dump diagnostics for networks and check connectivity.
ifconfig
ping -c 1 $INTERNAL_LISTEN_HOST

# Check that ssh to localhost via Docker gateway works.
if ! ssh -n $INTERNAL_LISTEN_HOST "echo 'SSH success!'"; then
  echo "Failed to ssh, will try to add docker network gateway to known hosts"
  ssh-keyscan $INTERNAL_LISTEN_HOST >> ~/.ssh/known_hosts
  ssh -n $INTERNAL_LISTEN_HOST "echo 'SSH success!'"
fi

# Build Impala and load data with the non-dockerised path.
# TODO: this is a bit awkward. It would be better to have a single invocation
# to run everything with docker.
# Skip building backend tests, which aren't used.
time -p ./buildall.sh -format -testdata -notests < /dev/null
# Kill non-dockerised cluster.
start-impala-cluster.py --kill

# Build the docker images required to start the cluster.
# parquet-reader and impala-profile-tool are needed for e2e tests but not built for
# non-test build.
IMAGE_TYPE=docker_debug
case ${IMPALA_DOCKER_JAVA:-8} in
  11)
    IMAGE_TYPE=${IMAGE_TYPE}_java11
    ;;
  17)
    IMAGE_TYPE=${IMAGE_TYPE}_java17
    ;;
  *)
    ;;
esac
make -j ${IMPALA_BUILD_THREADS} ${IMAGE_TYPE}_images parquet-reader impala-profile-tool

source_impala_config

FAIR_SCHED_CONF=/opt/impala/conf/minicluster-fair-scheduler.xml
LLAMA_CONF=/opt/impala/conf/minicluster-llama-site.xml
export TEST_START_CLUSTER_ARGS="--docker_network=${DOCKER_NETWORK}"
TEST_START_CLUSTER_ARGS+=" --data_cache_dir=/tmp --data_cache_size=500m"
TEST_START_CLUSTER_ARGS+=" --impalad_args=--disk_spill_compression_codec=lz4"
TEST_START_CLUSTER_ARGS+=" --impalad_args=--disk_spill_punch_holes=true"
TEST_START_CLUSTER_ARGS+=" --impalad_args=-fair_scheduler_allocation_path=${FAIR_SCHED_CONF}"
TEST_START_CLUSTER_ARGS+=" --impalad_args=-llama_site_path=${LLAMA_CONF}"
export MAX_PYTEST_FAILURES=0
export NUM_CONCURRENT_TESTS=$(nproc)
# Frontend tests fail because of localhost hardcoded everywhere
export FE_TEST=false
# No need to run backend tests - they are identical with non-docker build.
export BE_TEST=false
# TODO: custom cluster tests may provide some useful coverage but require work
# to make them start up dockerised clusters and will probably make more assumptions
# about the cluster being tested.
export CLUSTER_TEST=false
RET_CODE=0
if ! ./bin/run-all-tests.sh; then
  RET_CODE=1
fi

# Shutdown minicluster at the end
./testdata/bin/kill-all.sh

exit $RET_CODE
