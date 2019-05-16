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
  . ./bin/impala-config.sh
  set -u
}

onexit() {
  # Clean up docker containers and networks that may have been created by
  # these tests.
  docker rm -f $(docker ps -a -q) || true
  docker network rm $DOCKER_NETWORK || true
}
trap onexit EXIT

source_impala_config

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
# parquet-reader is needed for e2e tests but not built for non-test build.
make -j ${IMPALA_BUILD_THREADS} docker_images parquet-reader

source_impala_config

export TEST_START_CLUSTER_ARGS="--docker_network=${DOCKER_NETWORK}"
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
./bin/run-all-tests.sh
