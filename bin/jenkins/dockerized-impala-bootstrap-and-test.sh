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

# This script sets up docker on Ubuntu 16.04 and then run's
# Impala's tests with a dockerised minicluster.

set -eu -o pipefail

ROOT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/../.." && pwd )"
cd "$ROOT_DIR"

source ./bin/bootstrap_system.sh

# Install docker
./bin/jenkins/install_docker.sh

# Preserve some important environment variables so that they are available to
# dockerized-impala-run-tests.sh.
# NOTE: A Jenkins job can also call dockerized-impala-preserve-vars.py directly
# to preserve additional variables.
./bin/jenkins/dockerized-impala-preserve-vars.py \
    EE_TEST EE_TEST_FILES JDBC_TEST EXPLORATION_STRATEGY CMAKE_BUILD_TYPE \
    IMPALA_DOCKER_JAVA

# Execute the tests using su to re-login so that group change made above
# setup_docker takes effect. This does a full re-login and does not stay
# in the current directory, so change back to $IMPALA_HOME (resolved in
# the current environment) before executing the script.
sudo su - $USER -c "cd ${IMPALA_HOME} && ./bin/jenkins/dockerized-impala-run-tests.sh"
