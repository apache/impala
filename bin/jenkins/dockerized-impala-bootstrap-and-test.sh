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

# Following install instructions from
# https://docs.docker.com/install/linux/docker-ce/ubuntu/#install-docker-ce-1
sudo apt-get install -y apt-transport-https ca-certificates curl \
     gnupg-agent software-properties-common
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -
# Bail if the fingerprint isn't what we expected.
sudo apt-key fingerprint 0EBFCD88 | \
  grep '9DC8 5822 9FC7 DD38 854A  E2D8 8D81 803C 0EBF CD88'
sudo add-apt-repository \
   "deb [arch=amd64] https://download.docker.com/linux/ubuntu \
   $(lsb_release -cs) stable"
sudo apt-get update
sudo apt-get install -y docker-ce docker-ce-cli containerd.io
sudo service docker restart
sudo groupadd -f docker
sudo usermod -aG docker $USER

# Execute the tests using su to re-login so that group change made above
# setup_docker takes effect.
sudo su $USER -c "./bin/jenkins/dockerized-impala-run-tests.sh"
