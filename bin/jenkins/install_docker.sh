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
#
# This script installs and starts Docker. This currently supports
# Ubuntu and Redhat distributions. It does the following:
# 1. Adds the appropriate repository that has Docker
# 2. Installs Docker from that repository
# 3. Starts the Docker service
# 4. Adds the current user to the docker group
#
# After this runs, the user will still need to relogin to detect
# membership in the docker group.

set -euo pipefail

# This can get more detailed if there are specific steps
# for specific versions, but at the moment the distribution
# is all we need.
DISTRIBUTION=Unknown
if [[ -f /etc/redhat-release ]]; then
  echo "Identified Redhat system."
  DISTRIBUTION=Redhat
else
  source /etc/lsb-release
  if [[ $DISTRIB_ID == Ubuntu ]]; then
    echo "Identified Ubuntu system."
    DISTRIBUTION=Ubuntu
  fi
fi

if [[ $DISTRIBUTION == Unknown ]]; then
  echo "ERROR: Did not detect supported distribution."
  echo "Only Ubuntu and Redhat-based distributions are supported."
  exit 1
fi

if [[ $DISTRIBUTION == Ubuntu ]]; then
  # Following install instructions from
  # https://docs.docker.com/engine/install/ubuntu/
  # TODO: These instructions are from an old version of that page, and we should
  # look into updating them.
  export DEBIAN_FRONTEND=noninteractive
  sudo apt-get install -y apt-transport-https ca-certificates curl \
     gnupg-agent software-properties-common
  curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -
  # Bail if the fingerprint isn't what we expected.
  sudo apt-key fingerprint 0EBFCD88 | \
    grep '9DC8 5822 9FC7 DD38 854A  E2D8 8D81 803C 0EBF CD88'
  sudo add-apt-repository \
     "deb [arch=amd64] https://download.docker.com/linux/ubuntu \
     $(source /etc/os-release && echo $UBUNTU_CODENAME) stable"
  sudo apt-get update
  sudo apt-get install -y docker-ce docker-ce-cli containerd.io
  sudo service docker restart
elif [[ $DISTRIBUTION == Redhat ]]; then
  # Following install instructions from
  # https://docs.docker.com/engine/install/centos/
  sudo yum install -y yum-utils
  sudo yum-config-manager \
      --add-repo \
      https://download.docker.com/linux/centos/docker-ce.repo
  # Go get the key and verify it is the expected value.
  curl -fsSL https://download.docker.com/linux/centos/gpg > docker_key.gpg
  gpg --import docker_key.gpg
  # Bail if the fingerprint isn't what we expected
  gpg --fingerprint "Docker Release (CE rpm) <docker@docker.com>" | \
      grep '060A 61C5 1B55 8A7F 742B  77AA C52F EB6B 621E 9F35'
  sudo rpmkeys --import docker_key.gpg
  sudo yum -y install docker-ce docker-ce-cli containerd.io docker-compose-plugin
  sudo systemctl start docker
fi

# Add the current user to the docker group
sudo groupadd -f docker
sudo usermod -aG docker $USER
