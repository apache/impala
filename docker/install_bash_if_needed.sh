#!/bin/sh
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
# This installs bash if the base Docker image does not have it.
# Bash is needed for the other helper shell scripts for Impala images,
# so the first step is to make it available.

set -eu

# Default level of extra debugging tools, controlled by the --install-debug-tools flag.
INSTALL_DEBUG_TOOLS=none

DRY_RUN=false

if command -v bash; then
  echo "Bash found, skipping installation."
  exit 0
fi

# This can get more detailed if there are specific steps
# for specific versions, but at the moment the distribution
# is all we need.
DISTRIBUTION=Unknown
if [ -f /etc/redhat-release ]; then
  echo "Identified Redhat system."
  DISTRIBUTION=Redhat
elif [ -f /etc/lsb-release ]; then
  . /etc/lsb-release
  if [ $DISTRIB_ID = Ubuntu ]; then
    echo "Identified Ubuntu system."
    DISTRIBUTION=Ubuntu
  fi
# Check /etc/os-release last: it exists on Red Hat and Ubuntu systems as well
elif [ -f /etc/os-release ]; then
  source /etc/os-release
  if [ $ID = wolfi -o $ID = chainguard ]; then
    echo "Identified Wolfi-based system."
    DISTRIBUTION=Chainguard
  fi
fi

if [ $DISTRIBUTION = Unknown ]; then
  echo "ERROR: Did not detect supported distribution."
  echo "Only Ubuntu, Red Hat (or related), or Wolfi base images are supported."
  exit 1
fi

# Install minimal set of files.
# Optionally install extra debug tools.
if [ $DISTRIBUTION = Ubuntu ]; then
  export DEBIAN_FRONTEND=noninteractive
  apt-get update
  apt-get install -y bash
elif [ $DISTRIBUTION = Redhat ]; then
  yum install -y bash
elif [ $DISTRIBUTION = Chainguard ]; then
  apk add --no-cache --no-interactive bash
fi

