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

# This script automates setup of distcc servers on Ubuntu. This
# script can be run outside of an Impala repository and will
# bootstrap the server from scratch.
#
# Usage:
# ------
# This script must be run as root. Arguments are forwarded to
# distcc_server_setup.sh to specify IP ranges, etc.

set -eu -o pipefail

if [[ "$USER" != "root" ]]; then
  echo "Must be run as root."
  exit 1
fi

OS_ID=$(source /etc/os-release && echo $ID)
OS_VERSION=$(source /etc/os-release && echo $VERSION_ID)
if [[ "$OS_ID" == Ubuntu ]]; then
  if ! [[ $OS_VERSION == 16.04 || $OS_VERSION == 18.04 || $OS_VERSION == 20.04 ]]; then
    echo "This script only supports Ubuntu 16.04, 18.04, and 20.04" >&2
    exit 1
  fi
fi

# Install basic packages required to get to distcc_server_setup.sh
# git: needed to clone the repo
# openjdk: needed because JAVA_HOME is checked by impala-config.sh
# distcc: needed to set up the distccd user.
# libsasl2-dev: needed to bootstrap python virtualenv
apt-get install -y git openjdk-8-jdk-headless distcc libsasl2-dev

mkdir -p /opt/distcc
chown distccd /opt/distcc
cd /opt/distcc

# Toolchain must be owned by distcc so that it can execute binaries.
# So check out the Impala repo and download the toolchain as the
# distccd user.
sudo -u distccd -H bash <<"EOF"
  set -euo pipefail
  set -x
  # Set HOME as workaround for ccache trying to access /.ccache
  HOME=$(pwd)
  if [[ ! -d impala ]]; then
    git clone https://github.com/apache/impala.git
  fi
  cd impala
  echo 'export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64' >> bin/impala-config-local.sh
  export IMPALA_HOME=$(pwd)
 . bin/impala-config.sh
 ./infra/python/deps/download_requirements
 DOWNLOAD_CDH_COMPONENTS=false ./bin/bootstrap_toolchain.py
EOF

# To resolve CVE-2004-2687, newer distcc versions only allow programs to be executed
# if they have a symlink under '/usr/lib/distcc'.
# https://github.com/distcc/distcc/commit/dfb45b528746bf89c030fccac307ebcf7c988512
sudo ln -s $(which ccache) /usr/lib/distcc/ccache

(cd impala && ./bin/distcc/distcc_server_setup.sh "$@")
