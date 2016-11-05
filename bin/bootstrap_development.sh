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

# This script bootstraps a development environment from almost nothing; it is known to
# work on Ubuntu 14.04, and it definitely clobbers some local environment, so it's best to
# run this in a sandbox first, like a VM or docker.
#
# The intended user is a person who wants to start contributing code to Impala. This
# script serves as an executable reference point for how to get started.
#
# At this time, it completes in about 6.5 hours. It generates and loads the test data and
# metadata without using a snapshot (which takes about 3 hours) and it then runs the full
# testsuite (frontend, backend, end-to-end, JDBC, and custom cluster) in "core"
# exploration mode.

set -eux -o pipefail

HOMEDIR="/home/$(whoami)/"

if [[ ! -d "${HOMEDIR}" ]]
then
    echo "${HOMEDIR} is needed for installing Impala dependencies"
    exit 1
fi

if [[ -z "${JAVA_HOME}" ]]
then
    echo "JAVA_HOME must be set to install Impala dependencies"
    exit 1
fi

if ! sudo true
then
    echo "Passwordless sudo is needed for this script"
    exit 1
fi

IMPALA_SETUP_REPO_URL="https://github.com/awleblang/impala-setup"

# Place to download setup scripts
TMPDIR=$(mktemp -d)
function cleanup {
    rm -rf "${TMPDIR}"
}
trap cleanup EXIT

# Install build and test pre-reqs
pushd "${TMPDIR}"
git clone "${IMPALA_SETUP_REPO_URL}" impala-setup
cd impala-setup
chmod +x ./install.sh
sudo ./install.sh
popd

# HDFS bug workaround
echo "127.0.0.1 $(hostname -s) $(hostname)" | sudo tee -a /etc/hosts
echo "NoHostAuthenticationForLocalhost yes" >> ~/.ssh/config

pushd "$(dirname $0)/.."
export IMPALA_HOME="$(pwd)"
export MAX_PYTEST_FAILURES=0
source bin/impala-config.sh
./buildall.sh -noclean -format -testdata -build_shared_libs
popd
