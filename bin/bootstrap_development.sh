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
# work on Ubuntu 16.04. It clobbers some local environment and system
# configurations, so it is best to run this in a fresh install. It also sets up the
# ~/.bashrc for the calling user and impala-config-local.sh with some environment
# variables to make Impala compile and run after this script is complete.
#
# The intended user is a person who wants to start contributing code to Impala. This
# script serves as an executable reference point for how to get started. It takes about
# two hours to complete.
#
# To run this in a Docker container:
#
#   1. Run with --privileged
#   2. Give the container a non-root sudoer wih NOPASSWD:
#      apt-get update
#      apt-get install sudo
#      adduser --disabled-password --gecos '' impdev
#      echo 'impdev ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers
#   3. Run this script as that user: su - impdev -c /bootstrap_development.sh

set -eu -o pipefail

BINDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Check available disk space before starting.
available_disk=$(command df --output=avail --block-size=1G . | sed 1d)
if [[ $available_disk -lt 80 ]]; then
  echo "Insufficient disk space ($available_disk GB), \
Impala requires at least 80GB to build and load test data"
  exit 1
fi

source "${BINDIR}/bootstrap_system.sh"

export MAX_PYTEST_FAILURES=0
source bin/impala-config.sh > /dev/null 2>&1

BOUNDED_CONCURRENCY=$((AVAILABLE_MEM / 4))
if [[ $AVAILABLE_MEM -lt 4 ]]; then
  echo "Insufficient memory ($AVAILABLE_MEM GB) to link Impala test binaries"
  echo "Increase memory, or run buildall.sh -format -testdata -notests"
  exit 1
elif [[ $BOUNDED_CONCURRENCY -lt $IMPALA_BUILD_THREADS ]]; then
  echo "Bounding concurrency to $BOUNDED_CONCURRENCY for link phase"
  IMPALA_BUILD_THREADS=$BOUNDED_CONCURRENCY
fi

time -p ./buildall.sh -format -testdata -skiptests

# To then run the tests:
# export NUM_CONCURRENT_TESTS=$(nproc)
# time -p bin/run-all-tests.sh
