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
# This installs the minimal dependencies needed for Impala
# services to run. It currently handles Ubuntu and Redhat
# distributions. This takes an optional argument "--install-debug-tools"
# which installs extra debugging tools like curl, ping, etc.

set -euo pipefail

INSTALL_DEBUG_TOOLS=false

function print_usage {
    echo "install_os_packages.sh - Helper script to install OS dependencies"
    echo "[--install-debug-tools] : Also install debug tools like curl, iproute, etc"
}

while [ -n "$*" ]
do
  case "$1" in
    --install-debug-tools)
      INSTALL_DEBUG_TOOLS=true
      ;;
    --help|*)
      print_usage
      exit 1
      ;;
  esac
  shift
done

echo "INSTALL_DEBUG_TOOLS=${INSTALL_DEBUG_TOOLS}"

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

# Install minimal set of files.
# Optionally install extra debug tools.
if [[ $DISTRIBUTION == Ubuntu ]]; then
  export DEBIAN_FRONTEND=noninteractive
  apt-get update
  apt-get install -y \
      krb5-user \
      libsasl2-2 \
      libsasl2-modules \
      libsasl2-modules-gssapi-mit \
      openjdk-8-jre-headless \
      tzdata
  if $INSTALL_DEBUG_TOOLS ; then
    echo "Installing extra debug tools"
    apt-get install -y \
        curl \
        dnsutils \
        iproute2 \
        iputils-ping \
        less \
        netcat-openbsd \
        sudo \
        vim
  fi
elif [[ $DISTRIBUTION == Redhat ]]; then
  yum install -y --disableplugin=subscription-manager \
      cyrus-sasl-gssapi \
      cyrus-sasl-plain \
      java-1.8.0-openjdk-headless \
      krb5-workstation \
      openldap-devel \
      tzdata
  if $INSTALL_DEBUG_TOOLS ; then
    echo "Installing extra debug tools"
    yum install -y --disableplugin=subscription-manager \
        bind-utils \
        curl \
        iproute \
        iputils \
        less \
        nmap-ncat \
        sudo \
        vim \
        which
  fi
fi

# To minimize the size for the Docker image, clean up any unnecessary files.
if [[ $DISTRIBUTION == Ubuntu ]]; then
  apt-get clean
  rm -rf /var/lib/apt/lists/*
elif [[ $DISTRIBUTION == Redhat ]]; then
  yum clean all
  rm -rf /var/cache/yum/*
fi
