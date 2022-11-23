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
USE_JAVA11=false

function print_usage {
    echo "install_os_packages.sh - Helper script to install OS dependencies"
    echo "[--install-debug-tools] : Also install debug tools like curl, iproute, etc"
    echo "[--use-java11] : Use Java 11 rather than the default Java 8."
}

while [ -n "$*" ]
do
  case "$1" in
    --install-debug-tools)
      INSTALL_DEBUG_TOOLS=true
      ;;
    --use-java11)
      USE_JAVA11=true
      ;;
    --help|*)
      print_usage
      exit 1
      ;;
  esac
  shift
done

echo "INSTALL_DEBUG_TOOLS=${INSTALL_DEBUG_TOOLS}"
echo "USE_JAVA11=${USE_JAVA11}"

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

    # Ubuntu 16.04 does not have Java 11 in its package repository,
    # so exit with an error.
    if [[ $DISTRIB_RELEASE == 16.04 ]] && $USE_JAVA11 ; then
      echo "ERROR: Java 11 is not supported on Ubuntu 16.04"
      exit 1
    fi
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
  if $USE_JAVA11 ; then
    apt-get install -y openjdk-11-jre-headless
  else
    apt-get install -y openjdk-8-jre-headless
  fi
  apt-get install -y \
      hostname \
      krb5-user \
      language-pack-en \
      libsasl2-2 \
      libsasl2-modules \
      libsasl2-modules-gssapi-mit \
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
  if $USE_JAVA11 ; then
    yum install -y --disableplugin=subscription-manager \
        java-11-openjdk-headless
  else
    yum install -y --disableplugin=subscription-manager \
        java-1.8.0-openjdk-headless
  fi
  yum install -y --disableplugin=subscription-manager \
      cyrus-sasl-gssapi \
      cyrus-sasl-plain \
      hostname \
      krb5-workstation \
      openldap-devel \
      tzdata

  # UTF-8 masking functions require the presence of en_US.utf8.
  # Install the appropriate language packs. Redhat/Centos 7 come
  # with en_US.utf8, so there is no need to install anything.
  if ! grep 'release 7\.' /etc/redhat-release; then
      yum install -y --disableplugin=subscription-manager \
          glibc-langpack-en \
          langpacks-en
  fi

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

# Verify en_US.utf8 is present
if ! locale -a | grep en_US.utf8 ; then
  echo "ERROR: en_US.utf8 locale is not present."
  exit 1
fi

if ! hostname ; then
  echo "ERROR: 'hostname' command failed."
  exit 1
fi

# Impala will fail to start if the permissions on /var/tmp are not set to include
# the sticky bit (i.e. +t). Some versions of Redhat UBI images do not have
# this set by default, so specifically set the sticky bit for both /tmp and /var/tmp.
chmod a=rwx,o+t /var/tmp /tmp

# To minimize the size for the Docker image, clean up any unnecessary files.
if [[ $DISTRIBUTION == Ubuntu ]]; then
  apt-get clean
  rm -rf /var/lib/apt/lists/*
elif [[ $DISTRIBUTION == Redhat ]]; then
  yum clean all
  rm -rf /var/cache/yum/*
fi
