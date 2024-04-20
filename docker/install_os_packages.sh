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

# Default level of extra debugging tools, controlled by the --install-debug-tools flag.
INSTALL_DEBUG_TOOLS=none

JAVA_VERSION=8
DRY_RUN=false
PKG_LIST=""
NON_PKG_NAMES=(apt-get yum install update)

function print_usage {
    echo "install_os_packages.sh - Helper script to install OS dependencies"
    echo "[--install-debug-tools <none|basic|full>] : set the level of debug tools"\
         "to install"
    echo "[--java <version>] : Use specified Java version rather than the default Java 8."
    echo "[--dry-run] : Print the list of packages to install."
}

# Wraps the passed in command to either execute it (DRY_RUN=false) or just use it
# to update PKG_LIST.
function wrap {
  if $DRY_RUN; then
    for arg in $@; do
      if [[ "${NON_PKG_NAMES[@]}"  =~ "$arg" ]]; then
        continue
      elif [[ "$arg" == "-"* ]]; then
        # Ignores command options
        continue
      elif [[ "$PKG_LIST" != "" ]]; then
        PKG_LIST="$PKG_LIST,$arg"
      else
        PKG_LIST="$arg"
      fi
    done
  else
    "$@"
  fi
}

while [ -n "$*" ]
do
  case "$1" in
    --install-debug-tools)
      INSTALL_DEBUG_TOOLS="${2-}"
      shift;
      ;;
    --java)
      JAVA_VERSION="${2-}"
      shift;
      ;;
    --dry-run)
      DRY_RUN=true
      ;;
    --help|*)
      print_usage
      exit 1
      ;;
  esac
  shift
done

echo "INSTALL_DEBUG_TOOLS=${INSTALL_DEBUG_TOOLS}"
echo "JAVA_VERSION=${JAVA_VERSION}"
echo "DRY_RUN=${DRY_RUN}"

case "$INSTALL_DEBUG_TOOLS" in
  none | basic | full)
    # These are valid.
    ;;
  "" | *)
    # The argument to --install-debug-tools is either missing, or is not a recognized
    # value.
    print_usage
    exit 1
    ;;
esac

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
    if [[ $DISTRIB_RELEASE == 16.04 ]] && [[ $JAVA_VERSION == 11 ]] ; then
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
  wrap apt-get update
  wrap apt-get install -y \
      hostname \
      krb5-user \
      language-pack-en \
      libsasl2-2 \
      libsasl2-modules \
      libsasl2-modules-gssapi-mit \
      openjdk-${JAVA_VERSION}-jre-headless \
      tzdata

  # On Ubuntu there are no extra tools installed for $INSTALL_DEBUG_TOOLS == basic

  if [[ $INSTALL_DEBUG_TOOLS == full ]]; then
    echo "Installing full debug tools"
    wrap apt-get install -y \
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
  if [[ $JAVA_VERSION == 8 ]]; then
    JAVA_VERSION=1.8.0
  fi
  wrap yum install -y --disableplugin=subscription-manager \
      cyrus-sasl-gssapi \
      cyrus-sasl-plain \
      hostname \
      java-${JAVA_VERSION}-openjdk-headless \
      krb5-workstation \
      openldap-devel \
      procps-ng \
      tzdata

  # UTF-8 masking functions require the presence of en_US.utf8.
  # Install the appropriate language packs. Redhat/Centos 7 come
  # with en_US.utf8, so there is no need to install anything.
  if ! grep 'release 7\.' /etc/redhat-release; then
      wrap yum install -y --disableplugin=subscription-manager \
          glibc-langpack-en \
          langpacks-en
  fi

  if [[ $INSTALL_DEBUG_TOOLS == basic || $INSTALL_DEBUG_TOOLS == full ]]; then
    echo "Installing basic debug tools"
    wrap yum install -y --disableplugin=subscription-manager \
        java-${JAVA_VERSION}-openjdk-devel \
        gdb
  fi

  if [[ $INSTALL_DEBUG_TOOLS == full ]]; then
    echo "Installing full debug tools"
    wrap yum install -y --disableplugin=subscription-manager \
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

if $DRY_RUN; then
  echo "The following packages would be installed:"
  echo "$PKG_LIST"
  exit 0
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

# graceful_shutdown_backends.sh requires the pgrep utility. Verify it is present.
if ! command -v pgrep ; then
  echo "ERROR: 'pgrep' is not present."
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
