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

# This script builds Impala from scratch. It is known to work on Ubuntu versions 20.04,
# 22.04 and 24.04. To run it you need to have:
#
# 1. At least 8GB of free disk space
# 4. A connection to the internet (parts of the build download dependencies)
#
# To bootstrap a more complete development environment that includes not only building
# Impala but also running and testing it, see bootstrap_development.sh in this directory.

# Set up some logging and exit conditions:
set -euxo pipefail

# Install non-java dependencies:
# Kerberos setup would pop up dialog boxes without this
export DEBIAN_FRONTEND=noninteractive
sudo -E apt-get --quiet update
# Always install Python 3 and set it to be the default Python
PACKAGES='g++ gcc git libsasl2-dev libssl-dev make ninja-build
     python3-dev python3-setuptools python3-venv libffi-dev language-pack-en
     libkrb5-dev krb5-admin-server krb5-kdc krb5-user libxml2-dev libxslt-dev wget'

sudo -E apt-get --yes --quiet install ${PACKAGES}

JDK_VERSION=17
if [[ "$(uname -p)" == 'aarch64' ]]; then
  PACKAGE_ARCH='arm64'
else
  PACKAGE_ARCH='amd64'
fi
sudo apt-get --yes --quiet install openjdk-${JDK_VERSION}-jdk openjdk-${JDK_VERSION}-source
export JAVA_HOME=/usr/lib/jvm/java-${JDK_VERSION}-openjdk-${PACKAGE_ARCH}

# Download Maven since the packaged version is pretty old.
: ${IMPALA_TOOLCHAIN_HOST:=native-toolchain.s3.amazonaws.com}
MVN_VERSION="3.9.15"
if [ ! -d "/usr/local/apache-maven-${MVN_VERSION}" ]; then
  sudo wget -nv \
    "https://${IMPALA_TOOLCHAIN_HOST}/maven/apache-maven-${MVN_VERSION}-bin.tar.gz"
  sha512sum -c - <<< "33d81e0ec785f0207e3e5e3ffb61863e1dca5784c15ac3fb5ff105f69cffbea484eb8d473ea60467a63f7b0570eef8622f2fed8eee96acbe668aa313391cddb3 apache-maven-${MVN_VERSION}-bin.tar.gz"
  sudo tar -C /usr/local -xzf apache-maven-${MVN_VERSION}-bin.tar.gz
  sudo ln -s /usr/local/apache-maven-${MVN_VERSION}/bin/mvn /usr/local/bin
fi

# Optionally try to prepopulate the m2 directory to save time. Since Maven has
# improved download parallelism, this now defaults to false.
if [[ "${PREPOPULATE_M2_REPOSITORY:-false}" == true ]] ; then
  echo ">>> Populating m2 directory..."
  if ! bin/jenkins/populate_m2_directory.py ; then
    echo "Failed to prepopulate the m2 directory. Continuing..."
  fi
else
  echo ">>> Skip populating m2 directory"
fi

./buildall.sh -notests -so
