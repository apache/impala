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

# This script builds Impala from scratch. It is known to work on Ubuntu 16.04. To run it
# you need to have:
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
sudo -E apt-get update
sudo -E apt-get --yes install g++ gcc git libsasl2-dev libssl-dev make python-dev \
     python-setuptools python3-dev python3-setuptools python3-venv libffi-dev \
     libkrb5-dev krb5-admin-server krb5-kdc krb5-user libxml2-dev libxslt-dev

source /etc/lsb-release

# Ubuntu 20's Python 2.7.18-1~20.04.5 version has a bug in its tarfile support.
# If we detect the affected tarfile.py, download a patched version and overwrite it.
if [[ $DISTRIB_ID == Ubuntu && $DISTRIB_RELEASE == 20.04 ]]; then
  if [[ -f /usr/lib/python2.7/tarfile.py ]]; then
    TARFILE_PY_HASH=$(sha1sum /usr/lib/python2.7/tarfile.py | cut -d' ' -f1)
    if [[ "${TARFILE_PY_HASH}" == "6e1a6d9ea2a535cbb17fe266ed9ac76eb5e27b89" ]]; then
      TMP_DIR=$(mktemp -d)
      pushd $TMP_DIR
      wget -nv https://launchpadlibrarian.net/759546541/tarfile.py
      sudo cp tarfile.py /usr/lib/python2.7/tarfile.py
      popd
      rm -rf $TMP_DIR
    fi
  fi
fi

JDK_VERSION=8
if [[ $DISTRIB_RELEASE = 14.04 ]]
then
  JDK_VERSION=7
fi
sudo apt-get --yes install openjdk-${JDK_VERSION}-jdk openjdk-${JDK_VERSION}-source
export JAVA_HOME=/usr/lib/jvm/java-${JDK_VERSION}-openjdk-amd64

# Download Maven since the packaged version is pretty old.
MVN_VERSION="3.9.8"
if [ ! -d "/usr/local/apache-maven-${MVN_VERSION}" ]; then
  sudo wget -nv \
    "https://archive.apache.org/dist/maven/maven-3/${MVN_VERSION}/binaries/apache-maven-${MVN_VERSION}-bin.tar.gz"
  sha512sum -c - <<< "7d171def9b85846bf757a2cec94b7529371068a0670df14682447224e57983528e97a6d1b850327e4ca02b139abaab7fcb93c4315119e6f0ffb3f0cbc0d0b9a2 apache-maven-${MVN_VERSION}-bin.tar.gz"
  sudo tar -C /usr/local -xzf apache-maven-${MVN_VERSION}-bin.tar.gz
  sudo ln -s /usr/local/apache-maven-${MVN_VERSION}/bin/mvn /usr/local/bin
fi

# Try to prepopulate the m2 directory to save time
if ! bin/jenkins/populate_m2_directory.py ; then
  echo "Failed to prepopulate the m2 directory. Continuing..."
fi

./buildall.sh -notests -so
