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

# This script bootstraps a system for Impala development from almost nothing; it is known
# to work on Ubuntu 14.04 and 16.04. It clobbers some local environment and system
# configurations, so it is best to run this in a fresh install. It also sets up the
# ~/.bashrc for the calling user and impala-config-local.sh with some environment
# variables to make Impala compile and run after this script is complete.
#
# The intended user is a person who wants to start contributing code to Impala. This
# script serves as an executable reference point for how to get started.
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

if [[ -t 1 ]] # if on an interactive terminal
then
  echo "This script will clobber some system settings. Are you sure you want to"
  echo -n "continue? "
  while true
  do
    read -p "[yes/no] " ANSWER
    ANSWER=$(echo "$ANSWER" | tr /a-z/ /A-Z/)
    if [[ $ANSWER = YES ]]
    then
      break
    elif [[ $ANSWER = NO ]]
    then
      echo "OK, Bye!"
      exit 1
    fi
  done
else
  export DEBIAN_FRONTEND=noninteractive
fi

set -x

source /etc/lsb-release

if ! [[ $DISTRIB_ID = Ubuntu ]]
then
  echo "This script only supports Ubuntu" >&2
  exit 1
fi

if ! [[ $DISTRIB_RELEASE = 14.04 || $DISTRIB_RELEASE = 16.04 ]]
then
  echo "This script only supports Ubuntu 14.04 and 16.04" >&2
  exit 1
fi

REAL_APT_GET=$(which apt-get)
function apt-get {
  for ITER in $(seq 1 20); do
    echo "ATTEMPT: ${ITER}"
    if sudo -E "${REAL_APT_GET}" "$@"
    then
      return 0
    fi
    sleep "${ITER}"
  done
  echo "NO MORE RETRIES"
  return 1
}

apt-get update
apt-get --yes install apt-utils
apt-get --yes install git

# If there is no Impala git repo, get one now
if ! [[ -d ~/Impala ]]
then
  time -p git clone https://git-wip-us.apache.org/repos/asf/incubator-impala.git ~/Impala
fi
cd ~/Impala
SET_IMPALA_HOME="export IMPALA_HOME=$(pwd)"
echo "$SET_IMPALA_HOME" >> ~/.bashrc
eval "$SET_IMPALA_HOME"

apt-get --yes install ccache g++ gcc libffi-dev liblzo2-dev libkrb5-dev \
        krb5-admin-server krb5-kdc krb5-user libsasl2-dev libsasl2-modules \
        libsasl2-modules-gssapi-mit libssl-dev make maven ninja-build ntp \
        ntpdate python-dev python-setuptools postgresql ssh wget vim-common psmisc

if ! { service --status-all | grep -E '^ \[ \+ \]  ssh$'; }
then
  sudo service ssh start
fi

# TODO: config ccache to give it plenty of space
# TODO: check that there is enough space on disk to do a build and data load
# TODO: make this work with non-bash shells

JDK_VERSION=8
if [[ $DISTRIB_RELEASE = 14.04 ]]
then
  JDK_VERSION=7
fi
apt-get --yes install openjdk-${JDK_VERSION}-jdk openjdk-${JDK_VERSION}-source
SET_JAVA_HOME="export JAVA_HOME=/usr/lib/jvm/java-${JDK_VERSION}-openjdk-amd64"
echo "$SET_JAVA_HOME" >> "${IMPALA_HOME}/bin/impala-config-local.sh"
eval "$SET_JAVA_HOME"

sudo service ntp stop
sudo ntpdate us.pool.ntp.org
# If on EC2, use Amazon's ntp servers
if which dmidecode && { sudo dmidecode -s bios-version | grep amazon; }
then
  sudo sed -i 's/ubuntu\.pool/amazon\.pool/' /etc/ntp.conf
  grep amazon /etc/ntp.conf
  grep ubuntu /etc/ntp.conf
fi
# While it is nice to have ntpd running to keep the clock in sync, that does not work in a
# --privileged docker container, and a non-privileged container cannot run ntpdate, which
# is strictly needed by Kudu.
# TODO: Make privileged docker start ntpd
sudo service ntp start || grep docker /proc/1/cgroup

# IMPALA-3932, IMPALA-3926
if [[ $DISTRIB_RELEASE = 16.04 ]]
then
  SET_LD_LIBRARY_PATH='export LD_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu/${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}'
elif [[ $DISTRIB_RELEASE = 14.04 ]]
then
  SET_LD_LIBRARY_PATH="unset LD_LIBRARY_PATH"
fi
echo "$SET_LD_LIBRARY_PATH" >> "${IMPALA_HOME}/bin/impala-config-local.sh"
eval "$SET_LD_LIBRARY_PATH"

# TODO: What are the security implications of this?
for PG_AUTH_FILE in /etc/postgresql/*/main/pg_hba.conf
do
  sudo sed -ri 's/local +all +all +peer/local all all trust/g' $PG_AUTH_FILE
done
sudo service postgresql restart
sudo /etc/init.d/postgresql reload
sudo service postgresql restart

# Set up postgress for HMS
if ! [[ 1 = $(sudo -u postgres psql -At -c "SELECT count(*) FROM pg_roles WHERE rolname = 'hiveuser';") ]]
then
  sudo -u postgres psql -c "CREATE ROLE hiveuser LOGIN PASSWORD 'password';"
fi
sudo -u postgres psql -c "ALTER ROLE hiveuser WITH CREATEDB;"
sudo -u postgres psql -c "SELECT * FROM pg_roles WHERE rolname = 'hiveuser';"

# Setup ssh to ssh to localhost
mkdir -p ~/.ssh
chmod go-rwx ~/.ssh
if ! [[ -f ~/.ssh/id_rsa ]]
then
  ssh-keygen -t rsa -N '' -q -f ~/.ssh/id_rsa
fi
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
echo "NoHostAuthenticationForLocalhost yes" >> ~/.ssh/config
ssh localhost whoami

# Workarounds for HDFS networking issues
echo "127.0.0.1 $(hostname -s) $(hostname)" | sudo tee -a /etc/hosts
# In Docker, one can change /etc/hosts as above but not with sed -i. The error message is
# "sed: cannot rename /etc/sedc3gPj8: Device or resource busy". The following lines are
# basically sed -i but with cp instead of mv for -i part.
NEW_HOSTS=$(mktemp)
sed 's/127.0.1.1/127.0.0.1/g' /etc/hosts > "${NEW_HOSTS}"
diff -u /etc/hosts "${NEW_HOSTS}" || true
sudo cp "${NEW_HOSTS}" /etc/hosts
rm "${NEW_HOSTS}"

sudo mkdir -p /var/lib/hadoop-hdfs
sudo chown $(whoami) /var/lib/hadoop-hdfs/

# TODO: restrict this to only the users it is needed for
echo "* - nofile 1048576" | sudo tee -a /etc/security/limits.conf

# LZO is not needed to compile or run Impala, but it is needed for the data load
if ! [[ -d ~/Impala-lzo ]]
then
  git clone https://github.com/cloudera/impala-lzo.git ~/Impala-lzo
fi
if ! [[ -d ~/hadoop-lzo ]]
then
  git clone https://github.com/cloudera/hadoop-lzo.git ~/hadoop-lzo
fi
cd ~/hadoop-lzo/
time -p ant package
cd "$IMPALA_HOME"
