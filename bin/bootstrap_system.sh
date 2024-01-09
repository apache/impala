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
# to work on Ubuntu 16.04. It clobbers some local environment and system
# configurations, so it is best to run this in a fresh install. It also sets up the
# ~/.bashrc for the calling user and impala-config-local.sh with some environment
# variables to make Impala compile and run after this script is complete.
# When IMPALA_HOME is set, the script will bootstrap Impala development in the
# location specified.
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
#
# This script has some specializations for CentOS/Redhat 6/7 and Ubuntu.
# Of note, inside of Docker, Redhat 7 doesn't allow you to start daemons
# with systemctl, so sshd and postgresql are started manually in those cases.

set -eu -o pipefail

: ${IMPALA_HOME:=$(cd "$(dirname $0)"/..; pwd)}
export IMPALA_HOME

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
fi

set -x

# Determine whether we're running on redhat or ubuntu
REDHAT=
REDHAT7=
REDHAT8=
REDHAT9=
UBUNTU=
UBUNTU16=
UBUNTU18=
UBUNTU20=
UBUNTU22=
IN_DOCKER=
if [[ -f /etc/redhat-release ]]; then
  REDHAT=true
  echo "Identified redhat system."
  if grep 'release 9\.' /etc/redhat-release; then
    REDHAT9=true
    echo "Identified redhat9 system."
  fi
  if grep 'release 8\.' /etc/redhat-release; then
    REDHAT8=true
    echo "Identified redhat8 system."
  fi
  if grep 'release 7\.' /etc/redhat-release; then
    REDHAT7=true
    echo "Identified redhat7 system."
  fi
  # TODO: restrict redhat versions
else
  source /etc/lsb-release
  if [[ $DISTRIB_ID = Ubuntu ]]
  then
    UBUNTU=true
    echo "Identified Ubuntu system."
    # Kerberos setup would pop up dialog boxes without this
    export DEBIAN_FRONTEND=noninteractive
    if [[ $DISTRIB_RELEASE = 16.04 ]]
    then
      UBUNTU16=true
      echo "Identified Ubuntu 16.04 system."
    elif [[ $DISTRIB_RELEASE = 18.04 ]]
    then
      UBUNTU18=true
      echo "Identified Ubuntu 18.04 system."
    elif [[ $DISTRIB_RELEASE = 20.04 ]]
    then
      UBUNTU20=true
      echo "Identified Ubuntu 20.04 system."
    elif [[ $DISTRIB_RELEASE = 22.04 ]]
    then
      UBUNTU22=true
      echo "Identified Ubuntu 22.04 system."
    else
      echo "This script supports Ubuntu versions 16.04, 18.04, 20.04, or 22.04" >&2
      exit 1
    fi
  else
    echo "This script only supports Ubuntu or RedHat" >&2
    exit 1
  fi
fi
if grep docker /proc/1/cgroup; then
  IN_DOCKER=true
  echo "Identified we are running inside of Docker."
fi

# Helper function to execute following command only on Ubuntu
function ubuntu {
  if [[ "$UBUNTU" == true ]]; then
    "$@"
  fi
}

# Helper function to execute following command only on Ubuntu 16.04
function ubuntu16 {
  if [[ "$UBUNTU16" == true ]]; then
    "$@"
  fi
}

# Helper function to execute following command only on Ubuntu 18.04
function ubuntu18 {
  if [[ "$UBUNTU18" == true ]]; then
    "$@"
  fi
}

function ubuntu20 {
  if [[ "$UBUNTU20" == true ]]; then
    "$@"
  fi
}

function ubuntu22 {
  if [[ "$UBUNTU22" == true ]]; then
    "$@"
  fi
}

# Helper function to execute following command only on RedHat
function redhat {
  if [[ "$REDHAT" == true ]]; then
    "$@"
  fi
}

# Helper function to execute following command only on RedHat7
function redhat7 {
  if [[ "$REDHAT7" == true ]]; then
    "$@"
  fi
}
# Helper function to execute following command only on RedHat8
function redhat8 {
  if [[ "$REDHAT8" == true ]]; then
    "$@"
  fi
}
# Helper function to execute following command only on RedHat8
function redhat9 {
  if [[ "$REDHAT9" == true ]]; then
    "$@"
  fi
}
# Helper function to execute following command only in docker
function indocker {
  if [[ "$IN_DOCKER" == true ]]; then
    "$@"
  fi
}
# Helper function to execute following command only outside of docker
function notindocker {
  if [[ "$IN_DOCKER" != true ]]; then
    "$@"
  fi
}

# Note that yum has its own retries; see yum.conf(5).
REAL_APT_GET=$(ubuntu which apt-get)
function apt-get {
  for ITER in $(seq 1 30); do
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

echo ">>> Installing build tools"
if [[ "$UBUNTU" == true ]]; then
  while sudo fuser /var/lib/dpkg/lock-frontend; do
    sleep 1
  done
fi

# Set UBUNTU_JAVA_VERSION, UBUNTU_PACKAGE_ARCH, REDHAT_JAVA_VERSION
source "$IMPALA_HOME/bin/impala-config-java.sh"

ubuntu apt-get update
ubuntu apt-get --yes install ccache curl file gawk g++ gcc apt-utils git libffi-dev \
        libkrb5-dev krb5-admin-server krb5-kdc krb5-user libsasl2-dev \
        libsasl2-modules libsasl2-modules-gssapi-mit libssl-dev make ninja-build \
        python3-dev python3-setuptools python3-venv postgresql \
        ssh wget vim-common psmisc lsof net-tools language-pack-en libxml2-dev \
        libxslt-dev openjdk-${UBUNTU_JAVA_VERSION}-jdk \
        openjdk-${UBUNTU_JAVA_VERSION}-source openjdk-${UBUNTU_JAVA_VERSION}-dbg

# Regular python packages don't exist on Ubuntu 22. Everything is Python 3.
ubuntu16 apt-get --yes install python python-dev python-setuptools
ubuntu18 apt-get --yes install python python-dev python-setuptools
ubuntu20 apt-get --yes install python python-dev python-setuptools

# Required by Kudu in the minicluster
ubuntu20 apt-get --yes install libtinfo5
ubuntu22 apt-get --yes install libtinfo5
ARCH_NAME=$(uname -p)
if [[ $ARCH_NAME == 'aarch64' ]]; then
  ubuntu apt-get --yes install unzip pkg-config flex maven python3-pip build-essential \
          texinfo bison autoconf automake libtool libz-dev libncurses-dev \
          libncurses5-dev libreadline-dev
fi

# Configure the default Java version to be the version we selected.
ubuntu sudo update-java-alternatives -s \
    java-1.${UBUNTU_JAVA_VERSION}.0-openjdk-${UBUNTU_PACKAGE_ARCH}

redhat sudo yum install -y file gawk gcc gcc-c++ git krb5-devel krb5-server \
        krb5-workstation libevent-devel libffi-devel make openssl-devel cyrus-sasl \
        cyrus-sasl-gssapi cyrus-sasl-devel cyrus-sasl-plain \
        postgresql postgresql-server rpm-build \
        wget vim-common nscd cmake zlib-devel \
        procps psmisc lsof openssh-server python3-devel python3-setuptools \
        net-tools langpacks-en glibc-langpack-en libxml2-devel libxslt-devel \
        java-${REDHAT_JAVA_VERSION}-openjdk-src java-${REDHAT_JAVA_VERSION}-openjdk-devel

# fuse-devel doesn't exist for Redhat 9
redhat7 sudo yum install -y fuse-devel curl
redhat8 sudo yum install -y fuse-devel curl
# Redhat9 can have curl-minimal preinstalled, which can conflict with curl.
# Adding --allowerasing allows curl to replace curl-minimal.
redhat9 sudo yum install -y --allowerasing curl

# RedHat / CentOS 8 exposes only specific versions of Python.
# Set up unversioned default Python 2.x for older CentOS versions
redhat7 sudo yum install -y python-devel python-setuptools python-argparse

# Install Python 2.x explicitly for CentOS 8
function setup_python2() {
  if command -v python && [[ $(python --version 2>&1 | cut -d ' ' -f 2) =~ 2\. ]]; then
    echo "We have Python 2.x";
  else
    if ! command -v python2; then
      # Python2 needs to be installed
      sudo dnf install -y python2
    fi
    # Here Python2 is installed, but is not the default Python.
    # 1. Link pip's version to Python's version
    sudo alternatives --add-slave python /usr/bin/python2 /usr/bin/pip pip /usr/bin/pip2
    sudo alternatives --add-slave python /usr/libexec/no-python  /usr/bin/pip pip \
        /usr/libexec/no-python
    # 2. Set Python2 (with pip2) to be the system default.
    sudo alternatives --set python /usr/bin/python2
  fi
  # Here the Python2 runtime is already installed, add the dev package
  sudo dnf -y install python2-devel
}

redhat8 setup_python2
redhat8 pip install --user argparse

# Point Python to Python 3 for Redhat 9 and Ubuntu 22
function setup_python3() {
  # If python is already set, then use it. Otherwise, try to point python to python3.
  if ! command -v python > /dev/null; then
    if command -v python3 ; then
      # Newer OSes (e.g. Redhat 9 and equivalents) make it harder to get Python 2, and we
      # need to start using Python 3 by default.
      # For these new OSes (Ubuntu 22, Redhat 9), there is no alternative entry for
      # python, so we need to create one from scratch.
      if command -v alternatives > /dev/null; then
        if sudo alternatives --list | grep python > /dev/null ; then
          sudo alternatives --set python /usr/bin/python3
        else
          # The alternative doesn't exist, create it
          sudo alternatives --install /usr/bin/python python /usr/bin/python3 20
        fi
      elif command -v update-alternatives > /dev/null; then
        # This is what Ubuntu 20/22+ does. There is no official python alternative,
        # so we need to create one.
        sudo update-alternatives --install /usr/bin/python python /usr/bin/python3 20
      else
        echo "ERROR: trying to set python to point to python3"
        echo "ERROR: alternatives/update-alternatives don't exist, so giving up..."
        exit 1
      fi
    fi
  fi
}

redhat9 setup_python3
ubuntu22 setup_python3

# CentOS repos don't contain ccache, so install from EPEL
redhat sudo yum install -y epel-release
redhat sudo yum install -y ccache

# Clean up yum caches
redhat sudo yum clean all

# Download Maven since the packaged version is pretty old.
if [ ! -d /usr/local/apache-maven-3.9.2 ]; then
  sudo wget -nv \
    https://archive.apache.org/dist/maven/maven-3/3.9.2/binaries/apache-maven-3.9.2-bin.tar.gz
  sha512sum -c - <<< '900bdeeeae550d2d2b3920fe0e00e41b0069f32c019d566465015bdd1b3866395cbe016e22d95d25d51d3a5e614af2c83ec9b282d73309f644859bbad08b63db  apache-maven-3.9.2-bin.tar.gz'
  sudo tar -C /usr/local -xzf apache-maven-3.9.2-bin.tar.gz
  # Ensure that Impala's preferred version is installed locally,
  # even if a previous version exists there.
  sudo ln -s -f /usr/local/apache-maven-3.9.2/bin/mvn /usr/local/bin

  # reset permissions on redhat8
  # TODO: figure out why this is necessary for redhat8
  MAVEN_DIRECTORY="/usr/local/apache-maven-3.9.2"
  redhat8 indocker sudo chmod 0755 ${MAVEN_DIRECTORY}
  redhat8 indocker sudo chmod 0755 ${MAVEN_DIRECTORY}/{bin,boot}
  redhat9 indocker sudo chmod 0755 ${MAVEN_DIRECTORY}
  redhat9 indocker sudo chmod 0755 ${MAVEN_DIRECTORY}/{bin,boot}
fi

if ! { service --status-all | grep -E '^ \[ \+ \]  ssh$'; }
then
  ubuntu sudo service ssh start
  redhat notindocker sudo service sshd start
  redhat indocker sudo /usr/bin/ssh-keygen -A
  redhat indocker sudo /usr/sbin/sshd
  # The CentOS 8.1 image includes /var/run/nologin by mistake; this file prevents
  # SSH logins. See https://github.com/CentOS/sig-cloud-instance-images/issues/60
  redhat8 indocker sudo rm -f /var/run/nologin
fi

# TODO: config ccache to give it plenty of space
# TODO: check that there is enough space on disk to do a build and data load
# TODO: make this work with non-bash shells

echo ">>> Configuring system"

redhat notindocker sudo service postgresql initdb
redhat notindocker sudo service postgresql stop
redhat indocker sudo -u postgres PGDATA=/var/lib/pgsql/data pg_ctl init
ubuntu sudo service postgresql stop

# These configurations expose connectiong to PostgreSQL via md5-hashed
# passwords over TCP to localhost, and the local socket is trusted
# widely.
ubuntu sudo sed -ri 's/local +all +all +peer/local all all trust/g' \
  /etc/postgresql/*/main/pg_hba.conf
# Accept remote connections from the hosts in the same subnet.
ubuntu sudo sed -ri "s/#listen_addresses = 'localhost'/listen_addresses = '0.0.0.0'/g" \
  /etc/postgresql/*/main/postgresql.conf
ubuntu sudo sed -ri 's/host +all +all +127.0.0.1\/32/host all all samenet/g' \
  /etc/postgresql/*/main/pg_hba.conf
redhat sudo sed -ri 's/local +all +all +(ident|peer)/local all all trust/g' \
  /var/lib/pgsql/data/pg_hba.conf
# Accept md5 passwords from localhost
redhat sudo sed -i -e 's,\(host.*\)ident,\1md5,' /var/lib/pgsql/data/pg_hba.conf
# Accept remote connections from the hosts in the same subnet.
redhat sudo sed -ri "s/#listen_addresses = 'localhost'/listen_addresses = '0.0.0.0'/g" \
  /var/lib/pgsql/data/postgresql.conf
redhat sudo sed -ri 's/host +all +all +127.0.0.1\/32/host all all samenet/g' \
  /var/lib/pgsql/data/pg_hba.conf

ubuntu sudo service postgresql start
redhat notindocker sudo service postgresql start
# Important to redirect pg_ctl to a logfile, lest it keep the stdout
# file descriptor open, preventing the shell from exiting.
redhat indocker sudo -u postgres PGDATA=/var/lib/pgsql/data bash -c \
  "pg_ctl start -w --timeout=120 >> /var/lib/pgsql/pg.log 2>&1"

# Set up postgres for HMS
if ! [[ 1 = $(sudo -u postgres psql -At -c "SELECT count(*) FROM pg_roles WHERE rolname = 'hiveuser';") ]]
then
  sudo -u postgres psql -c "CREATE ROLE hiveuser LOGIN PASSWORD 'password';"
fi
sudo -u postgres psql -c "ALTER ROLE hiveuser WITH CREATEDB;"
# On Ubuntu 18.04 aarch64 version, the sql 'select * from pg_roles' blocked,
# because output of 'select *' is too long to display in 1 line.
# So here just change it to 'select count(*)' as a work around.
if [[ $ARCH_NAME == 'aarch64' ]]; then
  sudo -u postgres psql -c "SELECT count(*) FROM pg_roles WHERE rolname = 'hiveuser';"
else
  sudo -u postgres psql -c "SELECT * FROM pg_roles WHERE rolname = 'hiveuser';"
fi

# Setup ssh to ssh to localhost
mkdir -p ~/.ssh
chmod go-rwx ~/.ssh
if ! [[ -f ~/.ssh/id_rsa ]]
then
  ssh-keygen -t rsa -N '' -q -f ~/.ssh/id_rsa
fi

{ echo "" | cat - ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys; } && chmod 0600 ~/.ssh/authorized_keys
echo -e "\nNoHostAuthenticationForLocalhost yes" >> ~/.ssh/config && chmod 0600 ~/.ssh/config
ssh localhost whoami

# Workarounds for HDFS networking issues: On the minicluster, tests that rely
# on WebHDFS may fail with "Connection refused" errors because the namenode
# will return a "Location:" redirect to the hostname, but the datanode is only
# listening on localhost. See also HDFS-13797. To reproduce this, the following
# snippet may be useful:
#
#  $impala-python
#  >>> import logging
#  >>> logging.basicConfig(level=logging.DEBUG)
#  >>> logging.getLogger("requests.packages.urllib3").setLevel(logging.DEBUG)
#  >>> from pywebhdfs.webhdfs import PyWebHdfsClient
#  >>> PyWebHdfsClient(host='localhost',port='5070', user_name='hdfs').read_file(
#         "/test-warehouse/tpch.region/region.tbl")
#  INFO:...:Starting new HTTP connection (1): localhost
#  DEBUG:...:"GET /webhdfs/v1//t....tbl?op=OPEN&user.name=hdfs HTTP/1.1" 307 0
#  INFO:...:Starting new HTTP connection (1): HOSTNAME.DOMAIN
#  Traceback (most recent call last):
#    ...
#  ...ConnectionError: ('Connection aborted.', error(111, 'Connection refused'))
# Prefer the FQDN first for rpc-mgr-kerberized-test as newer krb5 requires FQDN.
echo -e "\n127.0.0.1 $(hostname) $(hostname -s)" | sudo tee -a /etc/hosts
#
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
echo -e "\n* - nofile 1048576" | sudo tee -a /etc/security/limits.conf

# Increase memlock for HDFS caching. On RedHat systems this defaults to 64 (KB). Ubuntu
# uses systemd, which sets its own default. With Ubuntu 18.04 that default is 16 KB,
# 20.04+ defaults to 64 MB. Set all to 64 MB for the current user; Impala test systems
# require 10s of GBs of memory, so this setting should not be a problem.
echo -e "$USER - memlock 65536" | sudo tee /etc/security/limits.d/10-memlock.conf

# Default on CentOS limits a user to 1024 or 4096 processes (threads) , which isn't
# enough for minicluster with all of its friends.
redhat7 sudo sed -i 's,\*\s*soft\s*nproc\s*[0-9]*$,* soft nproc unlimited,' \
  /etc/security/limits.d/*-nproc.conf
redhat8 echo -e "* soft nproc unlimited" | sudo tee -a /etc/security/limits.conf
redhat9 echo -e "* soft nproc unlimited" | sudo tee -a /etc/security/limits.conf

echo ">>> Checking out Impala"

# If there is no Impala git repo, get one now
if ! [[ -d "$IMPALA_HOME" ]]
then
  time -p git clone https://gitbox.apache.org/repos/asf/impala.git "$IMPALA_HOME"
fi
cd "$IMPALA_HOME"
SET_IMPALA_HOME="export IMPALA_HOME=$(pwd)"
echo -e "\n$SET_IMPALA_HOME" >> ~/.bashrc
eval "$SET_IMPALA_HOME"

# Try to prepopulate the m2 directory to save time
if [[ "${PREPOPULATE_M2_REPOSITORY:-true}" == true ]] ; then
  echo ">>> Populating m2 directory..."
  if ! bin/jenkins/populate_m2_directory.py ; then
    echo "Failed to prepopulate the m2 directory. Continuing..."
  fi
else
  echo ">>> Skip populating m2 directory"
fi
