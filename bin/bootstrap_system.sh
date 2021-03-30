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
REDHAT6=
REDHAT7=
REDHAT8=
UBUNTU=
UBUNTU16=
UBUNTU18=
IN_DOCKER=
if [[ -f /etc/redhat-release ]]; then
  REDHAT=true
  echo "Identified redhat system."
  if grep 'release 8\.' /etc/redhat-release; then
    REDHAT8=true
    echo "Identified redhat8 system."
  fi
  if grep 'release 7\.' /etc/redhat-release; then
    REDHAT7=true
    echo "Identified redhat7 system."
  fi
  if grep 'release 6\.' /etc/redhat-release; then
    REDHAT6=true
    echo "Identified redhat6 system."
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
    else
      echo "This script only supports 16.04 or 18.04 of Ubuntu" >&2
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

# Helper function to execute following command only on RedHat
function redhat {
  if [[ "$REDHAT" == true ]]; then
    "$@"
  fi
}

# Helper function to execute following command only on RedHat6
function redhat6 {
  if [[ "$REDHAT6" == true ]]; then
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

echo ">>> Installing build tools"
ubuntu apt-get update
ubuntu apt-get --yes install ccache curl gawk g++ gcc libffi-dev liblzo2-dev \
        libkrb5-dev krb5-admin-server krb5-kdc krb5-user libsasl2-dev \
        libsasl2-modules libsasl2-modules-gssapi-mit libssl-dev make ninja-build \
        python-dev python-setuptools postgresql ssh wget vim-common psmisc \
        lsof openjdk-8-jdk openjdk-8-source openjdk-8-dbg apt-utils git ant

if [[ "$UBUNTU" == true ]]; then
  # Don't use openjdk-8-jdk 8u181-b13-1ubuntu0.16.04.1 which is known to break the
  # surefire tests. If we detect that version, we downgrade to the last known good one.
  # See https://bugs.debian.org/cgi-bin/bugreport.cgi?bug=911925 for details.
  JDK_BAD_VERSION="8u181-b13-1ubuntu0.16.04.1"
  if dpkg -l openjdk-8-jdk | grep -q $JDK_BAD_VERSION; then
    JDK_TARGET_VERSION="8u181-b13-0ubuntu0.16.04.1"
    DEB_DIR=$(mktemp -d)
    pushd $DEB_DIR
    wget --no-verbose \
        "https://launchpadlibrarian.net/380913637/openjdk-8-jdk_8u181-b13-0ubuntu0.16.04.1_amd64.deb" \
        "https://launchpadlibrarian.net/380913636/openjdk-8-jdk-headless_8u181-b13-0ubuntu0.16.04.1_amd64.deb" \
        "https://launchpadlibrarian.net/380913641/openjdk-8-jre_8u181-b13-0ubuntu0.16.04.1_amd64.deb" \
        "https://launchpadlibrarian.net/380913638/openjdk-8-jre-headless_8u181-b13-0ubuntu0.16.04.1_amd64.deb" \
        "https://launchpadlibrarian.net/380913642/openjdk-8-source_8u181-b13-0ubuntu0.16.04.1_all.deb" \
        "https://launchpadlibrarian.net/380913633/openjdk-8-dbg_8u181-b13-0ubuntu0.16.04.1_amd64.deb"
    sudo dpkg -i *.deb
    popd
    rm -rf $DEB_DIR
  fi
fi

# Ubuntu 18.04 installs OpenJDK 11 and configures it as the default Java version.
# Impala is currently tested with OpenJDK 8, so configure that version as the default.
ARCH_NAME=$(uname -p)
if [[ $ARCH_NAME == 'aarch64' ]]; then
  ubuntu18 sudo update-java-alternatives -s java-1.8.0-openjdk-arm64
else
  ubuntu18 sudo update-java-alternatives -s java-1.8.0-openjdk-amd64
fi

redhat sudo yum install -y curl gawk gcc gcc-c++ git krb5-devel krb5-server \
        krb5-workstation libevent-devel libffi-devel make openssl-devel cyrus-sasl \
        cyrus-sasl-gssapi cyrus-sasl-devel cyrus-sasl-plain \
        postgresql postgresql-server \
        wget vim-common nscd cmake lzo-devel fuse-devel zlib-devel \
        psmisc lsof openssh-server redhat-lsb java-1.8.0-openjdk-devel \
        java-1.8.0-openjdk-src

# Enable the Powertools repo for snappy-devel on RedHat 8
redhat8 sudo yum install -y dnf-plugins-core
redhat8 sudo yum install -y --enablerepo="PowerTools*" snappy-devel

# RedHat / CentOS 8 exposes only specific versions of Python.
# Set up unversioned default Python 2.x for older CentOS versions
redhat6 sudo yum install -y python-devel python-setuptools python-argparse
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

# CentOS repos don't contain ccache, so install from EPEL
redhat sudo yum install -y epel-release
redhat sudo yum install -y ccache

# Clean up yum caches
redhat sudo yum clean all

# Download ant for centos
redhat sudo wget -nv \
  https://downloads.apache.org/ant/binaries/apache-ant-1.9.14-bin.tar.gz
redhat sudo sha512sum -c - <<< '487dbd1d7f678a92924ba884a57e910ccb4fe565c554278795a8fdfc80c4e88d81ebc2ccecb5a8f353f0b2076572bb921499a2cadb064e0f44fc406a3c31da20  apache-ant-1.9.14-bin.tar.gz'
redhat sudo tar -C /usr/local -xzf apache-ant-1.9.14-bin.tar.gz
redhat sudo ln -sf /usr/local/apache-ant-1.9.14/bin/ant /usr/local/bin

# Download maven for all OSes, since the OS-packaged version can be
# pretty old.
if [ ! -d /usr/local/apache-maven-3.5.4 ]; then
  sudo wget -nv \
    https://downloads.apache.org/maven/maven-3/3.5.4/binaries/apache-maven-3.5.4-bin.tar.gz
  sudo sha512sum -c - <<< '2a803f578f341e164f6753e410413d16ab60fabe31dc491d1fe35c984a5cce696bc71f57757d4538fe7738be04065a216f3ebad4ef7e0ce1bb4c51bc36d6be86  apache-maven-3.5.4-bin.tar.gz'
  sudo tar -C /usr/local -x --no-same-owner -zf apache-maven-3.5.4-bin.tar.gz
  sudo ln -sf /usr/local/apache-maven-3.5.4/bin/mvn /usr/local/bin

  # reset permissions on redhat8
  # TODO: figure out why this is necessary for redhat8
  MAVEN_DIRECTORY="/usr/local/apache-maven-3.5.4"
  redhat8 indocker sudo chmod 0755 ${MAVEN_DIRECTORY}
  redhat8 indocker sudo chmod 0755 ${MAVEN_DIRECTORY}/{bin,boot}
fi

if ! { service --status-all | grep -E '^ \[ \+ \]  ssh$'; }
then
  ubuntu sudo service ssh start
  # TODO: CentOS/RH 7 uses systemd, and this doesn't work.
  redhat6 sudo service sshd start
  redhat7 notindocker sudo service sshd start
  redhat8 notindocker sudo service sshd start
  redhat7 indocker sudo /usr/bin/ssh-keygen -A
  redhat7 indocker sudo /usr/sbin/sshd
  redhat8 indocker sudo /usr/bin/ssh-keygen -A
  redhat8 indocker sudo /usr/sbin/sshd
  # The CentOS 8.1 image includes /var/run/nologin by mistake; this file prevents
  # SSH logins. See https://github.com/CentOS/sig-cloud-instance-images/issues/60
  redhat8 indocker sudo rm -f /var/run/nologin
fi

# TODO: config ccache to give it plenty of space
# TODO: check that there is enough space on disk to do a build and data load
# TODO: make this work with non-bash shells

echo ">>> Configuring system"

redhat6 sudo service postgresql initdb
redhat6 sudo service postgresql stop
redhat7 notindocker sudo service postgresql initdb
redhat7 notindocker sudo service postgresql stop
redhat7 indocker sudo -u postgres PGDATA=/var/lib/pgsql/data pg_ctl init
redhat8 notindocker sudo service postgresql initdb
redhat8 notindocker sudo service postgresql stop
redhat8 indocker sudo -u postgres PGDATA=/var/lib/pgsql/data pg_ctl init
ubuntu sudo service postgresql stop

# These configurations expose connectiong to PostgreSQL via md5-hashed
# passwords over TCP to localhost, and the local socket is trusted
# widely.
ubuntu sudo sed -ri 's/local +all +all +peer/local all all trust/g' \
  /etc/postgresql/*/main/pg_hba.conf
redhat sudo sed -ri 's/local +all +all +(ident|peer)/local all all trust/g' \
  /var/lib/pgsql/data/pg_hba.conf
# Accept md5 passwords from localhost
redhat sudo sed -i -e 's,\(host.*\)ident,\1md5,' /var/lib/pgsql/data/pg_hba.conf

ubuntu sudo service postgresql start
redhat6 sudo service postgresql start
redhat7 notindocker sudo service postgresql start
redhat8 notindocker sudo service postgresql start
# Important to redirect pg_ctl to a logfile, lest it keep the stdout
# file descriptor open, preventing the shell from exiting.
redhat7 indocker sudo -u postgres PGDATA=/var/lib/pgsql/data bash -c \
  "pg_ctl start -w --timeout=120 >> /var/lib/pgsql/pg.log 2>&1"
redhat8 indocker sudo -u postgres PGDATA=/var/lib/pgsql/data bash -c \
  "pg_ctl start -w --timeout=120 >> /var/lib/pgsql/pg.log 2>&1"

# Set up postgres for HMS
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
echo -e "\n127.0.0.1 $(hostname -s) $(hostname)" | sudo tee -a /etc/hosts
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

# Default on CentOS limits a user to 1024 or 4096 processes (threads) , which isn't
# enough for minicluster with all of its friends.
redhat6 sudo sed -i 's,\*\s*soft\s*nproc\s*[0-9]*$,* soft nproc unlimited,' \
  /etc/security/limits.d/*-nproc.conf
redhat7 sudo sed -i 's,\*\s*soft\s*nproc\s*[0-9]*$,* soft nproc unlimited,' \
  /etc/security/limits.d/*-nproc.conf
redhat8 echo -e "* soft nproc unlimited" | sudo tee -a /etc/security/limits.conf

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

# Ubuntu and RH install JDK's in slightly different paths.
if [[ $UBUNTU == true ]]; then
  # Assert that there's only one glob match.
  [ 1 == $(compgen -G "/usr/lib/jvm/java-8-openjdk-*" | wc -l) ]
  SET_JAVA_HOME="export JAVA_HOME=$(compgen -G '/usr/lib/jvm/java-8-openjdk-*')"
else
  # Assert that there's only one glob match.
  [ 1 == $(compgen -G "/usr/lib/jvm/java-1.8.0-openjdk-*" | wc -l) ]
  SET_JAVA_HOME="export JAVA_HOME=$(compgen -G '/usr/lib/jvm/java-1.8.0-openjdk-*')"
fi

echo -e "\n$SET_JAVA_HOME" >> "${IMPALA_HOME}/bin/impala-config-local.sh"
eval "$SET_JAVA_HOME"

# Assert that we have a java available
test -f $JAVA_HOME/bin/java

# LZO is not needed to compile or run Impala, but it is needed for the data load
echo ">>> Checking out Impala-lzo"
: ${IMPALA_LZO_HOME:="${IMPALA_HOME}/../Impala-lzo"}
if ! [[ -d "$IMPALA_LZO_HOME" ]]
then
  git clone --branch master https://github.com/cloudera/impala-lzo.git "$IMPALA_LZO_HOME"
fi

echo ">>> Checking out and building hadoop-lzo"

: ${HADOOP_LZO_HOME:="${IMPALA_HOME}/../hadoop-lzo"}
if ! [[ -d "$HADOOP_LZO_HOME" ]]
then
  git clone https://github.com/cloudera/hadoop-lzo.git "$HADOOP_LZO_HOME"
fi
cd "$HADOOP_LZO_HOME"
time -p ant package
cd "$IMPALA_HOME"

# Try to prepopulate the m2 directory to save time
if ! bin/jenkins/populate_m2_directory.py ; then
  echo "Failed to prepopulate the m2 directory. Continuing..."
fi
