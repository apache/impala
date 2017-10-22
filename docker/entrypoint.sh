#!/bin/bash
#
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
# Entrypoint code for test-with-docker.py containers. test-with-docker.py
# will create Docker containers with this script as the entrypoint,
# with a variety of arguments. See test-with-docker.py for a more
# general overview.
#
# This assumes that the following are already mounted inside
# the container:
#   /etc/localtime                      -> /mnt/localtime
#     Helps timestamps be in the time zone of the host
#   $IMPALA_HOME [a git repo of Impala] -> /repo
#     Used to check out Impala afresh
#   $IMPALA_HOME/logs/docker/<n1>/<n2> -> /logs
#     Used to save logs out to host.
#     <n1> represents the --name passed into
#     test-with-docker for the test run. <n2>
#     indicates which specific container is being run.
#   ~/.ccache [configurable]            -> /ccache
#     Used to speed up builds.
#
# Usage:
#   entrypoint.sh build <uid>
#   entrypoint.sh test_suite <suite>
#      where <suite> is one of: BE_TEST JDBC_TEST CLUSTER_TEST
#                               EE_TEST_SERIAL EE_TEST_PARALLEL

# Boostraps the container by creating a user and adding basic tools like Python and git.
# Takes a uid as an argument for the user to be created.
function build() {
  # Handy for testing.
  if [[ $TEST_TEST_WITH_DOCKER ]]; then
    # We sleep busily so that CPU metrics will show usage, to
    # better exercise the timeline code.
    echo sleeping busily for 4 seconds
    bash -c 'while [[ $SECONDS -lt 4 ]]; do :; done'
    return
  fi

  # Configure timezone, so any timestamps that appear are coherent with the host.
  configure_timezone

  # Assert we're superuser.
  [ "$(id -u)" = 0 ]
  if id $1 2> /dev/null; then
    echo "User with id $1 already exists. Please run this as a user id missing from " \
      "the base Ubuntu container."
    echo
    echo "Container users:"
    paste <(cut -d : -f3 /etc/passwd) <(cut -d : -f1 /etc/passwd) | sort -n
    exit 1
  fi
  apt-get update
  apt-get install -y sudo git lsb-release python

  adduser --disabled-password --gecos "" --uid $1 impdev
  echo "impdev ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers

  su impdev -c "$0 build_impdev"
}

# Sets up Impala environment
function impala_environment() {
  pushd /home/impdev/Impala
  export IMPALA_HOME=/home/impdev/Impala
  export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
  source bin/impala-config.sh
  popd
}

# Starts SSH and PostgreSQL; configures container as necessary;
# prepares Kudu for starting.
function boot_container() {
  pushd /home/impdev/Impala

  # Required for metastore
  sudo service postgresql start

  # Required for starting HBase
  sudo service ssh start

  # Make log directories. This is typically done in buildall.sh.
  mkdir -p logs/be_tests logs/fe_tests/coverage logs/ee_tests logs/custom_cluster_tests

  # Update /etc/hosts to remove the entry for the unique docker hostname,
  # and instead point it to 127.0.0.1. Otherwise, HttpFS returns Location:
  # redirects to said hostname, but the relevant datanode isn't listening
  # on the wildcard address.
  sed -e /$(hostname)/d /etc/hosts -e /127.0.0.1/s,localhost,"localhost $(hostname)," \
    > /tmp/hosts
  # "sed -i" in place doesn't work on Docker, because /etc/hosts is a bind mount.
  sudo cp /tmp/hosts /etc/hosts

  echo Hostname: $(hostname)
  echo Hosts file:
  cat /etc/hosts

  # Make a copy of Kudu's WALs to avoid isue with Docker filesystems (aufs and
  # overlayfs) that don't support os.rename(2) on directories, which Kudu
  # requires. We make a fresh copy of the data, in which case rename(2) works
  # presumably because there's only one layer involved. See
  # https://issues.apache.org/jira/browse/KUDU-1419.
  cd /home/impdev/Impala/testdata
  for x in cluster/cdh*/node-*/var/lib/kudu/*/wal; do
    mv $x $x-orig
    cp -r $x-orig $x
    rm -r $x-orig
  done

  # Wait for postgresql to really start; if it doesn't, Hive Metastore will fail to start.
  for i in {1..120}; do
    echo connecting to postgresql attempt $i
    if sudo -u postgres psql -c "select 1"; then
      break
    else
      sleep 2
    fi
  done
  sudo -u postgres psql -c "select 1"

  popd
}

# Runs bootstrap_system.sh and then builds Impala.
function build_impdev() {
  # Assert we're impdev now.
  [ "$(id -un)" = impdev ]

  # Link in ccache from host.
  ln -s /ccache /home/impdev/.ccache

  # Instead of doing a full "git clone" of /repo, which is the host's checkout,
  # we only fetch one branch, without tags. This keeps the checkout
  # considerably lighter.
  mkdir /home/impdev/Impala
  pushd /home/impdev/Impala
  git init
  git fetch /repo --no-tags HEAD
  git checkout -b test-with-docker FETCH_HEAD

  # Link in logs. Logs are on the host since that's the most important thing to
  # look at after the tests are run.
  ln -sf /logs logs

  bin/bootstrap_system.sh
  impala_environment

  # Builds Impala and loads test data.
  # Note that IMPALA-6494 prevents us from using shared library builds,
  # which are smaller and thereby speed things up.
  ./buildall.sh -noclean -format -testdata -skiptests

  # Shut down things cleanly.
  testdata/bin/kill-all.sh

  # Shutting down PostgreSQL nicely speeds up it's start time for new containers.
  sudo service postgresql stop

  # Clean up things we don't need to reduce image size
  find be -name '*.o' -execdir rm '{}' + # ~1.6GB

  popd
}

# Runs a suite passed in as the first argument. Tightly
# coupled with Impala's run-all-tests and the suite names.
# from test-with-docker.py.
#
# Before running tests, starts up the minicluster.
function test_suite() {
  cd /home/impdev/Impala

  # These test suites are for testing.
  if [[ $1 == NOOP ]]; then
    return 0
  fi
  if [[ $1 == NOOP_FAIL ]]; then
    return 1
  fi
  if [[ $1 == NOOP_SLEEP_FOREVER ]]; then
    # Handy to test timeouts.
    while true; do sleep 60; done
  fi

  # Assert that we're running as impdev
  [ "$(id -un)" = impdev ]

  # Assert that /home/impdev/Impala/logs is a symlink to /logs.
  [ "$(readlink /home/impdev/Impala/logs)" = /logs ]

  boot_container
  impala_environment

  # By default, the JVM will use 1/4 of your OS memory for its heap size. For a
  # long-running test, this will delay GC inside of impalad's leading to
  # unnecessarily large process RSS footprints. We cap the heap size at
  # a more reasonable size.  Note that "test_insert_large_string" fails
  # at 2g and 3g, so the suite that includes it (EE_TEST_PARALLEL) gets
  # additional memory.
  #
  # Similarly, bin/start-impala-cluster typically configures the memlimit
  # to be 80% of the machine memory, divided by the number of daemons.
  # If multiple containers are to be run simultaneously, this is scaled
  # down in test-with-docker.py (and further configurable with --impalad-mem-limit-bytes)
  # and passed in via $IMPALAD_MEM_LIMIT_BYTES to the container. There is a
  # relationship between the number of parallel tests that can be run by py.test and this
  # limit.
  JVM_HEAP_GB=2
  if [[ $1 = EE_TEST_PARALLEL ]]; then
    JVM_HEAP_GB=4
  fi
  export TEST_START_CLUSTER_ARGS="--jvm_args=-Xmx${JVM_HEAP_GB}g \
    --impalad_args=--mem_limit=$IMPALAD_MEM_LIMIT_BYTES"

  # BE tests don't require the minicluster, so we can run them directly.
  if [[ $1 = BE_TEST ]]; then
    # IMPALA-6494: thrift-server-test fails in Ubuntu16.04 for the moment; skip it.
    export SKIP_BE_TEST_PATTERN='thrift-server-test*'
    if ! bin/run-backend-tests.sh; then
      echo "Tests $1 failed!"
      return 1
    else
      echo "Tests $1 succeeded!"
      return 0
    fi
  fi

  # Start the minicluster
  testdata/bin/run-all.sh

  export MAX_PYTEST_FAILURES=0
  # Choose which suite to run; this is how run-all.sh chooses between them.
  export FE_TEST=false
  export BE_TEST=false
  export EE_TEST=false
  export JDBC_TEST=false
  export CLUSTER_TEST=false

  eval "export ${1}=true"

  if [[ ${1} = "EE_TEST_SERIAL" ]]; then
    # We bucket the stress tests with the parallel tests.
    export RUN_TESTS_ARGS="--skip-parallel --skip-stress"
    export EE_TEST=true
  elif [[ ${1} = "EE_TEST_PARALLEL" ]]; then
    export RUN_TESTS_ARGS="--skip-serial"
    export EE_TEST=true
  fi

  ret=0

  # Run tests.
  if ! time -p bin/run-all-tests.sh; then
    ret=1
    echo "Tests $1 failed!"
  else
    echo "Tests $1 succeeded!"
  fi
  # Oddly, I've observed bash fail to exit (and wind down the container),
  # leading to test-with-docker.py hitting a timeout. Killing the minicluster
  # daemons fixes this.
  testdata/bin/kill-all.sh || true
  return $ret
}

# Ubuntu's tzdata package is very finnicky, and if you
# mount /etc/localtime from the host to the container directly,
# it fails to install. However, if you make it a symlink
# and configure /etc/timezone to something that's not an
# empty string, you'll get the right behavior.
#
# The post installation script is findable by looking for "tzdata.postinst"
#
# Use this command to reproduce the Ubuntu issue:
#   docker run -v /etc/localtime:/mnt/localtime -ti ubuntu:16.04 bash -c '
#     date
#     ln -sf /mnt/localtime /etc/localtime
#     date +%Z > /etc/timezone
#     date
#     apt-get update > /dev/null
#     apt-get install tzdata
#     date'
function configure_timezone() {
  if ! diff -q /etc/localtime /mnt/localtime 2> /dev/null; then
    ln -sf /mnt/localtime /etc/localtime
    date +%Z > /etc/timezone
  fi
}

function main() {
  set -e

  # Run given command
  CMD="$1"
  shift

  echo ">>> ${CMD} $@ (begin)"
  set -x
  if "${CMD}" "$@"; then
    ret=0
  else
    ret=$?
  fi
  set +x
  echo ">>> ${CMD} $@ ($ret) (end)"
  exit $ret
}

# Run main() unless we're being sourced.
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  main "$@"
fi
