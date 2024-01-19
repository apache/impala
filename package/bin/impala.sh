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
# Script to manage Apache Impala services.
# User can customize environment variables in '../conf/impala-env.sh'.
# User can customize flags in '../conf/[impalad_flags...]', or override them in
# commandline arguments when needed(e.g. multiple instance).
#
# To launch impalad using another username (e.g. "impala"):
# sudo -E -u impala bin/impala.sh start impalad
#
# Note for setup:
# Please set the correct JAVA_HOME in '../conf/impala-env.sh'.
# Please set the correct hostnames in '../conf/[impalad_flags...]'.
# Please modify core-site.xml, hdfs-site.xml, hive-site.xml in conf based on the cluster.
set -euo pipefail

init() {
  : ${IMPALA_HOME:=$(cd $(dirname ${0})/..;pwd)}
  export IMPALA_HOME
  . ${IMPALA_HOME}/conf/impala-env.sh
}

check_counts() {
  local counts=${1} period=${2}
  [[ ${counts} =~ ^[0-9]+$ ]] || (echo "Invalid waiting counts:${counts}." && exit 1)
  [[ ${period} =~ ^[1-9][0-9]*$ ]] || (echo "Invalid waiting period:${period}." && exit 1)
}

# Return 0 if service is running, else otherwise.
status() {
  local service=
  while [[ $# -gt 0 ]]; do
    case ${1} in
      impalad|catalogd|admissiond|statestored) service=${1} && shift && break ;;
      *) usage && exit 1 ;;
    esac
  done
  local service_pidfile_key=${service^^}_PIDFILE
  local service_pidfile=${!service_pidfile_key}
  if [[ ! -f ${service_pidfile} ]]; then
    echo "${service} is stopped."
    return 1
  fi
  local pid=$(cat ${service_pidfile})
  if ps -p ${pid} -o comm=|grep ${service} &> /dev/null ; then
    echo "${service} is running with PID ${pid}."
    return 0
  fi
  echo "${service} is stopped."
  return 1
}

# Return 0 if service is stopped in expected time, else otherwise.
stop_await() {
  local service=${1} service_pidfile=${2} counts=${3} period=${4}
  [[ "${counts}" == "0" ]] && exit 0
  for ((i=1; i<=${counts}; i++)); do
    [[ ${i} -gt 1 ]] && sleep ${period}
    if ! kill -0 ${pid} &> /dev/null; then
      rm ${service_pidfile} && echo "(${i}/${counts}) ${service} is stopped." && return 0
    else
      echo "(${i}/${counts}) Waiting ${service} to stop."
    fi
  done
  return 1
}

#TODO: Add graceful shutdown for impalads
stop() {
  local service= counts=20 period=2
  while [[ $# -gt 0 ]]; do
    case ${1} in
      -c) counts=${2} && shift 2 ;;
      -p) period=${2} && shift 2 ;;
      impalad|catalogd|admissiond|statestored) service=${1} && shift && break ;;
      *) usage && exit 1 ;;
    esac
  done
  check_counts ${counts} ${period}
  local service_pidfile_key=${service^^}_PIDFILE
  local service_pidfile=${!service_pidfile_key}
  if [[ ! -f ${service_pidfile} ]]; then
    echo "Already stopped: PID file '${service_pidfile}' not found."
    exit 0
  fi
  local pid=$(cat ${service_pidfile})
  if ! ps -p ${pid} -o comm=|grep ${service} &> /dev/null ; then
    rm ${service_pidfile}
    echo "Already stopped: ${service} is not running with PID ${pid}." \
    "Removed stale file '${service_pidfile}'."
    exit 0
  fi
  echo "Killing ${service} with PID ${pid}."
  kill ${pid}
  if ! stop_await ${service} ${service_pidfile} ${counts} ${period}; then
    echo "Timed out waiting ${service} to stop, check logs for more details."
    exit 1
  fi
}

prerun() {
  local message=${1:-"on"}
  : ${JAVA_HOME:?"JAVA_HOME must be set to the location of your JDK!"}
  if [[ ${message} == "on" ]]; then
    echo "Using JAVA_HOME: ${JAVA_HOME}"
  fi
  local lib_jvm_dir=$(dirname $(find ${JAVA_HOME} -type f -name libjvm.so | head -1))
  local lib_jsig_dir=$(dirname $(find ${JAVA_HOME} -type f -name libjsig.so | head -1))
  if [[ -n "${HADOOP_HOME:=}" ]]; then
    : ${HADOOP_LIB_DIR:=${HADOOP_HOME}/lib/native}
    if [[ ${message} == "on" ]]; then
      echo "Using hadoop native libs in '${HADOOP_LIB_DIR}'"
    fi
  else
    : ${HADOOP_LIB_DIR:=${IMPALA_HOME}/lib/native}
    if [[ ${message} == "on" ]]; then
      echo "HADOOP_HOME not set, using hadoop native libs in '${HADOOP_LIB_DIR}'"
    fi
  fi
  export LIBHDFS_OPTS="${LIBHDFS_OPTS:=} -Djava.library.path=${HADOOP_LIB_DIR}"
  export LC_ALL=en_US.utf8
  export CLASSPATH="${CLASSPATH}:${IMPALA_HOME}/conf:${IMPALA_HOME}/lib/jars/*"
  export LD_LIBRARY_PATH+=":${IMPALA_HOME}/lib/native:${lib_jvm_dir}:${lib_jsig_dir}"
}

start() {
  prerun
  local service= counts=20 period=2
  while [[ $# -gt 0 ]]; do
    case ${1} in
      -c) counts=${2} && shift 2 ;;
      -p) period=${2} && shift 2 ;;
      impalad|catalogd|admissiond|statestored) service=${1} && shift && break ;;
      *) usage && exit 1 ;;
    esac
  done
  check_counts ${counts} ${period}
  status ${service} && exit 0
  local service_flagfile=${IMPALA_HOME}/conf/${service}_flags
  local service_pidfile_key=${service^^}_PIDFILE
  local service_pidfile=${!service_pidfile_key}
  mkdir -p $(dirname ${service_pidfile})
  # User can override '--flagfile' in the following commandline arguments.
  ${IMPALA_HOME}/sbin/${service} \
    --flagfile=${service_flagfile} \
    ${@} &
  local pid=$!
  echo ${pid} > ${service_pidfile}
  # Sleep 1s so the glog output won't be messed up with waiting messages.
  sleep 1
  health -c ${counts} -p ${period} ${service}
}

restart() {
  stop ${@} && start ${@}
}

health() {
  local service= counts=20 period=2
  while [[ $# -gt 0 ]]; do
    case ${1} in
      -c) counts=${2} && shift 2 ;;
      -p) period=${2} && shift 2 ;;
      impalad|catalogd|admissiond|statestored) service=${1} && shift ;;
      *) usage && exit 1 ;;
    esac
  done
  check_counts ${counts} ${period}
  [[ "${counts}" == "0" ]] && exit 0
  status ${service} || exit 1
  # Determine Web Server port
  local service_flagfile=${IMPALA_HOME}/conf/${service}_flags
  local service_pidfile_key=${service^^}_PIDFILE
  local service_pidfile=${!service_pidfile_key}
  local pid=$(cat ${service_pidfile})
  local port=
  if [[ $(ps --cols 10000 -o args= -p ${pid}) =~ -webserver_port=([0-9]+) ]]; then
    port=${BASH_REMATCH[1]}
  else
    port=$(awk -F= '/-webserver_port=/ {print $2;}' ${service_flagfile})
  fi
  if [[ -z "${port}" ]]; then
    case ${service} in
      impalad) port=25000;;
      catalogd) port=25020;;
      admissiond) port=25030;;
      statestored) port=25010;;
    esac
  fi
  # Request healthz code
  for ((i=1; i<=${counts}; i++)); do
    [[ ${i} -gt 1 ]] && sleep ${period} || true
    local code=$(curl -s http://localhost:${port}/healthz)
    if [[ $? != 0 ]]; then
      echo "(${i}/${counts}) ${service} on port ${port} is not ready."
    elif [[ "${code}" != "OK" ]]; then
      echo "(${i}/${counts}) Waiting for ${service} to be ready."
    else
      echo "(${i}/${counts}) ${service} is ready."
      exit 0
    fi
  done
  echo "Timed out waiting for ${service} to be ready, check logs for more details."
  exit 1
}

usage() {
  echo "Usage: $0 <command> [<options>] <service> [<flags>]"
  echo "       $0 --version"
  echo "       $0 --help"
  echo "  command: {start|stop|restart|status|health}"
  echo "  service: {impalad|catalogd|admissiond|statestored}"
  echo "  flags: in pattern '-key=val...'."
  echo
  echo "  start: start an Impala daemon service, wait until service is ready."
  echo "    options:"
  echo "      -c: maximum count of checks, defaults to 20."
  echo "      -p: seconds of period between checks, defaults to 2."
  echo
  echo "  stop: stop an Impala daemon service, wait until service is stopped."
  echo "    options:"
  echo "      -c: maximum count of checks, defaults to 20."
  echo "      -p: seconds of period between checks, defaults to 2."
  echo
  echo "  restart: restart an Impala daemon service."
  echo "    options: same as start command."
  echo
  echo "  status: check the process status of an Impala daemon service."
  echo
  echo "  health: waiting until an Impala daemon service is ready."
  echo "    options:"
  echo "      -c: maximum count of checks, defaults to 20."
  echo "      -p: seconds of period between checks, defaults to 2."
}

version() {
    if init &> /dev/null && prerun off && ${IMPALA_HOME}/sbin/impalad --version ; then
      exit 0
    fi
    echo "Failed to get version!"
    exit 1
}

main() {
  [[ $# -ge 1 && ${1} == "--help" ]] && usage && exit 0
  [[ $# -ge 1 && ${1} == "--version" ]] && version && exit 0
  [[ $# -lt 2 ]] && usage && exit 1
  local command=${1}
  case ${command} in
    start|stop|restart|status|health) shift && init && ${command} $@ ;;
    *) usage && exit 1 ;;
  esac
}

main ${@}
