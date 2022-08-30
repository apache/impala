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
set -euo pipefail
. $IMPALA_HOME/bin/report_build_error.sh
setup_report_build_error

: ${REMOTE_LOAD:=}

# Create a cache pool and encryption keys for tests

PREVIOUS_PRINCIPAL=""
CACHEADMIN_ARGS=""

# If we're kerberized, we need to become hdfs for this:
if ${CLUSTER_DIR}/admin is_kerberized; then
  if [[ -n "${REMOTE_LOAD:-}" ]]; then
    echo "REMOTE_LOAD: $REMOTE_LOAD"
    echo "Remote cluster testing is not supported with Kerberos"
    exit 1
  else
    PREVIOUS_PRINCIPAL=`klist | grep ^Default | awk '{print $3}'`
    PREVIOUS_USER=`echo ${PREVIOUS_PRINCIPAL} | awk -F/ '{print $1}'`
    CACHEADMIN_ARGS="-group supergroup -owner ${PREVIOUS_USER}"
    kinit -k -t ${KRB5_KTNAME} ${MINIKDC_PRINC_HDFS}
  fi
fi

# TODO: Investigate how to setup encryption keys for running HDFS encryption tests
# against a remote cluster, rather than the local mini-cluster (i.e., when REMOTE_LOAD
# is true. See: IMPALA-4344)

if [[ -z "$REMOTE_LOAD" ]]; then  # Otherwise assume KMS isn't setup.
  ${IMPALA_HOME}/testdata/bin/setup-dfs-keys.sh testkey{1,2}
fi

if [[ -n "${REMOTE_LOAD:-}" ]]; then
  # Create test cache pool if HADOOP_USER_NAME has a non-zero length
  if [[ -n "${HADOOP_USER_NAME:-}" ]]; then
      CACHEADMIN_ARGS="${CACHEADMIN_ARGS} -owner ${USER}"
  fi
fi

function create-pool {
  local pool_name=$1
  local pool_args=${2:-}
  if hdfs cacheadmin -listPools ${pool_name} | grep ${pool_name} &>/dev/null; then
    hdfs cacheadmin -removePool ${pool_name}
  fi
  hdfs cacheadmin -addPool ${pool_name} ${pool_args} ${CACHEADMIN_ARGS}
}

create-pool testPool
create-pool testPoolWithTtl "-maxTtl 7d"

# Back to ourselves:
if [ "${PREVIOUS_PRINCIPAL}" != "" ]; then
  kinit -k -t ${KRB5_KTNAME} ${PREVIOUS_PRINCIPAL}
fi

if [[ -n "${HDFS_ERASURECODE_POLICY:-}" ]]; then
  hdfs ec -enablePolicy -policy "${HDFS_ERASURECODE_POLICY}"
  hdfs ec -setPolicy -policy "${HDFS_ERASURECODE_POLICY}" \
    -path "${HDFS_ERASURECODE_PATH:=/test-warehouse}"
fi
