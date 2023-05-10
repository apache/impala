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

set -euo pipefail
. $IMPALA_HOME/bin/report_build_error.sh
setup_report_build_error
set -x

function setup-ranger {
  echo "SETTING UP RANGER"

  RANGER_SETUP_DIR="${IMPALA_HOME}/testdata/cluster/ranger/setup"

  perl -wpl -e 's/\$\{([^}]+)\}/defined $ENV{$1} ? $ENV{$1} : $&/eg' \
    "${RANGER_SETUP_DIR}/impala_group_owner.json.template" > \
    "${RANGER_SETUP_DIR}/impala_group_owner.json"

  GROUP_ID_OWNER=$(wget -qO - --auth-no-challenge --user=admin --password=admin \
    --post-file="${RANGER_SETUP_DIR}/impala_group_owner.json" \
    --header="accept:application/json" \
    --header="Content-Type:application/json" \
    http://localhost:6080/service/xusers/secure/groups |
    python -c "import sys, json; print(json.load(sys.stdin)['id'])")
  export GROUP_ID_OWNER

  GROUP_ID_NON_OWNER=$(wget -qO - --auth-no-challenge --user=admin \
    --password=admin --post-file="${RANGER_SETUP_DIR}/impala_group_non_owner.json" \
    --header="accept:application/json" \
    --header="Content-Type:application/json" \
    http://localhost:6080/service/xusers/secure/groups |
    python -c "import sys, json; print(json.load(sys.stdin)['id'])")
  export GROUP_ID_NON_OWNER

  perl -wpl -e 's/\$\{([^}]+)\}/defined $ENV{$1} ? $ENV{$1} : $&/eg' \
    "${RANGER_SETUP_DIR}/impala_user_owner.json.template" > \
    "${RANGER_SETUP_DIR}/impala_user_owner.json"

  perl -wpl -e 's/\$\{([^}]+)\}/defined $ENV{$1} ? $ENV{$1} : $&/eg' \
    "${RANGER_SETUP_DIR}/impala_user_non_owner.json.template" > \
    "${RANGER_SETUP_DIR}/impala_user_non_owner.json"

  if grep "\${[A-Z_]*}" "${RANGER_SETUP_DIR}/impala_user_owner.json"; then
    echo "Found undefined variables in ${RANGER_SETUP_DIR}/impala_user_owner.json."
    exit 1
  fi

  if grep "\${[A-Z_]*}" "${RANGER_SETUP_DIR}/impala_user_non_owner.json"; then
    echo "Found undefined variables in ${RANGER_SETUP_DIR}/impala_user_non_owner.json."
    exit 1
  fi

  wget -O /dev/null --auth-no-challenge --user=admin --password=admin \
    --post-file="${RANGER_SETUP_DIR}/impala_user_owner.json" \
    --header="Content-Type:application/json" \
    http://localhost:6080/service/xusers/secure/users

  wget -O /dev/null --auth-no-challenge --user=admin --password=admin \
    --post-file="${RANGER_SETUP_DIR}/impala_user_non_owner.json" \
    --header="Content-Type:application/json" \
    http://localhost:6080/service/xusers/secure/users

  wget -O /dev/null --auth-no-challenge --user=admin --password=admin \
    --post-file="${RANGER_SETUP_DIR}/impala_service.json" \
    --header="Content-Type:application/json" \
    http://localhost:6080/service/public/v2/api/service

  curl -f -u admin:admin -H "Accept: application/json" \
    -H "Content-Type: application/json" \
    -X PUT http://localhost:6080/service/public/v2/api/policy/5 \
    -d @"${RANGER_SETUP_DIR}/policy_5_revised.json"
}

setup-ranger
