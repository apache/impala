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

  GROUP_ID_NON_OWNER_2=$(wget -qO - --auth-no-challenge --user=admin \
    --password=admin --post-file="${RANGER_SETUP_DIR}/impala_group_non_owner_2.json" \
    --header="accept:application/json" \
    --header="Content-Type:application/json" \
    http://localhost:6080/service/xusers/secure/groups |
    python -c "import sys, json; print(json.load(sys.stdin)['id'])")
  export GROUP_ID_NON_OWNER_2

  perl -wpl -e 's/\$\{([^}]+)\}/defined $ENV{$1} ? $ENV{$1} : $&/eg' \
    "${RANGER_SETUP_DIR}/impala_user_owner.json.template" > \
    "${RANGER_SETUP_DIR}/impala_user_owner.json"

  perl -wpl -e 's/\$\{([^}]+)\}/defined $ENV{$1} ? $ENV{$1} : $&/eg' \
    "${RANGER_SETUP_DIR}/impala_user_non_owner.json.template" > \
    "${RANGER_SETUP_DIR}/impala_user_non_owner.json"

  perl -wpl -e 's/\$\{([^}]+)\}/defined $ENV{$1} ? $ENV{$1} : $&/eg' \
    "${RANGER_SETUP_DIR}/impala_user_non_owner_2.json.template" > \
    "${RANGER_SETUP_DIR}/impala_user_non_owner_2.json"

  if grep "\${[A-Z_]*}" "${RANGER_SETUP_DIR}/impala_user_owner.json"; then
    echo "Found undefined variables in ${RANGER_SETUP_DIR}/impala_user_owner.json."
    exit 1
  fi

  if grep "\${[A-Z_]*}" "${RANGER_SETUP_DIR}/impala_user_non_owner.json"; then
    echo "Found undefined variables in ${RANGER_SETUP_DIR}/impala_user_non_owner.json."
    exit 1
  fi

  if grep "\${[A-Z_]*}" "${RANGER_SETUP_DIR}/impala_user_non_owner_2.json"; then
    echo "Found undefined variables in ${RANGER_SETUP_DIR}/impala_user_non_owner_2.json."
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
    --post-file="${RANGER_SETUP_DIR}/impala_user_non_owner_2.json" \
    --header="Content-Type:application/json" \
    http://localhost:6080/service/xusers/secure/users

  wget -O /dev/null --auth-no-challenge --user=admin --password=admin \
    --post-file="${RANGER_SETUP_DIR}/impala_service.json" \
    --header="Content-Type:application/json" \
    http://localhost:6080/service/public/v2/api/service

  # The policy id corresponding to all the databases and tables is 4 in Apache Ranger,
  # whereas it is 5 in CDP Ranger. Getting the policy id via the following API call
  # makes this script more resilient to the change in the policy id.
  ALL_DATABASE_POLICY_ID=$(curl -u admin:admin -X GET \
    http://localhost:6080/service/public/v2/api/service/test_impala/policy/\
all%20-%20database \
    -H 'accept: application/json' | \
  python -c "import sys, json; print(json.load(sys.stdin)['id'])")
  export ALL_DATABASE_POLICY_ID

  perl -wpl -e 's/\$\{([^}]+)\}/defined $ENV{$1} ? $ENV{$1} : $&/eg' \
    "${RANGER_SETUP_DIR}/all_database_policy_revised.json.template" > \
    "${RANGER_SETUP_DIR}/all_database_policy_revised.json"

  if grep "\${[A-Z_]*}" "${RANGER_SETUP_DIR}/all_database_policy_revised.json"; then
    echo "Found undefined variables in \
    ${RANGER_SETUP_DIR}/all_database_policy_revised.json."
    exit 1
  fi

  curl -f -u admin:admin -H "Accept: application/json" \
    -H "Content-Type: application/json" \
    -X PUT http://localhost:6080/service/public/v2/api/policy/${ALL_DATABASE_POLICY_ID} \
    -d @"${RANGER_SETUP_DIR}/all_database_policy_revised.json"
}

setup-ranger
