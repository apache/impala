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

# Release cloud resources (useful for Jenkins jobs). Should be called
# when the minicluster is shut down. The minicluster should not be used
# after this called, as this removes the test warehouse data.

set -euo pipefail
trap 'echo Error in $0 at line $LINENO: $(cd "'$PWD'" && awk "NR == $LINENO" $0)' ERR

. ${IMPALA_HOME}/bin/impala-config.sh > /dev/null 2>&1
: ${TEST_WAREHOUSE_DIR=/test-warehouse}

# This is currently only implemented for s3.
# TODO: implement this for other cloud filesystems
# NOTE: Some environment variables referenced here are checked for validity in
# bin/impala-config.sh. Because this is releasing resources, we double check them here
# as well.
if [[ "${TARGET_FILESYSTEM}" == "s3" ]]; then
  # For S3, S3_BUCKET should always be defined.
  [[ -n "${S3_BUCKET}" ]]
  if [[ "${S3GUARD_ENABLED}" == "true" ]]; then
    # If S3GUARD_ENABLED == true, then S3GUARD_DYNAMODB_TABLE and S3GUARD_DYNAMODB_REGION
    # must also be defined. Verify that before proceeding.
    [[ -n "${S3GUARD_DYNAMODB_TABLE}" && -n "${S3GUARD_DYNAMODB_REGION}" ]]
    echo "Cleaning up s3guard and deleting Dynamo DB ${S3GUARD_DYNAMODB_TABLE} ..."
    hadoop s3guard destroy -meta "dynamodb://${S3GUARD_DYNAMODB_TABLE}" \
        -region "${S3GUARD_DYNAMODB_REGION}"
    echo "Done cleaning up s3guard"
  fi
  # Remove the test warehouse
  echo "Removing test warehouse from s3://${S3_BUCKET}${TEST_WAREHOUSE_DIR} ..."
  aws s3 rm --recursive --quiet s3://${S3_BUCKET}${TEST_WAREHOUSE_DIR}
  echo "Done removing test warehouse"
fi
