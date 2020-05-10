#!/bin/bash
################################################################################
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
################################################################################
#
# This script follows the pattern described in the docker best practices here:
# https://docs.docker.com/develop/develop-images/dockerfile_best-practices/#entrypoint
################################################################################

set -euo pipefail
set -x

if [[ $# -eq 0 ]]; then
  echo "Must provide at least one argument."
  exit 1
elif [[ "$1" = "load_tpcds" ]]; then
  echo "Loading TPC-DS data"
  IMPALA_TOOLCHAIN_BASE=https://native-toolchain.s3.amazonaws.com/build/7-f2ddef91e9/
  TPCDS_VERSION=2.1.0
  TPCDS_TARBALL=tpc-ds-${TPCDS_VERSION}-gcc-4.9.2-ec2-package-ubuntu-18-04.tar.gz
  TPCDS_URL=${IMPALA_TOOLCHAIN_BASE}tpc-ds/${TPCDS_VERSION}-gcc-4.9.2/${TPCDS_TARBALL}

  curl ${TPCDS_URL} --output tpcds.tar.gz
  tar xzf tpcds.tar.gz

  # The base directory for Hive external tables, in a mounted volume.
  WAREHOUSE_EXTERNAL_DIR=/user/hive/warehouse/external
  TPCDS_RAW_DIR=${WAREHOUSE_EXTERNAL_DIR}/tpcds_raw

  # Use a marker file to avoid regenerating the data if already present in
  # the warehouse. dsdgen is a serial process and somewhat slow.
  if ! stat ${TPCDS_RAW_DIR}/generated; then
    SCALE_FACTOR=1
    # Generate the data. This creates one .dat file for each table.
    ./tpc-ds-${TPCDS_VERSION}/bin/dsdgen -force -verbose -scale ${SCALE_FACTOR}

    # Move the tables into the warehouse, one per subdirectory
    for FILE in *.dat; do
      FILE_DIR=${TPCDS_RAW_DIR}/${FILE%.dat}
      rm -rf "${FILE_DIR}"
      mkdir -p "${FILE_DIR}"
      mv "${FILE}" "${FILE_DIR}"
    done
    touch ${TPCDS_RAW_DIR}/generated
  fi

  IMPALA_SHELL="impala-shell --protocol=hs2 -i docker_impalad-1_1"

  # Wait until Impala comes up (it started in parallel with the data loader).
  for i in $(seq 300); do
    if ${IMPALA_SHELL} -q 'select version()'; then
      break
    fi
    echo "Waiting for impala to come up"
    sleep 0.5
  done

  ${IMPALA_SHELL} -f /opt/impala/sql/load_tpcds_parquet.sql
  # Load data into Kudu if the Kudu master is up.
  if ping -c1 kudu-master-1; then
    ${IMPALA_SHELL} -f /opt/impala/sql/load_tpcds_kudu.sql
  fi
elif [[ "$1" = "impala-shell" ]]; then
  shift
  # Execute impala-shell with any extra arguments provided.
  exec impala-shell --protocol=hs2 --history_file=/tmp/impalahistory \
       -i docker_impalad-1_1 "$@"
else
  # Execute the provided input as a command
  exec "$@"
fi
