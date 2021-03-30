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
# This script can be executed in two ways:
# 1) Without any command line parameters - A normal data load will happen where data is
# generated as needed, generally by issuing 'INSERT INTO <table> SELECT *' commands.
# 2) With a command line parameter pointing to a test-warehouse snapshot file - In this
# case the snapshot file contents will be copied into HDFS prior to calling the data load
# scripts. This speeds up overall data loading time because it usually means only the
# table metadata needs to be created.
#
# For more information look at testdata/bin/load-test-warehouse-snapshot.sh and
# bin/load-data.py

set -euo pipefail
. $IMPALA_HOME/bin/report_build_error.sh
setup_report_build_error

. ${IMPALA_HOME}/bin/impala-config.sh > /dev/null 2>&1
. ${IMPALA_HOME}/testdata/bin/run-step.sh

# Environment variables used to direct the data loading process to an external cluster.
# TODO: We need a better way of managing how these get set. See IMPALA-4346
: ${HS2_HOST_PORT=localhost:11050}
: ${HDFS_NN=${INTERNAL_LISTEN_HOST}:20500}
: ${IMPALAD=localhost}
: ${REMOTE_LOAD=}
: ${CM_HOST=}
: ${IMPALA_SERIAL_DATALOAD=}
# We don't expect dataload to take more than 2.5 hours.
: ${TIMEOUT_FOR_CREATE_LOAD_DATA_MINS:= 150}

SKIP_METADATA_LOAD=0
SKIP_SNAPSHOT_LOAD=0
SKIP_RANGER=0
SNAPSHOT_FILE=""
LOAD_DATA_ARGS=""
EXPLORATION_STRATEGY="exhaustive"
export JDBC_URL="jdbc:hive2://${HS2_HOST_PORT}/default;"
HIVE_CMD="beeline -n $USER -u $JDBC_URL"

# For logging when using run-step.
LOG_DIR=${IMPALA_DATA_LOADING_LOGS_DIR}

echo "Executing: create-load-data.sh $@"

while [ -n "$*" ]
do
  case $1 in
    -exploration_strategy)
      EXPLORATION_STRATEGY=${2-}
      if [[ -z "$EXPLORATION_STRATEGY" ]]; then
        echo "Must provide an exploration strategy from e.g. core, exhaustive"
        exit 1;
      fi
      shift;
      ;;
    -skip_metadata_load)
      SKIP_METADATA_LOAD=1
      ;;
    -skip_snapshot_load)
      SKIP_SNAPSHOT_LOAD=1
      ;;
    -snapshot_file)
      SNAPSHOT_FILE=${2-}
      if [ ! -f $SNAPSHOT_FILE ]; then
        echo "-snapshot_file does not exist: $SNAPSHOT_FILE"
        exit 1;
      fi
      shift;
      ;;
    -cm_host)
      CM_HOST=${2-}
      shift;
      ;;
    -skip_ranger)
      SKIP_RANGER=1
      ;;
    -timeout)
      TIMEOUT_FOR_CREATE_LOAD_DATA_MINS=${2-}
      shift;
      ;;
    -help|-h|*)
      echo "create-load-data.sh : Creates data and loads from scratch"
      echo "[-skip_metadata_load] : Skips loading of metadata"
      echo "[-skip_snapshot_load] : Assumes that the snapshot is already loaded"
      echo "[-snapshot_file] : Loads the test warehouse snapshot into hdfs"
      echo "[-cm_host] : Address of the Cloudera Manager host if loading to a remote cluster"
      echo "[-skip_ranger] : Skip the set-up for Ranger."
      echo "[-timeout] : The timeout in minutes for loading data."
      exit 1;
      ;;
    esac
  shift;
done

if [[ -n $REMOTE_LOAD ]]; then
  SKIP_RANGER=1
fi

"${IMPALA_HOME}/bin/script-timeout-check.sh" -timeout $TIMEOUT_FOR_CREATE_LOAD_DATA_MINS \
    -script_name "$(basename $0)" &
TIMEOUT_PID=$!

if [[ $SKIP_METADATA_LOAD -eq 0  && "$SNAPSHOT_FILE" = "" ]]; then
  run-step "Generating HBase data" create-hbase.log \
      ${IMPALA_HOME}/testdata/bin/create-hbase.sh
  run-step "Creating /test-warehouse HDFS directory" create-test-warehouse-dir.log \
      hadoop fs -mkdir /test-warehouse
elif [ $SKIP_SNAPSHOT_LOAD -eq 0 ]; then
  run-step "Loading HDFS data from snapshot: $SNAPSHOT_FILE" \
      load-test-warehouse-snapshot.log \
      ${IMPALA_HOME}/testdata/bin/load-test-warehouse-snapshot.sh "$SNAPSHOT_FILE"
  # Don't skip the metadata load if a schema change is detected.
  if ! ${IMPALA_HOME}/testdata/bin/check-schema-diff.sh; then
    if [[ "${TARGET_FILESYSTEM}" == "isilon" || "${TARGET_FILESYSTEM}" == "s3" || \
          "${TARGET_FILESYSTEM}" == "local" || "${TARGET_FILESYSTEM}" == "gs" ]] ; then
      echo "ERROR in $0 at line $LINENO: A schema change has been detected in the"
      echo "metadata, but it cannot be loaded on isilon, s3, gcs or local and the"
      echo "target file system is ${TARGET_FILESYSTEM}.  Exiting."
      exit 1
    fi
    echo "Schema change detected, metadata will be loaded."
    SKIP_METADATA_LOAD=0
  fi
else
  # hdfs data already exists, don't load it.
  echo Skipping loading data to hdfs.
fi

echo "Derived params for create-load-data.sh:"
echo "EXPLORATION_STRATEGY=${EXPLORATION_STRATEGY:-}"
echo "SKIP_METADATA_LOAD=${SKIP_METADATA_LOAD:-}"
echo "SKIP_SNAPSHOT_LOAD=${SKIP_SNAPSHOT_LOAD:-}"
echo "SNAPSHOT_FILE=${SNAPSHOT_FILE:-}"
echo "CM_HOST=${CM_HOST:-}"
echo "REMOTE_LOAD=${REMOTE_LOAD:-}"

function start-impala {
  : ${START_CLUSTER_ARGS=""}
  # Use a fast statestore update so that DDL operations run faster.
  START_CLUSTER_ARGS_INT="--state_store_args=--statestore_update_frequency_ms=50"
  if [[ "${TARGET_FILESYSTEM}" == "local" ]]; then
    START_CLUSTER_ARGS_INT+=("--impalad_args=--abort_on_config_error=false -s 1")
  else
    START_CLUSTER_ARGS_INT+=("-s 3")
  fi
  if [[ "${ERASURE_CODING}" == true ]]; then
    START_CLUSTER_ARGS="${START_CLUSTER_ARGS} \
      --impalad_args=--default_query_options=allow_erasure_coded_files=true"
  fi
  START_CLUSTER_ARGS_INT+=("${START_CLUSTER_ARGS}")
  ${IMPALA_HOME}/bin/start-impala-cluster.py --log_dir=${IMPALA_DATA_LOADING_LOGS_DIR} \
    ${START_CLUSTER_ARGS_INT[@]}
}

function restart-cluster {
  # Break out each individual step for clarity
  echo "Shutting down Impala"
  ${IMPALA_HOME}/bin/start-impala-cluster.py --kill
  echo "Shutting down the minicluster"
  ${IMPALA_HOME}/testdata/bin/kill-all.sh
  echo "Starting the minicluster"
  ${IMPALA_HOME}/testdata/bin/run-all.sh
  echo "Starting Impala"
  start-impala
}

function load-custom-schemas {
  # HDFS commandline calls are slow, so consolidate the manipulation into
  # as few calls as possible by populating a temporary directory with the
  # appropriate structure and copying it in a single call.
  TMP_DIR=$(mktemp -d)

  # Cleanup old schemas dir
  hadoop fs -rm -r -f /test-warehouse/schemas
  SCHEMA_SRC_DIR=${IMPALA_HOME}/testdata/data/schemas
  SCHEMA_TMP_DIR="${TMP_DIR}/schemas"
  mkdir ${SCHEMA_TMP_DIR}
  mkdir ${SCHEMA_TMP_DIR}/enum
  ln -s ${SCHEMA_SRC_DIR}/zipcode_incomes.parquet ${SCHEMA_TMP_DIR}
  ln -s ${SCHEMA_SRC_DIR}/alltypestiny.parquet ${SCHEMA_TMP_DIR}
  ln -s ${SCHEMA_SRC_DIR}/enum/* ${SCHEMA_TMP_DIR}/enum
  ln -s ${SCHEMA_SRC_DIR}/malformed_decimal_tiny.parquet ${SCHEMA_TMP_DIR}
  ln -s ${SCHEMA_SRC_DIR}/decimal.parquet ${SCHEMA_TMP_DIR}
  ln -s ${SCHEMA_SRC_DIR}/nested/modern_nested.parquet ${SCHEMA_TMP_DIR}
  ln -s ${SCHEMA_SRC_DIR}/nested/legacy_nested.parquet ${SCHEMA_TMP_DIR}

  # CHAR and VARCHAR tables written by Hive
  mkdir -p ${TMP_DIR}/chars_formats_avro_snap \
   ${TMP_DIR}/chars_formats_parquet \
   ${TMP_DIR}/chars_formats_text \
   ${TMP_DIR}/chars_formats_orc_def

  ln -s ${IMPALA_HOME}/testdata/data/chars-formats.avro ${TMP_DIR}/chars_formats_avro_snap
  ln -s ${IMPALA_HOME}/testdata/data/chars-formats.parquet ${TMP_DIR}/chars_formats_parquet
  ln -s ${IMPALA_HOME}/testdata/data/chars-formats.orc ${TMP_DIR}/chars_formats_orc_def
  ln -s ${IMPALA_HOME}/testdata/data/chars-formats.txt ${TMP_DIR}/chars_formats_text

  # File used by CreateTableLikeOrc tests
  ln -s ${IMPALA_HOME}/testdata/data/alltypes_non_acid.orc ${SCHEMA_TMP_DIR}

  hadoop fs -put -f ${TMP_DIR}/* /test-warehouse

  rm -r ${TMP_DIR}
}

function load-data {
  WORKLOAD=${1}
  EXPLORATION_STRATEGY=${2:-"core"}
  TABLE_FORMATS=${3:-}
  FORCE_LOAD=${4:-}

  MSG="Loading workload '$WORKLOAD'"
  ARGS=("--workloads $WORKLOAD")
  MSG+=" using exploration strategy '$EXPLORATION_STRATEGY'"
  ARGS+=("-e $EXPLORATION_STRATEGY")
  if [ $TABLE_FORMATS ]; then
    MSG+=" in table formats '$TABLE_FORMATS'"
    ARGS+=("--table_formats $TABLE_FORMATS")
  fi
  if [ $LOAD_DATA_ARGS ]; then
    ARGS+=("$LOAD_DATA_ARGS")
  fi
  # functional-query is unique. The dataset name is not the same as the workload name.
  if [ "${WORKLOAD}" = "functional-query" ]; then
    WORKLOAD="functional"
  fi

  # TODO: Why is there a REMOTE_LOAD condition? See IMPALA-4347
  #
  # Force load the dataset if we detect a schema change.
  if [[ -z "$REMOTE_LOAD" ]]; then
    if ! ${IMPALA_HOME}/testdata/bin/check-schema-diff.sh $WORKLOAD; then
      ARGS+=("--force")
      echo "Force loading $WORKLOAD because a schema change was detected"
    elif [ "${FORCE_LOAD}" = "force" ]; then
      ARGS+=("--force")
      echo "Force loading."
    fi
  fi

  ARGS+=("--impalad ${IMPALAD}")
  ARGS+=("--hive_hs2_hostport ${HS2_HOST_PORT}")
  ARGS+=("--hdfs_namenode ${HDFS_NN}")

  # Disable parallelism for dataload if IMPALA_SERIAL_DATALOAD is set
  if [[ "${IMPALA_SERIAL_DATALOAD}" -eq 1 ]]; then
    ARGS+=("--num_processes 1")
  fi

  if [[ -n ${TABLE_FORMATS} ]]; then
    # TBL_FMT_STR replaces slashes with underscores,
    # e.g., kudu/none/none -> kudu_none_none
    TBL_FMT_STR=${TABLE_FORMATS//[\/]/_}
    LOG_BASENAME=data-load-${WORKLOAD}-${EXPLORATION_STRATEGY}-${TBL_FMT_STR}.log
  else
    LOG_BASENAME=data-load-${WORKLOAD}-${EXPLORATION_STRATEGY}.log
  fi

  LOG_FILE=${IMPALA_DATA_LOADING_LOGS_DIR}/${LOG_BASENAME}
  echo "$MSG. Logging to ${LOG_FILE}"
  # Use unbuffered logging by executing with -u
  if ! impala-python -u ${IMPALA_HOME}/bin/load-data.py ${ARGS[@]} &> ${LOG_FILE}; then
    echo Error loading data. The end of the log file is:
    tail -n 50 $LOG_FILE
    return 1
  fi
}

function cache-test-tables {
  echo CACHING  tpch.nation AND functional.alltypestiny
  # uncaching the tables first makes this operation idempotent.
  ${IMPALA_HOME}/bin/impala-shell.sh -i ${IMPALAD}\
    -q "alter table functional.alltypestiny set uncached"
  ${IMPALA_HOME}/bin/impala-shell.sh -i ${IMPALAD}\
    -q "alter table tpch.nation set uncached"
  ${IMPALA_HOME}/bin/impala-shell.sh -i ${IMPALAD}\
    -q "alter table tpch.nation set cached in 'testPool'"
  ${IMPALA_HOME}/bin/impala-shell.sh -i ${IMPALAD} -q\
    "alter table functional.alltypestiny set cached in 'testPool'"
}

function load-aux-workloads {
  LOG_FILE=${IMPALA_DATA_LOADING_LOGS_DIR}/data-load-auxiliary-workloads-core.log
  rm -f $LOG_FILE
  # Load all the auxiliary workloads (if any exist)
  if [ -d ${IMPALA_AUX_WORKLOAD_DIR} ] && [ -d ${IMPALA_AUX_DATASET_DIR} ]; then
    echo Loading auxiliary workloads. Logging to $LOG_FILE.
    if ! impala-python -u ${IMPALA_HOME}/bin/load-data.py --workloads all\
        --impalad=${IMPALAD}\
        --hive_hs2_hostport=${HS2_HOST_PORT}\
        --hdfs_namenode=${HDFS_NN}\
        --workload_dir=${IMPALA_AUX_WORKLOAD_DIR}\
        --dataset_dir=${IMPALA_AUX_DATASET_DIR}\
        --exploration_strategy=core ${LOAD_DATA_ARGS} >> $LOG_FILE 2>&1; then
      echo Error loading aux workloads. The end of the log file is:
      tail -n 20 $LOG_FILE
      return 1
    fi
  else
    echo "Skipping load of auxilary workloads because directories do not exist"
  fi
}

function setup-ranger {
  echo "SETTING UP RANGER"

  RANGER_SETUP_DIR="${IMPALA_HOME}/testdata/cluster/ranger/setup"

  perl -wpl -e 's/\$\{([^}]+)\}/defined $ENV{$1} ? $ENV{$1} : $&/eg' \
    "${RANGER_SETUP_DIR}/impala_group_owner.json.template" > \
    "${RANGER_SETUP_DIR}/impala_group_owner.json"

  export GROUP_ID_OWNER=$(wget -qO - --auth-no-challenge --user=admin --password=admin \
    --post-file="${RANGER_SETUP_DIR}/impala_group_owner.json" \
    --header="accept:application/json" \
    --header="Content-Type:application/json" \
    http://localhost:6080/service/xusers/secure/groups |
    python -c "import sys, json; print json.load(sys.stdin)['id']")

  export GROUP_ID_NON_OWNER=$(wget -qO - --auth-no-challenge --user=admin \
    --password=admin --post-file="${RANGER_SETUP_DIR}/impala_group_non_owner.json" \
    --header="accept:application/json" \
    --header="Content-Type:application/json" \
    http://localhost:6080/service/xusers/secure/groups |
    python -c "import sys, json; print json.load(sys.stdin)['id']")

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

  curl -u admin:admin -H "Accept: application/json" -H "Content-Type: application/json" \
    -X PUT http://localhost:6080/service/public/v2/api/policy/4 \
    -d @"${RANGER_SETUP_DIR}/policy_4_revised.json"
}

function copy-and-load-dependent-tables {
  # COPY
  # TODO: The multi-format table will move these files. So we need to copy them to a
  # temporary location for that table to use. Should find a better way to handle this.
  echo COPYING AND LOADING DATA FOR DEPENDENT TABLES
  hadoop fs -rm -r -f /test-warehouse/alltypesmixedformat \
    /tmp/alltypes_rc /tmp/alltypes_seq /tmp/alltypes_parquet
  hadoop fs -mkdir -p /tmp/alltypes_seq/year=2009 \
    /tmp/alltypes_rc/year=2009 /tmp/alltypes_parquet/year=2009

  # The file written by hive to /test-warehouse will be strangely replicated rather than
  # erasure coded if EC is not set in /tmp
  if [[ -n "${HDFS_ERASURECODE_POLICY:-}" ]]; then
    hdfs ec -setPolicy -policy "${HDFS_ERASURECODE_POLICY}" -path "/tmp/alltypes_rc"
    hdfs ec -setPolicy -policy "${HDFS_ERASURECODE_POLICY}" -path "/tmp/alltypes_seq"
    hdfs ec -setPolicy -policy "${HDFS_ERASURECODE_POLICY}" -path "/tmp/alltypes_parquet"
  fi

  hadoop fs -cp /test-warehouse/alltypes_seq/year=2009/month=2/ /tmp/alltypes_seq/year=2009
  hadoop fs -cp /test-warehouse/alltypes_rc/year=2009/month=3/ /tmp/alltypes_rc/year=2009
  hadoop fs -cp /test-warehouse/alltypes_parquet/year=2009/month=4/ /tmp/alltypes_parquet/year=2009

  # Create a hidden file in AllTypesSmall
  hadoop fs -cp -f /test-warehouse/zipcode_incomes/DEC_00_SF3_P077_with_ann_noheader.csv \
   /test-warehouse/alltypessmall/year=2009/month=1/_hidden
  hadoop fs -cp -f /test-warehouse/zipcode_incomes/DEC_00_SF3_P077_with_ann_noheader.csv \
   /test-warehouse/alltypessmall/year=2009/month=1/.hidden

  # In case the data is updated by a non-super user, make sure the user can write
  # by chmoding 777 /tmp/alltypes_rc and /tmp/alltypes_seq. This is needed in order
  # to prevent this error during data load to a remote cluster:
  #
  #   ERROR : Failed with exception Unable to move source hdfs://cluster-1.foo.cloudera.com:
  #   8020/tmp/alltypes_seq/year=2009/month=2/000023_0 to destination hdfs://cluster-1.foo.
  #   cloudera.com:8020/test-warehouse/alltypesmixedformat/year=2009/month=2/000023_0
  #   [...]
  #   Caused by: org.apache.hadoop.security.AccessControlException:
  #   Permission denied: user=impala, access=WRITE
  #   inode="/tmp/alltypes_seq/year=2009/month=2":hdfs:supergroup:drwxr-xr-x
  #
  # The error occurs while loading dependent tables.
  #
  # See: logs/data_loading/copy-and-load-dependent-tables.log)
  # See also: IMPALA-4345
  hadoop fs -chmod -R 777 /tmp/alltypes_rc /tmp/alltypes_seq /tmp/alltypes_parquet

  # For tables that rely on loading data from local fs test-wareload-house
  # TODO: Find a good way to integrate this with the normal data loading scripts
  beeline -n $USER -u "${JDBC_URL}" -f\
    ${IMPALA_HOME}/testdata/bin/load-dependent-tables.sql
}

function create-internal-hbase-table {
  # TODO: For some reason DROP TABLE IF EXISTS sometimes fails on HBase if the table does
  # not exist. To work around this, disable exit on error before executing this command.
  # Need to investigate this more, but this works around the problem to unblock automation.
  set +o errexit
  beeline -n $USER -u "${JDBC_URL}" -e\
    "DROP TABLE IF EXISTS functional_hbase.internal_hbase_table;"
  echo "disable 'functional_hbase.internal_hbase_table'" | hbase shell
  echo "drop 'functional_hbase.internal_hbase_table'" | hbase shell
  set -e
  # Used by CatalogTest to confirm that non-external HBase tables are identified
  # correctly (IMP-581)
  # Note that the usual 'hbase.table.name' property is not specified to avoid
  # creating tables in HBase as a side-effect.
  cat > /tmp/create-hbase-internal.sql << EOF
    CREATE TABLE functional_hbase.internal_hbase_table(key int, value string)
    STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
    WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key,cf1:val");
EOF
  beeline -n $USER -u "${JDBC_URL}" -f /tmp/create-hbase-internal.sql
  rm -f /tmp/create-hbase-internal.sql
}

function load-custom-data {
  # Add a sequence file that only contains a header (see IMPALA-362)
  hadoop fs -put -f ${IMPALA_HOME}/testdata/tinytable_seq_snap/tinytable_seq_snap_header_only \
                    /test-warehouse/tinytable_seq_snap

  # IMPALA-1619: payload compressed with snappy used for constructing large snappy block
  # compressed file
  hadoop fs -put -f ${IMPALA_HOME}/testdata/compressed_formats/compressed_payload.snap \
                    /test-warehouse/compressed_payload.snap

  # Create Avro tables
  beeline -n $USER -u "${JDBC_URL}" -f\
    ${IMPALA_HOME}/testdata/avro_schema_resolution/create_table.sql

  # Delete potentially existing avro data
  hadoop fs -rm -f /test-warehouse/avro_schema_resolution_test/*.avro

  # Upload Avro data to the 'schema_resolution_test' table
  hadoop fs -put ${IMPALA_HOME}/testdata/avro_schema_resolution/records*.avro \
    /test-warehouse/avro_schema_resolution_test
}

function build-and-copy-hive-udfs {
  # Build the test Hive UDFs
  pushd ${IMPALA_HOME}/java/test-hive-udfs
  ${IMPALA_HOME}/bin/mvn-quiet.sh clean
  ${IMPALA_HOME}/bin/mvn-quiet.sh package
  popd
  # Copy the test UDF/UDA libraries into HDFS
  ${IMPALA_HOME}/testdata/bin/copy-udfs-udas.sh -build
}

# Additional data loading actions that must be executed after the main data is loaded.
function custom-post-load-steps {
  # TODO: Why is there a REMOTE_LOAD condition? See IMPALA-4347
  if [[ -z "$REMOTE_LOAD" ]]; then
    # Configure alltypes_seq as a read-only table. This is required for fe tests.
    # Set both read and execute permissions because accessing the contents of a directory on
    # the local filesystem requires the x permission (while on HDFS it requires the r
    # permission).
    hadoop fs -chmod -R 555 \
      ${FILESYSTEM_PREFIX}/test-warehouse/alltypes_seq/year=2009/month=1 \
      ${FILESYSTEM_PREFIX}/test-warehouse/alltypes_seq/year=2009/month=3
  fi

  hadoop fs -mkdir -p ${FILESYSTEM_PREFIX}/test-warehouse/lineitem_multiblock_parquet \
    ${FILESYSTEM_PREFIX}/test-warehouse/lineitem_sixblocks_parquet \
    ${FILESYSTEM_PREFIX}/test-warehouse/lineitem_multiblock_one_row_group_parquet

  #IMPALA-1881: data file produced by hive with multiple blocks.
  hadoop fs -Ddfs.block.size=1048576 -put -f \
    ${IMPALA_HOME}/testdata/LineItemMultiBlock/000000_0 \
    ${FILESYSTEM_PREFIX}/test-warehouse/lineitem_multiblock_parquet

  # IMPALA-2466: Add more tests to the HDFS Parquet scanner (Added after IMPALA-1881)
  hadoop fs -Ddfs.block.size=1048576 -put -f \
    ${IMPALA_HOME}/testdata/LineItemMultiBlock/lineitem_sixblocks.parquet \
    ${FILESYSTEM_PREFIX}/test-warehouse/lineitem_sixblocks_parquet

  # IMPALA-2466: Add more tests to the HDFS Parquet scanner (this has only one row group)
  hadoop fs -Ddfs.block.size=1048576 -put -f \
    ${IMPALA_HOME}/testdata/LineItemMultiBlock/lineitem_one_row_group.parquet \
    ${FILESYSTEM_PREFIX}/test-warehouse/lineitem_multiblock_one_row_group_parquet

  # IMPALA-3307: Upload test time-zone database
  hadoop fs -Ddfs.block.size=1048576 -put -f ${IMPALA_HOME}/testdata/tzdb \
    ${FILESYSTEM_PREFIX}/test-warehouse/
}

function copy-and-load-ext-data-source {
  # Copy the test data source library into HDFS
  ${IMPALA_HOME}/testdata/bin/copy-data-sources.sh
  # Create data sources table.
  ${IMPALA_HOME}/bin/impala-shell.sh -i ${IMPALAD} -f\
    ${IMPALA_HOME}/testdata/bin/create-data-source-table.sql
}

function check-hdfs-health {
  if [[ -n "${HDFS_ERASURECODE_POLICY:-}" ]]; then
    if ! grep "Replicated Blocks:[[:space:]]*#[[:space:]]*Total size:[[:space:]]*0 B"\
        <<< $(hdfs fsck /test-warehouse | tr '\n' '#'); then
        echo "There are some replicated files despite that erasure coding is on"
        echo "Failing the data loading job"
        exit 1
    fi
    return
  fi
  MAX_FSCK=30
  SLEEP_SEC=120
  LAST_NUMBER_UNDER_REPLICATED=-1
  for ((FSCK_COUNT = 0; FSCK_COUNT <= MAX_FSCK; FSCK_COUNT++)); do
    FSCK_OUTPUT="$(hdfs fsck /test-warehouse)"
    echo "$FSCK_OUTPUT"
    NUMBER_UNDER_REPLICATED=$(
        grep -oP "Under-replicated blocks:[[:space:]]*\K[[:digit:]]*" <<< "$FSCK_OUTPUT")
    if [[ "$NUMBER_UNDER_REPLICATED" -eq 0 ]] ; then
      # All the blocks are fully-replicated. The data loading can continue.
      return
    fi
    if [[ $(($FSCK_COUNT + 1)) -eq "$MAX_FSCK" ]] ; then
      echo "Some HDFS blocks are still under-replicated after running HDFS fsck"\
          "$MAX_FSCK times."
      echo "Some tests cannot pass without fully-replicated blocks (IMPALA-3887)."
      echo "Failing the data loading."
      exit 1
    fi
    if [[ "$NUMBER_UNDER_REPLICATED" -eq "$LAST_NUMBER_UNDER_REPLICATED" ]] ; then
      echo "There are under-replicated blocks in HDFS and HDFS is not making progress"\
          "in $SLEEP_SEC seconds. Attempting to restart HDFS to resolve this issue."
      # IMPALA-7119: Other minicluster components (like HBase) can fail if HDFS is
      # restarted by itself, so restart the whole cluster, including Impala.
      restart-cluster
    fi
    LAST_NUMBER_UNDER_REPLICATED="$NUMBER_UNDER_REPLICATED"
    echo "$NUMBER_UNDER_REPLICATED under replicated blocks remaining."
    echo "Sleeping for $SLEEP_SEC seconds before rechecking."
    sleep "$SLEEP_SEC"
  done
}

function warm-up-hive {
  echo "Running warm up Hive statements"
  $HIVE_CMD -e "create database if not exists functional;"
  $HIVE_CMD -e "create table if not exists hive_warm_up_tbl (i int);"
  # The insert below starts a Tez session (if Hive uses Tez) and initializes
  # .hiveJars directory in HDFS, see IMPALA-8841.
  $HIVE_CMD -e "insert overwrite table hive_warm_up_tbl values (1);"
}

# For kerberized clusters, use kerberos
if ${CLUSTER_DIR}/admin is_kerberized; then
  LOAD_DATA_ARGS="${LOAD_DATA_ARGS} --use_kerberos --principal=${MINIKDC_PRINC_HIVE}"
fi

# Start Impala
if [[ -z "$REMOTE_LOAD" ]]; then
  run-step "Starting Impala cluster" start-impala-cluster.log start-impala
fi

# The hdfs environment script sets up kms (encryption) and cache pools (hdfs caching).
# On a non-hdfs filesystem, we don't test encryption or hdfs caching, so this setup is not
# needed.
if [[ "${TARGET_FILESYSTEM}" == "hdfs" ]]; then
  run-step "Setting up HDFS environment" setup-hdfs-env.log \
      ${IMPALA_HOME}/testdata/bin/setup-hdfs-env.sh
fi

if [ $SKIP_METADATA_LOAD -eq 0 ]; then
  # Using Hive in non-parallel mode before starting parallel execution may help with some
  # flakiness during data load, see IMPALA-8841. The problem only occurs in Hive 3
  # environment, but always doing the warm up shouldn't hurt much and may make it easier
  # to investigate future issues where Hive doesn't work at all.
  warm-up-hive
  run-step "Loading custom schemas" load-custom-schemas.log load-custom-schemas
  # Run some steps in parallel, with run-step-backgroundable / run-step-wait-all.
  # This is effective on steps that take a long time and don't depend on each
  # other. Functional-query takes about ~35 minutes, and TPC-H and TPC-DS can
  # finish while functional-query is running.
  run-step-backgroundable "Loading functional-query data" load-functional-query.log \
      load-data "functional-query" "exhaustive"
  run-step-backgroundable "Loading TPC-H data" load-tpch.log load-data "tpch" "core"
  run-step-backgroundable "Loading TPC-DS data" load-tpcds.log load-data "tpcds" "core"
  run-step-wait-all
  # Load tpch nested data.
  # TODO: Hacky and introduces more complexity into the system, but it is expedient.
  if [[ -n "$CM_HOST" ]]; then
    LOAD_NESTED_ARGS="--cm-host $CM_HOST"
  fi
  run-step "Loading nested parquet data" load-nested.log \
    ${IMPALA_HOME}/testdata/bin/load_nested.py \
    -t tpch_nested_parquet -f parquet/none ${LOAD_NESTED_ARGS:-}
  run-step "Loading nested orc data" load-nested.log \
    ${IMPALA_HOME}/testdata/bin/load_nested.py \
    -t tpch_nested_orc_def -f orc/def ${LOAD_NESTED_ARGS:-}
  run-step "Loading auxiliary workloads" load-aux-workloads.log load-aux-workloads
  run-step "Loading dependent tables" copy-and-load-dependent-tables.log \
      copy-and-load-dependent-tables
  run-step "Loading custom data" load-custom-data.log load-custom-data
  run-step "Creating many block table" create-table-many-blocks.log \
      ${IMPALA_HOME}/testdata/bin/create-table-many-blocks.sh -p 1234 -b 1
elif [ "${TARGET_FILESYSTEM}" = "hdfs" ];  then
  echo "Skipped loading the metadata."
  run-step "Loading HBase data only" load-hbase-only.log \
      load-data "functional-query" "core" "hbase/none"
fi

if [[ $SKIP_METADATA_LOAD -eq 1 ]]; then
  # Tests depend on the kudu data being clean, so load the data from scratch.
  # This is only necessary if this is not a full dataload, because a full dataload
  # already loads Kudu functional and TPC-H tables from scratch.
  run-step-backgroundable "Loading Kudu functional" load-kudu.log \
        load-data "functional-query" "core" "kudu/none/none" force
  run-step-backgroundable "Loading Kudu TPCH" load-kudu-tpch.log \
        load-data "tpch" "core" "kudu/none/none" force
fi
run-step-backgroundable "Loading Hive UDFs" build-and-copy-hive-udfs.log \
    build-and-copy-hive-udfs
run-step-wait-all
run-step "Running custom post-load steps" custom-post-load-steps.log \
    custom-post-load-steps

if [ "${TARGET_FILESYSTEM}" = "hdfs" ]; then
  # Caching tables in s3 returns an IllegalArgumentException, see IMPALA-1714
  run-step "Caching test tables" cache-test-tables.log cache-test-tables

  # TODO: Modify the .sql file that creates the table to take an alternative location into
  # account.
  run-step "Loading external data sources" load-ext-data-source.log \
      copy-and-load-ext-data-source

  run-step "Creating internal HBase table" create-internal-hbase-table.log \
      create-internal-hbase-table

  run-step "Checking HDFS health" check-hdfs-health.log check-hdfs-health

  # Saving the list of created files can help in debugging missing files.
  run-step "Logging created files" created-files.log hdfs dfs -ls -R /test-warehouse
fi

# TODO: Investigate why all stats are not preserved. Theoretically, we only need to
# recompute stats for HBase.
run-step "Computing table stats" compute-table-stats.log \
    ${IMPALA_HOME}/testdata/bin/compute-table-stats.sh

# IMPALA-8346: this step only applies if the cluster is the local minicluster
if [[ -z "$REMOTE_LOAD" ]]; then
  run-step "Creating tpcds testcase data" create-tpcds-testcase-data.log \
      ${IMPALA_HOME}/testdata/bin/create-tpcds-testcase-files.sh
fi

if [[ $SKIP_RANGER -eq 0 ]]; then
  run-step "Setting up Ranger" setup-ranger.log setup-ranger
fi

# Restart the minicluster. This is strictly to provide a sanity check that
# restarting the minicluster works and doesn't impact the tests. This is a common
# operation for developers, so it is nice to test it.
restart-cluster

# Kill the spawned timeout process and its child sleep process.
# There may not be a sleep process, so ignore failure.
pkill -P $TIMEOUT_PID || true
kill $TIMEOUT_PID
