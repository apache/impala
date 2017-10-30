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
trap 'echo Error in $0 at line $LINENO: $(cd "'$PWD'" && awk "NR == $LINENO" $0)' ERR

. ${IMPALA_HOME}/bin/impala-config.sh > /dev/null 2>&1
. ${IMPALA_HOME}/testdata/bin/run-step.sh

# Environment variables used to direct the data loading process to an external cluster.
# TODO: We need a better way of managing how these get set. See IMPALA-4346
: ${HS2_HOST_PORT=localhost:11050}
: ${HDFS_NN=localhost:20500}
: ${IMPALAD=localhost:21000}
: ${REMOTE_LOAD=}
: ${CM_HOST=}

SKIP_METADATA_LOAD=0
SKIP_SNAPSHOT_LOAD=0
SNAPSHOT_FILE=""
LOAD_DATA_ARGS=""
EXPLORATION_STRATEGY="exhaustive"
export JDBC_URL="jdbc:hive2://${HS2_HOST_PORT}/default;"

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
    -help|-h|*)
      echo "create-load-data.sh : Creates data and loads from scratch"
      echo "[-skip_metadata_load] : Skips loading of metadata"
      echo "[-skip_snapshot_load] : Assumes that the snapshot is already loaded"
      echo "[-snapshot_file] : Loads the test warehouse snapshot into hdfs"
      echo "[-cm_host] : Address of the Cloudera Manager host if loading to a remote cluster"
      exit 1;
      ;;
    esac
  shift;
done

if [[ $SKIP_METADATA_LOAD -eq 0  && "$SNAPSHOT_FILE" = "" ]]; then
  if [[ -z "$REMOTE_LOAD" ]]; then
    run-step "Loading Hive Builtins" load-hive-builtins.log \
      ${IMPALA_HOME}/testdata/bin/load-hive-builtins.sh
  fi
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
          "${TARGET_FILESYSTEM}" == "local" ]] ; then
      echo "ERROR in $0 at line $LINENO: A schema change has been detected in the"
      echo "metadata, but it cannot be loaded on isilon, s3 or local and the"
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

function load-custom-schemas {
  SCHEMA_SRC_DIR=${IMPALA_HOME}/testdata/data/schemas
  SCHEMA_DEST_DIR=/test-warehouse/schemas
  # clean the old schemas directory.
  hadoop fs -rm -r -f ${SCHEMA_DEST_DIR}
  hadoop fs -mkdir ${SCHEMA_DEST_DIR}
  hadoop fs -put $SCHEMA_SRC_DIR/zipcode_incomes.parquet ${SCHEMA_DEST_DIR}/
  hadoop fs -put $SCHEMA_SRC_DIR/alltypestiny.parquet ${SCHEMA_DEST_DIR}/
  hadoop fs -put $SCHEMA_SRC_DIR/enum ${SCHEMA_DEST_DIR}/
  hadoop fs -put $SCHEMA_SRC_DIR/malformed_decimal_tiny.parquet ${SCHEMA_DEST_DIR}/
  hadoop fs -put $SCHEMA_SRC_DIR/decimal.parquet ${SCHEMA_DEST_DIR}/
  hadoop fs -put $SCHEMA_SRC_DIR/nested/modern_nested.parquet ${SCHEMA_DEST_DIR}/
  hadoop fs -put $SCHEMA_SRC_DIR/nested/legacy_nested.parquet ${SCHEMA_DEST_DIR}/

  # CHAR and VARCHAR tables written by Hive
  hadoop fs -mkdir -p /test-warehouse/chars_formats_avro_snap/
  hadoop fs -put -f ${IMPALA_HOME}/testdata/data/chars-formats.avro \
    /test-warehouse/chars_formats_avro_snap
  hadoop fs -mkdir -p /test-warehouse/chars_formats_parquet/
  hadoop fs -put -f ${IMPALA_HOME}/testdata/data/chars-formats.parquet \
    /test-warehouse/chars_formats_parquet
  hadoop fs -mkdir -p /test-warehouse/chars_formats_text/
  hadoop fs -put -f ${IMPALA_HOME}/testdata/data/chars-formats.txt \
    /test-warehouse/chars_formats_text
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

function copy-auth-policy {
  echo COPYING AUTHORIZATION POLICY FILE
  hadoop fs -rm -f ${FILESYSTEM_PREFIX}/test-warehouse/authz-policy.ini
  hadoop fs -put ${IMPALA_HOME}/fe/src/test/resources/authz-policy.ini \
      ${FILESYSTEM_PREFIX}/test-warehouse/
}

function copy-and-load-dependent-tables {
  # COPY
  # TODO: The multi-format table will move these files. So we need to copy them to a
  # temporary location for that table to use. Should find a better way to handle this.
  echo COPYING AND LOADING DATA FOR DEPENDENT TABLES
  hadoop fs -rm -r -f /test-warehouse/alltypesmixedformat
  hadoop fs -rm -r -f /tmp/alltypes_rc
  hadoop fs -rm -r -f /tmp/alltypes_seq
  hadoop fs -mkdir -p /tmp/alltypes_seq/year=2009
  hadoop fs -mkdir -p /tmp/alltypes_rc/year=2009
  hadoop fs -cp /test-warehouse/alltypes_seq/year=2009/month=2/ /tmp/alltypes_seq/year=2009
  hadoop fs -cp /test-warehouse/alltypes_rc/year=2009/month=3/ /tmp/alltypes_rc/year=2009

  # Create a hidden file in AllTypesSmall
  hadoop fs -rm -f /test-warehouse/alltypessmall/year=2009/month=1/_hidden
  hadoop fs -rm -f /test-warehouse/alltypessmall/year=2009/month=1/.hidden
  hadoop fs -cp /test-warehouse/zipcode_incomes/DEC_00_SF3_P077_with_ann_noheader.csv \
   /test-warehouse/alltypessmall/year=2009/month=1/_hidden
  hadoop fs -cp /test-warehouse/zipcode_incomes/DEC_00_SF3_P077_with_ann_noheader.csv \
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
  hadoop fs -chmod -R 777 /tmp/alltypes_rc
  hadoop fs -chmod -R 777 /tmp/alltypes_seq

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
  # Load the index files for corrupted lzo data.
  hadoop fs -mkdir -p /test-warehouse/bad_text_lzo_text_lzo
  hadoop fs -rm -f /test-warehouse/bad_text_lzo_text_lzo/bad_text.lzo.index
  hadoop fs -put ${IMPALA_HOME}/testdata/bad_text_lzo/bad_text.lzo.index \
      /test-warehouse/bad_text_lzo_text_lzo/

  hadoop fs -rm -r -f /bad_text_lzo_text_lzo/
  hadoop fs -mv /test-warehouse/bad_text_lzo_text_lzo/ /
  # Cleanup the old bad_text_lzo files, if they exist.
  hadoop fs -rm -r -f /test-warehouse/bad_text_lzo/

  # TODO: Why is there a REMOTE_LOAD condition? See IMPALA-4347
  if [[ -z $REMOTE_LOAD ]]; then
    # Index all lzo files in HDFS under /test-warehouse
    ${IMPALA_HOME}/testdata/bin/lzo_indexer.sh /test-warehouse
  fi

  hadoop fs -mv /bad_text_lzo_text_lzo/ /test-warehouse/

  # Remove all index files in this partition.
  hadoop fs -rm -f /test-warehouse/alltypes_text_lzo/year=2009/month=1/*.lzo.index

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
  pushd ${IMPALA_HOME}/tests/test-hive-udfs
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
    hadoop fs -chmod -R 555 ${FILESYSTEM_PREFIX}/test-warehouse/alltypes_seq/year=2009/month=1
    hadoop fs -chmod -R 555 ${FILESYSTEM_PREFIX}/test-warehouse/alltypes_seq/year=2009/month=3
  fi

  #IMPALA-1881: data file produced by hive with multiple blocks.
  hadoop fs -mkdir -p ${FILESYSTEM_PREFIX}/test-warehouse/lineitem_multiblock_parquet
  hadoop fs -Ddfs.block.size=1048576 -put -f \
    ${IMPALA_HOME}/testdata/LineItemMultiBlock/000000_0 \
    ${FILESYSTEM_PREFIX}/test-warehouse/lineitem_multiblock_parquet

  # IMPALA-2466: Add more tests to the HDFS Parquet scanner (Added after IMPALA-1881)
  hadoop fs -mkdir -p ${FILESYSTEM_PREFIX}/test-warehouse/lineitem_sixblocks_parquet && \
  hadoop fs -Ddfs.block.size=1048576 -put -f \
    ${IMPALA_HOME}/testdata/LineItemMultiBlock/lineitem_sixblocks.parquet \
    ${FILESYSTEM_PREFIX}/test-warehouse/lineitem_sixblocks_parquet

  # IMPALA-2466: Add more tests to the HDFS Parquet scanner (this has only one row group)
  hadoop fs -mkdir -p ${FILESYSTEM_PREFIX}/test-warehouse/lineitem_multiblock_one_row_group_parquet && \
  hadoop fs -Ddfs.block.size=1048576 -put -f \
    ${IMPALA_HOME}/testdata/LineItemMultiBlock/lineitem_one_row_group.parquet \
    ${FILESYSTEM_PREFIX}/test-warehouse/lineitem_multiblock_one_row_group_parquet
}

function copy-and-load-ext-data-source {
  # Copy the test data source library into HDFS
  ${IMPALA_HOME}/testdata/bin/copy-data-sources.sh
  # Create data sources table.
  ${IMPALA_HOME}/bin/impala-shell.sh -i ${IMPALAD} -f\
    ${IMPALA_HOME}/testdata/bin/create-data-source-table.sql
}

# For kerberized clusters, use kerberos
if ${CLUSTER_DIR}/admin is_kerberized; then
  LOAD_DATA_ARGS="${LOAD_DATA_ARGS} --use_kerberos --principal=${MINIKDC_PRINC_HIVE}"
fi

# Start Impala
: ${START_CLUSTER_ARGS=""}
if [[ "${TARGET_FILESYSTEM}" == "local" ]]; then
  START_CLUSTER_ARGS="--impalad_args=--abort_on_config_error=false -s 1 ${START_CLUSTER_ARGS}"
else
  START_CLUSTER_ARGS="-s 3 ${START_CLUSTER_ARGS}"
fi
if [[ -z "$REMOTE_LOAD" ]]; then
  run-step "Starting Impala cluster" start-impala-cluster.log \
    ${IMPALA_HOME}/bin/start-impala-cluster.py --log_dir=${IMPALA_DATA_LOADING_LOGS_DIR} \
    ${START_CLUSTER_ARGS}
fi

# The hdfs environment script sets up kms (encryption) and cache pools (hdfs caching).
# On a non-hdfs filesystem, we don't test encryption or hdfs caching, so this setup is not
# needed.
if [[ "${TARGET_FILESYSTEM}" == "hdfs" ]]; then
  run-step "Setting up HDFS environment" setup-hdfs-env.log \
      ${IMPALA_HOME}/testdata/bin/setup-hdfs-env.sh
fi

if [ $SKIP_METADATA_LOAD -eq 0 ]; then
  run-step "Loading custom schemas" load-custom-schemas.log load-custom-schemas
  run-step "Loading functional-query data" load-functional-query.log \
      load-data "functional-query" "exhaustive"
  run-step "Loading TPC-H data" load-tpch.log load-data "tpch" "core"
  # Load tpch nested data.
  # TODO: Hacky and introduces more complexity into the system, but it is expedient.
  if [[ -n "$CM_HOST" ]]; then
    LOAD_NESTED_ARGS="--cm-host $CM_HOST"
  fi
  run-step "Loading nested data" load-nested.log \
    ${IMPALA_HOME}/testdata/bin/load_nested.py ${LOAD_NESTED_ARGS:-}
  run-step "Loading TPC-DS data" load-tpcds.log load-data "tpcds" "core"
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

if $KUDU_IS_SUPPORTED; then
  # Tests depend on the kudu data being clean, so load the data from scratch.
  run-step "Loading Kudu functional" load-kudu.log \
        load-data "functional-query" "core" "kudu/none/none" force
  run-step "Loading Kudu TPCH" load-kudu-tpch.log \
        load-data "tpch" "core" "kudu/none/none" force
fi
run-step "Loading Hive UDFs" build-and-copy-hive-udfs.log \
    build-and-copy-hive-udfs
run-step "Running custom post-load steps" custom-post-load-steps.log \
    custom-post-load-steps

if [ "${TARGET_FILESYSTEM}" = "hdfs" ]; then
  # Caching tables in s3 returns an IllegalArgumentException, see IMPALA-1714
  run-step "Caching test tables" cache-test-tables.log cache-test-tables

  # TODO: Modify the .sql file that creates the table to take an alternative location into
  # account.
  run-step "Loading external data sources" load-ext-data-source.log \
      copy-and-load-ext-data-source

  # HBase splitting is only relevant for FE tests
  if [[ -z "$REMOTE_LOAD" ]]; then
    run-step "Splitting HBase" create-hbase.log ${IMPALA_HOME}/testdata/bin/split-hbase.sh
  fi

  run-step "Creating internal HBase table" create-internal-hbase-table.log \
      create-internal-hbase-table
fi

# TODO: Investigate why all stats are not preserved. Theoretically, we only need to
# recompute stats for HBase.
run-step "Computing table stats" compute-table-stats.log \
    ${IMPALA_HOME}/testdata/bin/compute-table-stats.sh

run-step "Copying auth policy file" copy-auth-policy.log copy-auth-policy
