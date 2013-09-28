#!/bin/bash
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
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

if [ x${JAVA_HOME} == x ]; then
  echo JAVA_HOME not set
  exit 1
fi
. ${IMPALA_HOME}/bin/impala-config.sh
set -e

# If the user has specified a command line argument, treat it as the test-warehouse
# snapshot file and pass it to the load-test-warehouse-snapshot.sh script for processing.
if [[ $1 ]]; then
  ${IMPALA_HOME}/testdata/bin/load-test-warehouse-snapshot.sh "$1"
else
  echo "Loading hive builtins"
  ${IMPALA_HOME}/testdata/bin/load-hive-builtins.sh
fi
set -u

IMPALAD_LOG_DIR=${IMPALA_TEST_CLUSTER_LOG_DIR}/data_loading
mkdir -p ${IMPALAD_LOG_DIR}

# Load the data set
pushd ${IMPALA_HOME}/bin
./start-impala-cluster.py -s 3 --wait_for_cluster --log_dir=${IMPALAD_LOG_DIR}
# Use unbuffered logging by executing these data loading steps with 'python -u'
python -u ./load-data.py --workloads functional-query --exploration_strategy exhaustive
echo "Loading data into hbasealltypeserror and hbasealltypeserrononulls"
${IMPALA_HOME}/testdata/bin/create-hbase.sh
python -u ./load-data.py --workloads tpcds --exploration_strategy core
python -u ./load-data.py --workloads tpch --exploration_strategy core
# Load all the auxiliary workloads (if any exist)
if [ -d ${IMPALA_AUX_WORKLOAD_DIR} ] && [ -d ${IMPALA_AUX_DATASET_DIR} ]; then
  python -u ./load-data.py --workloads all --workload_dir=${IMPALA_AUX_WORKLOAD_DIR}\
      --dataset_dir=${IMPALA_AUX_DATASET_DIR} --exploration_strategy core
else
  echo "Skipping load of auxilary workloads because directories do not exist"
fi

./start-impala-cluster.py --kill_only
popd

# Split HBase table
echo "Splitting HBase table"
${IMPALA_HOME}/testdata/bin/split-hbase.sh

echo COPYING AUTHORIZATION POLICY FILE
hadoop fs -rm -f /test-warehouse/authz-policy.ini
hadoop fs -put ${IMPALA_HOME}/fe/src/test/resources/authz-policy.ini /test-warehouse/

# TODO: The multi-format table will move these files. So we need to copy them to a
# temporary location for that table to use. Should find a better way to handle this.
echo COPYING DATA FOR DEPENDENT TABLES
hadoop fs -rm -r -f /test-warehouse/alltypesmixedformat
hadoop fs -rm -r -f /tmp/alltypes_rc
hadoop fs -rm -r -f /tmp/alltypes_seq
hadoop fs -mkdir -p /tmp/alltypes_seq/year=2009
hadoop fs -mkdir -p /tmp/alltypes_rc/year=2009
hadoop fs -cp  /test-warehouse/alltypes_seq/year=2009/month=2/ /tmp/alltypes_seq/year=2009
hadoop fs -cp  /test-warehouse/alltypes_rc/year=2009/month=3/ /tmp/alltypes_rc/year=2009

# Create a hidden file in AllTypesSmall
hadoop fs -rm -f /test-warehouse/alltypessmall/year=2009/month=1/_hidden
hadoop fs -rm -f /test-warehouse/alltypessmall/year=2009/month=1/.hidden
hadoop fs -cp  /test-warehouse/zipcode_incomes/DEC_00_SF3_P077_with_ann_noheader.csv \
 /test-warehouse/alltypessmall/year=2009/month=1/_hidden
hadoop fs -cp  /test-warehouse/zipcode_incomes/DEC_00_SF3_P077_with_ann_noheader.csv \
 /test-warehouse/alltypessmall/year=2009/month=1/.hidden

# TODO: For some reason DROP TABLE IF EXISTS sometimes fails on HBase if the table does
# not exist. To work around this, disable exit on error before executing this command.
# Need to investigate this more, but this works around the problem to unblock automation.
set +o errexit
${HIVE_HOME}/bin/hive -hiveconf hive.root.logger=WARN,console -v \
    -e "DROP TABLE IF EXISTS functional_hbase.internal_hbase_table"
echo "disable 'functional_hbase.internal_hbase_table'" | hbase shell
echo "drop 'functional_hbase.internal_hbase_table'" | hbase shell
set -e

# For tables that rely on loading data from local fs test-warehouse
# TODO: Find a good way to integrate this with the normal data loading scripts
${HIVE_HOME}/bin/hive -hiveconf hive.root.logger=WARN,console -v \
  -f ${IMPALA_HOME}/testdata/bin/load-dependent-tables.sql
if [ $? != 0 ]; then
  echo DEPENDENT LOAD FAILED
  exit 1
fi

# Load the index files for corrupted lzo data.
hadoop fs -rm -f /test-warehouse/bad_text_lzo_text_lzo/bad_text.lzo.index
hadoop fs -put ${IMPALA_HOME}/testdata/bad_text_lzo/bad_text.lzo.index \
    /test-warehouse/bad_text_lzo_text_lzo/

hadoop fs -rm -r -f /bad_text_lzo_text_lzo/
hadoop fs -mv /test-warehouse/bad_text_lzo_text_lzo/ /
# Cleanup the old bad_text_lzo files, if they exist.
hadoop fs -rm -r -f /test-warehouse/bad_text_lzo/

# Index all lzo files in HDFS under /test-warehouse
${IMPALA_HOME}/testdata/bin/lzo_indexer.sh /test-warehouse

hadoop fs -mv /bad_text_lzo_text_lzo/ /test-warehouse/

# Remove an index file so we test an un-indexed LZO file
hadoop fs -rm /test-warehouse/alltypes_text_lzo/year=2009/month=1/000013_0.lzo.index

# Add a sequence file that only contains a header (see IMPALA-362)
hadoop fs -put -f ${IMPALA_HOME}/testdata/tinytable_seq_snap/tinytable_seq_snap_header_only \
                  /test-warehouse/tinytable_seq_snap

# Create special table for testing Avro schema resolution
# (see testdata/avro_schema_resolution/README)
pushd ${IMPALA_HOME}/testdata/avro_schema_resolution
hive -f create_table.sql
popd

${IMPALA_HOME}/testdata/bin/compute-table-stats.sh

# Copy the test UDF library into HDFS
hadoop fs -put -f ${IMPALA_HOME}/be/build/debug/testutil/libTestUdfs.so /test-warehouse
hadoop fs -put -f ${IMPALA_HOME}/be/build/debug/testutil/test-udfs.ll /test-warehouse
