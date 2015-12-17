#!/usr/bin/env bash
# Copyright 2012 Cloudera Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Script that allows easily creating tables with a large number of partitions
# and/or blocks. To achieve generation of a large number of blocks, the script
# generates many tiny files. Each file will be assigned a unique block. These
# files are copied to HDFS and all partitions are mapped to this location. This
# way a table with 100K blocks can be created by using 100 partitions x 1000
# blocks/files.

set -euo pipefail
trap 'echo Error in $0 at line $LINENO: $(cd "'$PWD'" && awk "NR == $LINENO" $0)' ERR

. ${IMPALA_HOME}/bin/impala-config.sh > /dev/null 2>&1

LOCAL_OUTPUT_DIR=$(mktemp -dt "impala_test_tmp.XXXXXX")
echo $LOCAL_OUTPUT_DIR

BLOCKS_PER_PARTITION=-1
NUM_PARTITIONS=-1

# parse command line options
while getopts "p:b:" OPTION
do
  case "$OPTION" in
    p)
      NUM_PARTITIONS=$OPTARG
      ;;
    b)
      BLOCKS_PER_PARTITION=$OPTARG
      ;;
    ?)
      echo "create-table-many-blocks.sh -p <num partitions> -b <num blocks / partition>"
      exit 1;
      ;;
  esac
done

if [ $NUM_PARTITIONS -lt 1 ]; then
  echo "Must specify a value of 1 or more for the number of partitions"
  exit 1
fi

if [ $BLOCKS_PER_PARTITION -lt 0 ]; then
  echo "Must specify a value of 0 or greater for blocks per partition"
  exit 1
fi

HDFS_PATH=/test-warehouse/many_blocks_num_blocks_per_partition_${BLOCKS_PER_PARTITION}/
DB_NAME=scale_db
TBL_NAME=num_partitions_${NUM_PARTITIONS}_blocks_per_partition_${BLOCKS_PER_PARTITION}

hive -e "create database if not exists scale_db"
hive -e "drop table if exists ${DB_NAME}.${TBL_NAME}"
hive -e "create external table ${DB_NAME}.${TBL_NAME} (i int) partitioned by (j int)"

# Generate many (small) files. Each file will be assigned a unique block.
echo "Generating ${BLOCKS_PER_PARTITION} files"
for b in $(seq ${BLOCKS_PER_PARTITION})
do
  echo $b > ${LOCAL_OUTPUT_DIR}/impala_$b.data
done

echo "Copying data files to HDFS"
hadoop fs -rm -r -f ${HDFS_PATH}
hadoop fs -mkdir -p ${HDFS_PATH}
hadoop fs -put ${LOCAL_OUTPUT_DIR}/* ${HDFS_PATH}

echo "Generating DDL statements"
# Use Hive to create the partitions because it supports bulk adding of partitions.
# Hive doesn't allow fully qualified table names in ALTER statements, so start with a
# USE <db>.
echo "use ${DB_NAME};" > ${LOCAL_OUTPUT_DIR}/hive_create_partitions.q

# Generate the H-SQL bulk partition DDL statement
echo "ALTER TABLE ${TBL_NAME} ADD " >> ${LOCAL_OUTPUT_DIR}/hive_create_partitions.q
for p in $(seq ${NUM_PARTITIONS})
do
  echo " PARTITION (j=$p) LOCATION '${HDFS_PATH}'" >>\
      ${LOCAL_OUTPUT_DIR}/hive_create_partitions.q
done
echo ";" >> ${LOCAL_OUTPUT_DIR}/hive_create_partitions.q

echo "Executing DDL via Hive"
hive -f ${LOCAL_OUTPUT_DIR}/hive_create_partitions.q

echo "Done! Final result in table: ${DB_NAME}.${TBL_NAME}"
