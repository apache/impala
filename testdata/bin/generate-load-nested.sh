#!/bin/bash
# Copyright 2015 Cloudera Inc.
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

set -euo pipefail
trap 'echo Error in $0 at line $LINENO: $(awk "NR == $LINENO" $0)' ERR

SHELL_CMD=${IMPALA_HOME}/bin/impala-shell.sh

usage() {
  echo "Usage: $0 [-n num databases] [-e num elements]" 1>&2;
  echo "       [-i num list items] [-f]" 1>&2;
  echo "Tables will be flattened if -f is set." 1>&2;
  exit 1;
}

NUM_DATABASES=1
NUM_ELEMENTS=$((10**7))
NUM_FIELDS=30
FLATTEN=false
while getopts ":n:e:i:f" OPTION; do
  case "${OPTION}" in
    n)
      NUM_DATABASES=${OPTARG}
      ;;
    e)
      NUM_ELEMENTS=${OPTARG}
      ;;
    i)
      NUM_FIELDS=${OPTARG}
      ;;
    f)
      FLATTEN=true
      ;;
    *)
      usage
      ;;
  esac
done

RANDOM_SCHEMA_GENERATOR=${IMPALA_HOME}/testdata/bin/random_avro_schema.py;
HDFS_DIR=/test-warehouse/random_nested_data

hdfs dfs -rm -r -f ${HDFS_DIR}
hdfs dfs -mkdir -p ${HDFS_DIR}

BASE_DIR=/tmp/random_nested_data
rm -rf ${BASE_DIR}

for ((NUM=0; NUM < $NUM_DATABASES; NUM++))
do
  DB_DIR=${BASE_DIR}/db_${NUM}
  mkdir -p ${DB_DIR}

  ${RANDOM_SCHEMA_GENERATOR} --target_dir=${DB_DIR}

  AVRO_SCHEMAS=${DB_DIR}/*.avsc
  for AVRO_SCHEMA_PATH in ${AVRO_SCHEMAS}
  do
    FILE_NAME=$(basename ${AVRO_SCHEMA_PATH})
    TABLE_NAME="${FILE_NAME%.*}"
    mvn -f "${IMPALA_HOME}/testdata/pom.xml" exec:java \
      -Dexec.mainClass="com.cloudera.impala.datagenerator.RandomNestedDataGenerator" \
      -Dexec.args="${AVRO_SCHEMA_PATH} ${NUM_ELEMENTS} ${NUM_FIELDS} ${DB_DIR}/${TABLE_NAME}/${TABLE_NAME}.parquet";

    if $FLATTEN; then
      mvn -f "${IMPALA_HOME}/testdata/TableFlattener/pom.xml" \
        exec:java -Dexec.mainClass=com.cloudera.impala.infra.tableflattener.Main \
        -Dexec.arguments="file://${DB_DIR}/${TABLE_NAME}/${TABLE_NAME}.parquet,file://${DB_DIR}/${TABLE_NAME}_flat,-sfile://${AVRO_SCHEMA_PATH}";
    fi
  done

  hdfs dfs -put ${DB_DIR} ${HDFS_DIR}

  ${SHELL_CMD} -q "
    DROP DATABASE IF EXISTS random_nested_db_${NUM} CASCADE;
    CREATE DATABASE random_nested_db_${NUM};"

  if $FLATTEN; then
    ${SHELL_CMD} -q "
      DROP DATABASE IF EXISTS random_nested_db_flat_${NUM} CASCADE;
      CREATE DATABASE random_nested_db_flat_${NUM};"
  fi

  for AVRO_SCHEMA_PATH in ${AVRO_SCHEMAS}
  do
    FILE_NAME=$(basename ${AVRO_SCHEMA_PATH})
    TABLE_NAME="${FILE_NAME%.*}"

    ${SHELL_CMD} -q "
      USE random_nested_db_${NUM};
      CREATE EXTERNAL TABLE ${TABLE_NAME} like parquet
      '${HDFS_DIR}/db_${NUM}/${TABLE_NAME}/${TABLE_NAME}.parquet'
      STORED AS PARQUET
      LOCATION '${HDFS_DIR}/db_${NUM}/${TABLE_NAME}';"

    if $FLATTEN; then
      FLAT_TABLE_DIR=${DB_DIR}/${TABLE_NAME}_flat/*
      for FLAT_TABLE in ${FLAT_TABLE_DIR}
      do
        FLAT_TABLE_NAME=$(basename ${FLAT_TABLE})
        FLAT_FILE_NAME=$(basename ${FLAT_TABLE}/*)
        ${SHELL_CMD} -q "
          USE random_nested_db_flat_${NUM};
          CREATE EXTERNAL TABLE ${FLAT_TABLE_NAME} like parquet
          '${HDFS_DIR}/db_${NUM}/${TABLE_NAME}_flat/${FLAT_TABLE_NAME}/${FLAT_FILE_NAME}'
          STORED AS PARQUET
          LOCATION '${HDFS_DIR}/db_${NUM}/${TABLE_NAME}_flat/${FLAT_TABLE_NAME}';"
      done
    fi
  done

  if $FLATTEN; then
    dropdb --if-exists -U postgres random_nested_db_flat_${NUM}
    ${IMPALA_HOME}/tests/comparison/data_generator.py --use-postgresql \
      --db-name=random_nested_db_flat_${NUM} migrate
  fi
done
