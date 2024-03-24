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
# This script does the following:
# 1. Starts the mysqld container and creates databases, user, tables.
# 2. Loads the test data in the mysql tables from data files.
# 3. Downloads and installs the mysql jdbc drivers.

set -euo pipefail
. $IMPALA_HOME/bin/report_build_error.sh
setup_report_build_error

. ${IMPALA_HOME}/bin/impala-config.sh > /dev/null 2>&1

# Start mysql server in a docker image
mysqld_status="stopped"
retry_count=0
# Check docker command
if [ $(docker ps > /dev/null 2>&1; echo $?) -gt 0 ];then
  echo "Error: Can't run docker without sudo"
  exit 10
fi
while [ $retry_count -lt 10 ] && [ $mysqld_status = "stopped" ];
do
  docker rm -f mysql 2>/dev/null
  docker run --name mysql -e MYSQL_ROOT_PASSWORD=secret -d -p 3306:3306 mysql
  if [ $(docker ps | grep -c mysql) -eq 1 ];then
    mysqld_status="running"
  fi
  # wait 10 seconds before re-trying.
  sleep 10;
  ((retry_count+=1))
done

if [ $mysqld_status = "stopped" ];then
  echo "Error: Could't start mysqld docker container. Exiting"
  exit 2
fi

# Check if mysqld.sock exists
if [ $(docker exec -i mysql ls /var/run/mysqld/mysqld.sock > /dev/null 2>&1;\
echo $?) -gt 0 ]; then
  echo "Error: File /var/run/mysqld/mysqld.sock not found"
  exit 30
fi

# Add permission to mysql socket file
docker exec -i mysql chmod 777 /var/run/mysqld/mysqld.sock

# Run a test query
if [[ $(docker exec -i mysql mysql -uroot -psecret <<< 'select 1' > \
/dev/null 2>&1; echo $?) -gt 0 ]]; then
  echo "Error: Can't run mysql command"
  exit 20
fi

# Create database functional and user hiveuser with read/write privileges
docker exec -i mysql mysql -uroot -psecret <<< 'drop database \
if exists functional;\
CREATE DATABASE  functional;\
SET GLOBAL local_infile=1;\
DROP USER IF EXISTS  hiveuser;
CREATE USER "hiveuser" IDENTIFIED  BY "password";\
GRANT CREATE, ALTER, DROP, INSERT, UPDATE, DELETE, SELECT ON functional.* \
TO "hiveuser";'

# Create jdbc tables
cat > /tmp/mysql_jdbc_alltypes.sql <<__EOT__
DROP TABLE IF EXISTS alltypes;
CREATE TABLE alltypes
(
    id              INT,
    bool_col        BOOLEAN,
    tinyint_col     SMALLINT,
    smallint_col    SMALLINT,
    int_col         INT,
    bigint_col      BIGINT,
    float_col       FLOAT,
    double_col      DOUBLE PRECISION,
    date_col        DATE,
    string_col      VARCHAR(10),
    timestamp_col   TIMESTAMP
);
__EOT__

docker exec -i mysql mysql -uroot -psecret functional < \
  /tmp/mysql_jdbc_alltypes.sql

cat > /tmp/mysql_jdbc_alltypes_with_case_sensitive_names.sql <<__EOT__
DROP TABLE IF EXISTS AllTypesCaseSensitiveNames;
CREATE TABLE AllTypesCaseSensitiveNames
(
    id            INT,
    Bool_col      BOOLEAN,
    Tinyint_col   SMALLINT,
    Smallint_col  SMALLINT,
    Int_col       INT,
    Bigint_col    BIGINT,
    Float_col     FLOAT,
    Double_col    DOUBLE PRECISION,
    Date_col      DATE,
    String_col    VARCHAR(10),
    Timestamp_col TIMESTAMP
);
__EOT__

docker exec -i mysql mysql -uroot -psecret functional < \
  /tmp/mysql_jdbc_alltypes_with_case_sensitive_names.sql

# Create a table with decimal type of columns.
# Note that the decimal scale in MySQL has a range of 0 to 30, which is smaller than the
# scale range in Impala (0 to 38).
cat > /tmp/mysql_jdbc_decimal_tbl.sql <<__EOT__
DROP TABLE IF EXISTS decimal_tbl;
CREATE TABLE decimal_tbl
(
    d1 DECIMAL(9,0),
    d2 DECIMAL(10,0),
    d3 DECIMAL(20,10),
    d4 DECIMAL(38,30),
    d5 DECIMAL(10,5)
);
__EOT__

docker exec -i mysql mysql -uroot -psecret functional < \
  /tmp/mysql_jdbc_decimal_tbl.sql

# Load data to jdbc table
cat ${IMPALA_HOME}/testdata/target/AllTypes/* > /tmp/mysql_jdbc_alltypes.csv
docker cp /tmp/mysql_jdbc_alltypes.csv mysql:/tmp

loadCmd="LOAD DATA LOCAL INFILE '/tmp/mysql_jdbc_alltypes.csv' INTO TABLE alltypes \
  COLUMNS TERMINATED BY ',' (id, bool_col, tinyint_col, smallint_col, int_col, \
  bigint_col, float_col, double_col, @date_col, string_col, timestamp_col) \
  set date_col = STR_TO_DATE(@date_col, '%m/%d/%Y')"

docker exec -i mysql mysql -uroot -psecret functional --local-infile=1 <<<  "$loadCmd"

loadCmd="LOAD DATA LOCAL INFILE '/tmp/mysql_jdbc_alltypes.csv' INTO TABLE \
  AllTypesCaseSensitiveNames COLUMNS TERMINATED BY ',' (id, bool_col, tinyint_col, \
  smallint_col, int_col, bigint_col, float_col, double_col, @date_col, string_col, \
  timestamp_col) set date_col = STR_TO_DATE(@date_col, '%m/%d/%Y')"

docker exec -i mysql mysql -uroot -psecret functional --local-infile=1 <<<  "$loadCmd"

cat ${IMPALA_HOME}/testdata/data/decimal_tbl.txt > /tmp/mysql_jdbc_decimal_tbl.csv
docker cp /tmp/mysql_jdbc_decimal_tbl.csv mysql:/tmp

loadCmd="LOAD DATA LOCAL INFILE '/tmp/mysql_jdbc_decimal_tbl.csv' INTO TABLE \
  decimal_tbl COLUMNS TERMINATED BY ','"

docker exec -i mysql mysql -uroot -psecret functional --local-infile=1 <<<  "$loadCmd"


EXT_DATA_SOURCE_SRC_PATH=${IMPALA_HOME}/java/ext-data-source
EXT_DATA_SOURCES_HDFS_PATH=${FILESYSTEM_PREFIX}/test-warehouse/data-sources
JDBC_DRIVERS_HDFS_PATH=${EXT_DATA_SOURCES_HDFS_PATH}/jdbc-drivers

hadoop fs -mkdir -p ${JDBC_DRIVERS_HDFS_PATH}

# Download and Copy mysql JDBC driver to HDFS
pushd /tmp
wget "https://downloads.mysql.com/archives\
/get/p/3/file/mysql-connector-j-8.1.0.tar.gz"
tar xzf mysql-connector-j-8.1.0.tar.gz
popd
hadoop fs -put -f \
  /tmp/mysql-connector-j-8.1.0/mysql-connector-j-8.1.0.jar \
  ${JDBC_DRIVERS_HDFS_PATH}/mysql-jdbc.jar

echo "Copied /tmp/mysql-connector-j-8.1.0/mysql-connector-j-8.1.0.jar "\
  "into HDFS ${JDBC_DRIVERS_HDFS_PATH}"
