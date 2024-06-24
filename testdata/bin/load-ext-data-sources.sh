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
# This script create and load the jdbc data source target table in Postgres.

set -euo pipefail
. $IMPALA_HOME/bin/report_build_error.sh
setup_report_build_error

. ${IMPALA_HOME}/bin/impala-config.sh > /dev/null 2>&1

# Create functional.alltype table
dropdb -U hiveuser functional || true
createdb -U hiveuser functional

# Create jdbc table
cat > /tmp/jdbc_alltypes.sql <<__EOT__
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
sudo -u postgres psql -U hiveuser -d functional -f /tmp/jdbc_alltypes.sql

# Create jdbc table with case sensitive names for table and columns.
cat > /tmp/jdbc_alltypes_with_quote.sql <<__EOT__
DROP TABLE IF EXISTS "AllTypesWithQuote";
CREATE TABLE "AllTypesWithQuote"
(
    "id"            INT,
    "Bool_col"      BOOLEAN,
    "Tinyint_col"   SMALLINT,
    "Smallint_col"  SMALLINT,
    "Int_col"       INT,
    "Bigint_col"    BIGINT,
    "Float_col"     FLOAT,
    "Double_col"    DOUBLE PRECISION,
    "date_col"      DATE,
    "String_col"    VARCHAR(10),
    "Timestamp_col" TIMESTAMP
);
__EOT__
sudo -u postgres psql -U hiveuser -d functional -f /tmp/jdbc_alltypes_with_quote.sql

# Create a table with decimal type of columns
cat > /tmp/jdbc_decimal_tbl.sql <<__EOT__
DROP TABLE IF EXISTS decimal_tbl;
CREATE TABLE decimal_tbl
(
    d1 DECIMAL(9,0),
    d2 DECIMAL(10,0),
    d3 DECIMAL(20,10),
    d4 DECIMAL(38,38),
    d5 DECIMAL(10,5)
);
__EOT__
sudo -u postgres psql -U hiveuser -d functional -f /tmp/jdbc_decimal_tbl.sql

# Create test_strategy1 table for unit test
cat > /tmp/jdbc_test_strategy.sql << __EOT__
DROP TABLE IF EXISTS test_strategy;
CREATE TABLE test_strategy
(
  strategy_id INT,
  name VARCHAR(50),
  referrer VARCHAR(1024),
  landing VARCHAR(1024),
  priority INT,
  implementation VARCHAR(512),
  last_modified timestamp,
  PRIMARY KEY (strategy_id)
);

INSERT INTO test_strategy (strategy_id, name, referrer, landing, priority,
  implementation, last_modified) VALUES
  (1, 'S1', 'aaa', 'abc', 1000, NULL, '2012-05-08 15:01:15'),
  (2, 'S2', 'bbb', 'def', 990, NULL, '2012-05-08 15:01:15'),
  (3, 'S3', 'ccc', 'ghi', 1000, NULL, '2012-05-08 15:01:15'),
  (4, 'S4', 'ddd', 'jkl', 980, NULL, '2012-05-08 15:01:15'),
  (5, 'S5', 'eee', NULL, NULL, NULL, '2012-05-08 15:01:15');
__EOT__
sudo -u postgres psql -U hiveuser -d functional -f /tmp/jdbc_test_strategy.sql

# Create country table
cat > /tmp/jdbc_country.sql << __EOT__
DROP TABLE IF EXISTS country;
CREATE TABLE country
(
  id int,
  name varchar(20),
  bool_col BOOLEAN,
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
INSERT INTO country (id, name, bool_col, tinyint_col, smallint_col, int_col,
bigint_col, float_col, double_col, date_col, string_col, timestamp_col)
VALUES
(1, 'India', TRUE, 10, 100, 1000, 10000, 1.1, 1.11, '2024-01-01',
  'IN', '2024-01-01 10:00:00'),
(2, 'Russia', FALSE, 20, 200, 2000, 20000, 2.2, 2.22, '2024-02-01',
  'RU', '2024-02-01 11:00:00'),
(3, 'USA', TRUE, 30, 300, 3000, 30000, 3.3, 3.33, '2024-03-01',
  'US', '2024-03-01 12:00:00');
__EOT__
sudo -u postgres psql -U hiveuser -d functional -f /tmp/jdbc_country.sql

# Load data to jdbc table
cat ${IMPALA_HOME}/testdata/target/AllTypes/* > /tmp/jdbc_alltypes.csv
loadCmd="\COPY alltypes FROM '/tmp/jdbc_alltypes.csv' DELIMITER ',' CSV"
sudo -u postgres psql -d functional -c "$loadCmd"

loadCmd="\COPY \"AllTypesWithQuote\" FROM '/tmp/jdbc_alltypes.csv' DELIMITER ',' CSV"
sudo -u postgres psql -d functional -c "$loadCmd"

cat ${IMPALA_HOME}/testdata/data/decimal_tbl.txt > /tmp/jdbc_decimal_tbl.csv
loadCmd="\COPY decimal_tbl FROM '/tmp/jdbc_decimal_tbl.csv' DELIMITER ',' CSV"
sudo -u postgres psql -d functional -c "$loadCmd"

# Create impala tables and load data
cat > /tmp/impala_jdbc_alltypes.sql <<__EOT__
USE FUNCTIONAL;
DROP TABLE IF EXISTS alltypes_with_date;
CREATE TABLE alltypes_with_date
(
    id              INT,
    bool_col        BOOLEAN,
    tinyint_col     SMALLINT,
    smallint_col    SMALLINT,
    int_col         INT,
    bigint_col      BIGINT,
    float_col       FLOAT,
    double_col      DOUBLE,
    date_col        DATE,
    string_col      STRING,
    timestamp_col   TIMESTAMP
) STORED as PARQUET;

INSERT INTO alltypes_with_date
SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col,
  double_col, CAST(to_timestamp(date_string_col, 'MM/dd/yy') as DATE), string_col,
  timestamp_col
FROM FUNCTIONAL.alltypes;
__EOT__

IMPALAD=${IMPALAD:-localhost}
${IMPALA_HOME}/bin/impala-shell.sh -i ${IMPALAD} -f /tmp/impala_jdbc_alltypes.sql

# Clean tmp files
rm /tmp/jdbc_alltypes.*
rm /tmp/jdbc_alltypes_with_quote.*
rm /tmp/jdbc_decimal_tbl.*
rm /tmp/jdbc_test_strategy.*
rm /tmp/jdbc_country.*
rm /tmp/impala_jdbc_alltypes.sql
