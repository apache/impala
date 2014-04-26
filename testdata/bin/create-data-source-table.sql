-- Copyright 2012 Cloudera Inc.
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
-- http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

-- Create test data sources and tables

USE functional;

DROP DATA SOURCE IF EXISTS AllTypesDataSource;
CREATE DATA SOURCE AllTypesDataSource
LOCATION '/test-warehouse/data-sources/test-data-source.jar'
CLASS 'com.cloudera.impala.extdatasource.AllTypesDataSource'
API_VERSION 'V1';

DROP TABLE IF EXISTS alltypes_datasource;
CREATE TABLE alltypes_datasource (
  bool_col BOOLEAN,
  tinyint_col TINYINT,
  smallint_col SMALLINT,
  int_col INT,
  bigint_col BIGINT,
  float_col FLOAT,
  double_col DOUBLE,
  timestamp_col TIMESTAMP,
  string_col STRING)
PRODUCED BY DATA SOURCE AllTypesDataSource("TestInitString");

DROP TABLE IF EXISTS decimal_datasource;
CREATE TABLE decimal_datasource (
  d1 DECIMAL(9,0),
  d2 DECIMAL(10,0),
  d3 DECIMAL(20,10),
  d4 DECIMAL(38,37),
  d5 DECIMAL(10,5))
PRODUCED BY DATA SOURCE AllTypesDataSource;
