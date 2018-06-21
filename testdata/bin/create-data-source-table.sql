--
-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--   http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.

-- Create test data sources and tables

USE functional;

DROP DATA SOURCE IF EXISTS AllTypesDataSource;
CREATE DATA SOURCE AllTypesDataSource
LOCATION '/test-warehouse/data-sources/test-data-source.jar'
CLASS 'org.apache.impala.extdatasource.AllTypesDataSource'
API_VERSION 'V1';

DROP TABLE IF EXISTS alltypes_datasource;
CREATE TABLE alltypes_datasource (
  id INT,
  bool_col BOOLEAN,
  tinyint_col TINYINT,
  smallint_col SMALLINT,
  int_col INT,
  bigint_col BIGINT,
  float_col FLOAT,
  double_col DOUBLE,
  timestamp_col TIMESTAMP,
  string_col STRING,
  dec_col1 DECIMAL(9,0),
  dec_col2 DECIMAL(10,0),
  dec_col3 DECIMAL(20,10),
  dec_col4 DECIMAL(38,37),
  dec_col5 DECIMAL(10,5),
  date_col DATE)
PRODUCED BY DATA SOURCE AllTypesDataSource("TestInitString");
