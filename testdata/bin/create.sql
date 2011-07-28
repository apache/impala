DROP TABLE IF EXISTS AllTypes;
CREATE TABLE AllTypes (
  id int,
  bool_col boolean,
  tinyint_col tinyint,
  smallint_col smallint,
  int_col int,
  bigint_col bigint,
  float_col float,
  double_col double,
  date_string_col string,
  string_col string)
partitioned by (year int, month int)
row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS AllTypesSmall;
CREATE TABLE AllTypesSmall LIKE AllTypes;

DROP TABLE IF EXISTS AlltypesError;
CREATE TABLE AllTypesError LIKE AllTypes;

DROP TABLE IF EXISTS DelimErrorTable;
CREATE TABLE DelimErrorTable (
  id int,
  name string)
partitioned by (year int, month int) row format delimited 
  fields terminated by '<>'
  escaped by '$$'
  collection items terminated by '^^'
  map keys terminated by '**' 
  stored as textfile;

DROP TABLE IF EXISTS TestTbl;
CREATE TABLE TestTbl (
  id bigint,
  name string,
  zip int)
row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

CREATE DATABASE IF NOT EXISTS testdb1;

DROP TABLE IF EXISTS testdb1.AllTypes;
CREATE TABLE testdb1.AllTypes (
  id int,
  bool_col boolean,
  tinyint_col tinyint,
  smallint_col smallint,
  int_col int,
  bigint_col bigint,
  float_col float,
  double_col double,
  date_string_col string,
  string_col string)
row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS testdb1.TestTbl;
CREATE TABLE testdb1.TestTbl (
  id bigint,
  name string,
  birthday string)
row format delimited fields terminated by ','  escaped by '\\' stored as textfile;
