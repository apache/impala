DROP TABLE IF EXISTS AllTypesAggMini;
-- Note no partitioning, because data already loaded into non-partitioned dirs
CREATE EXTERNAL TABLE AllTypesAggMini (
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
row format delimited fields terminated by ','  escaped by '\\' stored as textfile location '/impala-dist-test';
