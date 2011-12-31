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

DROP TABLE IF EXISTS AllTypesNoPart;
CREATE TABLE AllTypesNoPart (
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

DROP TABLE IF EXISTS AllTypes_rc;
CREATE TABLE AllTypes_rc (
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
STORED AS RCFILE;

DROP TABLE IF EXISTS AllTypesSmall;
CREATE TABLE AllTypesSmall LIKE AllTypes;

DROP TABLE IF EXISTS AllTypesSmall_rc;
CREATE TABLE AllTypesSmall_rc LIKE AllTypes_rc;

DROP TABLE IF EXISTS AlltypesError;
CREATE TABLE AllTypesError LIKE AllTypes;

DROP TABLE IF EXISTS AlltypesError_rc;
CREATE TABLE AllTypesError_rc LIKE AllTypes_rc;

DROP TABLE IF EXISTS AlltypesErrorNoNulls;
CREATE TABLE AllTypesErrorNoNulls LIKE AllTypes;

DROP TABLE IF EXISTS AlltypesErrorNoNulls_rc;
CREATE TABLE AllTypesErrorNoNulls_rc LIKE AllTypes_rc;

DROP TABLE IF EXISTS AllTypesAgg;
CREATE TABLE AllTypesAgg (
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
partitioned by (year int, month int, day int)
row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS AllTypesAgg_rc;
CREATE TABLE AllTypesAgg_rc (
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
partitioned by (year int, month int, day int)
STORED AS RCFILE;

DROP TABLE IF EXISTS AllTypesAggNoNulls;
CREATE TABLE AllTypesAggNoNulls LIKE AllTypesAgg;

DROP TABLE IF EXISTS AllTypesAggNoNulls_rc;
CREATE TABLE AllTypesAggNoNulls_rc LIKE AllTypesAgg_rc;

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

DROP TABLE IF EXISTS TestTbl_rc;
CREATE TABLE TestTbl_rc (
  id bigint,
  name string,
  zip int)
STORED AS RCFILE;

DROP TABLE IF EXISTS DimTbl;
CREATE TABLE DimTbl (
  id bigint,
  name string,
  zip int)
row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS JoinTbl;
CREATE TABLE JoinTbl (
  test_id bigint,
  test_name string,
  test_zip int,
  alltypes_id int)
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

DROP TABLE IF EXISTS LikeTbl;
CREATE TABLE LikeTbl (
  str_col string,
  match_like_col string,
  no_match_like_col string,
  match_regex_col string,
  no_match_regex_col string)
row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS HbaseAllTypesSmall;
CREATE EXTERNAL TABLE HbaseAllTypesSmall (
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
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
  "hbase.columns.mapping" =
  ":key,bools:bool_col,ints:tinyint_col,ints:smallint_col,ints:int_col,ints:bigint_col,floats:float_col,floats:double_col,strings:date_string_col,strings:string_col"
)
TBLPROPERTIES("hbase.table.name" = "hbasealltypessmall");

DROP TABLE IF EXISTS HbaseAllTypesError;
CREATE EXTERNAL TABLE HbaseAllTypesError (
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
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
  "hbase.columns.mapping" =
  ":key,bools:bool_col,ints:tinyint_col,ints:smallint_col,ints:int_col,ints:bigint_col,floats:float_col,floats:double_col,strings:date_string_col,strings:string_col"
)
TBLPROPERTIES("hbase.table.name" = "hbasealltypeserror");

DROP TABLE IF EXISTS HbaseAllTypesErrorNoNulls;
CREATE EXTERNAL TABLE HbaseAllTypesErrorNoNulls (
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
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
  "hbase.columns.mapping" =
  ":key,bools:bool_col,ints:tinyint_col,ints:smallint_col,ints:int_col,ints:bigint_col,floats:float_col,floats:double_col,strings:date_string_col,strings:string_col"
)
TBLPROPERTIES("hbase.table.name" = "hbasealltypeserrornonulls");

DROP TABLE IF EXISTS HbaseAllTypesAgg;
CREATE EXTERNAL TABLE HbaseAllTypesAgg (
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
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
  "hbase.columns.mapping" =
  ":key,bools:bool_col,ints:tinyint_col,ints:smallint_col,ints:int_col,ints:bigint_col,floats:float_col,floats:double_col,strings:date_string_col,strings:string_col"
)
TBLPROPERTIES("hbase.table.name" = "hbasealltypesagg");

DROP TABLE IF EXISTS HbaseStringIds;
CREATE EXTERNAL TABLE HbaseStringIds (
  id string,
  bool_col boolean,
  tinyint_col tinyint,
  smallint_col smallint,
  int_col int,
  bigint_col bigint,
  float_col float,
  double_col double,
  date_string_col string,
  string_col string)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
  "hbase.columns.mapping" =
  ":key,bools:bool_col,ints:tinyint_col,ints:smallint_col,ints:int_col,ints:bigint_col,floats:float_col,floats:double_col,strings:date_string_col,strings:string_col"
)
TBLPROPERTIES("hbase.table.name" = "hbasealltypesagg");

DROP TABLE IF EXISTS EscapeNoQuotes;
CREATE TABLE EscapeNoQuotes (
  col1 string,
  col2 string,
  col3 int,
  col4 int)
row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS Overflow;
CREATE TABLE Overflow (
  tinyint_col tinyint,
  smallint_col smallint,
  int_col int,
  bigint_col bigint,
  float_col float,
  double_col double)
row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS GrepTiny;
CREATE TABLE GrepTiny (
  field string);

DROP TABLE IF EXISTS RankingsSmall;
CREATE TABLE RankingsSmall (
  pageRank int,
  pageURL string,
  avgDuration int)
row format delimited fields terminated by '|'  stored as textfile;

DROP TABLE IF EXISTS UserVisitsSmall;
CREATE TABLE UserVisitsSmall (
  sourceIP string,
  destURL string,
  visitDate string,
  adRevenue float,
  userAgent string,
  cCode string,
  lCode string,
  sKeyword string,
  avgTimeOnSite int)
row format delimited fields terminated by '|'  stored as textfile;

