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
  string_col string,
  timestamp_col timestamp)
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
  string_col string,
  timestamp_col timestamp)
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
  string_col string,
  timestamp_col timestamp)
partitioned by (year int, month int)
STORED AS RCFILE;

DROP TABLE IF EXISTS AllTypes_seq;
CREATE TABLE AllTypes_seq (
  id int,
  bool_col boolean,
  tinyint_col tinyint,
  smallint_col smallint,
  int_col int,
  bigint_col bigint,
  float_col float,
  double_col double,
  date_string_col string,
  string_col string,
  timestamp_col timestamp)
partitioned by (year int, month int)
STORED AS SEQUENCEFILE;


DROP TABLE IF EXISTS AllTypes_seq_def;
CREATE TABLE AllTypes_seq_def LIKE AllTypes_seq;

DROP TABLE IF EXISTS AllTypes_seq_gzip;
CREATE TABLE AllTypes_seq_gzip LIKE AllTypes_seq;

DROP TABLE IF EXISTS AllTypes_seq_bzip;
CREATE TABLE AllTypes_seq_bzip LIKE AllTypes_seq;

DROP TABLE IF EXISTS AllTypes_seq_snap;
CREATE TABLE AllTypes_seq_snap LIKE AllTypes_seq;

DROP TABLE IF EXISTS AllTypes_seq_record_def;
CREATE TABLE AllTypes_seq_record_def LIKE AllTypes_seq;

DROP TABLE IF EXISTS AllTypes_seq_record_gzip;
CREATE TABLE AllTypes_seq_record_gzip LIKE AllTypes_seq;

DROP TABLE IF EXISTS AllTypes_seq_record_bzip;
CREATE TABLE AllTypes_seq_record_bzip LIKE AllTypes_seq;

DROP TABLE IF EXISTS AllTypes_seq_record_snap;
CREATE TABLE AllTypes_seq_record_snap LIKE AllTypes_seq;

DROP TABLE IF EXISTS AllTypesSmall;
CREATE TABLE AllTypesSmall LIKE AllTypes;

DROP TABLE IF EXISTS AllTypesSmall_rc;
CREATE TABLE AllTypesSmall_rc LIKE AllTypes_rc;

DROP TABLE IF EXISTS AllTypesSmall_seq;
CREATE TABLE AllTypesSmall_seq LIKE AllTypes_seq;

DROP TABLE IF EXISTS AllTypesSmall_seq_def;
CREATE TABLE AllTypesSmall_seq_def LIKE AllTypes_seq;

DROP TABLE IF EXISTS AllTypesSmall_seq_gzip;
CREATE TABLE AllTypesSmall_seq_gzip LIKE AllTypes_seq;

DROP TABLE IF EXISTS AllTypesSmall_seq_bzip;
CREATE TABLE AllTypesSmall_seq_bzip LIKE AllTypes_seq;

DROP TABLE IF EXISTS AllTypesSmall_seq_snap;
CREATE TABLE AllTypesSmall_seq_snap LIKE AllTypes_seq;

DROP TABLE IF EXISTS AllTypesSmall_seq_record_def;
CREATE TABLE AllTypesSmall_seq_record_def LIKE AllTypes_seq;

DROP TABLE IF EXISTS AllTypesSmall_seq_record_gzip;
CREATE TABLE AllTypesSmall_seq_record_gzip LIKE AllTypes_seq;

DROP TABLE IF EXISTS AllTypesSmall_seq_record_bzip;
CREATE TABLE AllTypesSmall_seq_record_bzip LIKE AllTypes_seq;

DROP TABLE IF EXISTS AllTypesSmall_seq_record_snap;
CREATE TABLE AllTypesSmall_seq_record_snap LIKE AllTypes_seq;

DROP TABLE IF EXISTS AlltypesError;
CREATE TABLE AllTypesError LIKE AllTypes;

DROP TABLE IF EXISTS AlltypesError_rc;
CREATE TABLE AllTypesError_rc LIKE AllTypes_rc;

DROP TABLE IF EXISTS AlltypesError_seq;
CREATE TABLE AllTypesError_seq LIKE AllTypes_seq;

DROP TABLE IF EXISTS AlltypesError_seq_def;
CREATE TABLE AllTypesError_seq_def LIKE AllTypes_seq;

DROP TABLE IF EXISTS AlltypesError_seq_gzip;
CREATE TABLE AllTypesError_seq_gzip LIKE AllTypes_seq;

DROP TABLE IF EXISTS AlltypesError_seq_bzip;
CREATE TABLE AllTypesError_seq_bzip LIKE AllTypes_seq;

DROP TABLE IF EXISTS AlltypesError_seq_snap;
CREATE TABLE AllTypesError_seq_snap LIKE AllTypes_seq;

DROP TABLE IF EXISTS AlltypesError_seq_record_def;
CREATE TABLE AllTypesError_seq_record_def LIKE AllTypes_seq;

DROP TABLE IF EXISTS AlltypesError_seq_record_gzip;
CREATE TABLE AllTypesError_seq_record_gzip LIKE AllTypes_seq;

DROP TABLE IF EXISTS AlltypesError_seq_record_bzip;
CREATE TABLE AllTypesError_seq_record_bzip LIKE AllTypes_seq;

DROP TABLE IF EXISTS AlltypesError_seq_record_snap;
CREATE TABLE AllTypesError_seq_record_snap LIKE AllTypes_seq;

DROP TABLE IF EXISTS AlltypesErrorNoNulls;
CREATE TABLE AllTypesErrorNoNulls LIKE AllTypes;

DROP TABLE IF EXISTS AlltypesErrorNoNulls_rc;
CREATE TABLE AllTypesErrorNoNulls_rc LIKE AllTypes_rc;

DROP TABLE IF EXISTS AlltypesErrorNoNulls_seq;
CREATE TABLE AllTypesErrorNoNulls_seq LIKE AllTypes_seq;

DROP TABLE IF EXISTS AlltypesErrorNoNulls_seq_def;
CREATE TABLE AllTypesErrorNoNulls_seq_def LIKE AllTypes_seq;

DROP TABLE IF EXISTS AlltypesErrorNoNulls_seq_gzip;
CREATE TABLE AllTypesErrorNoNulls_seq_gzip LIKE AllTypes_seq;

DROP TABLE IF EXISTS AlltypesErrorNoNulls_seq_bzip;
CREATE TABLE AllTypesErrorNoNulls_seq_bzip LIKE AllTypes_seq;

DROP TABLE IF EXISTS AlltypesErrorNoNulls_seq_snap;
CREATE TABLE AllTypesErrorNoNulls_seq_snap LIKE AllTypes_seq;

DROP TABLE IF EXISTS AlltypesErrorNoNulls_seq_record_def;
CREATE TABLE AllTypesErrorNoNulls_seq_record_def LIKE AllTypes_seq;

DROP TABLE IF EXISTS AlltypesErrorNoNulls_seq_record_gzip;
CREATE TABLE AllTypesErrorNoNulls_seq_record_gzip LIKE AllTypes_seq;

DROP TABLE IF EXISTS AlltypesErrorNoNulls_seq_record_bzip;
CREATE TABLE AllTypesErrorNoNulls_seq_record_bzip LIKE AllTypes_seq;

DROP TABLE IF EXISTS AlltypesErrorNoNulls_seq_record_snap;
CREATE TABLE AllTypesErrorNoNulls_seq_record_snap LIKE AllTypes_seq;

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
  string_col string,
  timestamp_col timestamp)
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
  string_col string,
  timestamp_col timestamp)
partitioned by (year int, month int, day int)
STORED AS RCFILE;

DROP TABLE IF EXISTS AllTypesAgg_seq;
CREATE TABLE AllTypesAgg_seq (
  id int,
  bool_col boolean,
  tinyint_col tinyint,
  smallint_col smallint,
  int_col int,
  bigint_col bigint,
  float_col float,
  double_col double,
  date_string_col string,
  string_col string,
  timestamp_col timestamp)
partitioned by (year int, month int, day int)
STORED AS SEQUENCEFILE;

DROP TABLE IF EXISTS AllTypesAgg_seq_def;
CREATE TABLE AllTypesAgg_seq_def LIKE AllTypesAgg_seq;

DROP TABLE IF EXISTS AllTypesAgg_seq_gzip;
CREATE TABLE AllTypesAgg_seq_gzip LIKE AllTypesAgg_seq;

DROP TABLE IF EXISTS AllTypesAgg_seq_bzip;
CREATE TABLE AllTypesAgg_seq_bzip LIKE AllTypesAgg_seq;

DROP TABLE IF EXISTS AllTypesAgg_seq_snap;
CREATE TABLE AllTypesAgg_seq_snap LIKE AllTypesAgg_seq;

DROP TABLE IF EXISTS AllTypesAgg_seq_record_def;
CREATE TABLE AllTypesAgg_seq_record_def LIKE AllTypesAgg_seq;

DROP TABLE IF EXISTS AllTypesAgg_seq_record_gzip;
CREATE TABLE AllTypesAgg_seq_record_gzip LIKE AllTypesAgg_seq;

DROP TABLE IF EXISTS AllTypesAgg_seq_record_bzip;
CREATE TABLE AllTypesAgg_seq_record_bzip LIKE AllTypesAgg_seq;

DROP TABLE IF EXISTS AllTypesAgg_seq_record_snap;
CREATE TABLE AllTypesAgg_seq_record_snap LIKE AllTypesAgg_seq;

DROP TABLE IF EXISTS AllTypesAggNoNulls;
CREATE TABLE AllTypesAggNoNulls LIKE AllTypesAgg;

DROP TABLE IF EXISTS AllTypesAggNoNulls_rc;
CREATE TABLE AllTypesAggNoNulls_rc LIKE AllTypesAgg_rc;

DROP TABLE IF EXISTS AllTypesAggNoNulls_seq;
CREATE TABLE AllTypesAggNoNulls_seq LIKE AllTypesAgg_seq;

DROP TABLE IF EXISTS AllTypesAggNoNulls_seq_def;
CREATE TABLE AllTypesAggNoNulls_seq_def LIKE AllTypesAgg_seq;

DROP TABLE IF EXISTS AllTypesAggNoNulls_seq_gzip;
CREATE TABLE AllTypesAggNoNulls_seq_gzip LIKE AllTypesAgg_seq;

DROP TABLE IF EXISTS AllTypesAggNoNulls_seq_bzip;
CREATE TABLE AllTypesAggNoNulls_seq_bzip LIKE AllTypesAgg_seq;

DROP TABLE IF EXISTS AllTypesAggNoNulls_seq_snap;
CREATE TABLE AllTypesAggNoNulls_seq_snap LIKE AllTypesAgg_seq;

DROP TABLE IF EXISTS AllTypesAggNoNulls_seq_record_def;
CREATE TABLE AllTypesAggNoNulls_seq_record_def LIKE AllTypesAgg_seq;

DROP TABLE IF EXISTS AllTypesAggNoNulls_seq_record_gzip;
CREATE TABLE AllTypesAggNoNulls_seq_record_gzip LIKE AllTypesAgg_seq;

DROP TABLE IF EXISTS AllTypesAggNoNulls_seq_record_bzip;
CREATE TABLE AllTypesAggNoNulls_seq_record_bzip LIKE AllTypesAgg_seq;

DROP TABLE IF EXISTS AllTypesAggNoNulls_seq_record_snap;
CREATE TABLE AllTypesAggNoNulls_seq_record_snap LIKE AllTypesAgg_seq;

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

DROP TABLE IF EXISTS TestTbl_seq;
CREATE TABLE TestTbl_seq (
  id bigint,
  name string,
  zip int)
STORED AS SEQUENCEFILE;

DROP TABLE IF EXISTS TestTbl_seq_def;
CREATE TABLE TestTbl_seq_def LIKE TestTbl_seq;

DROP TABLE IF EXISTS TestTbl_seq_gzip;
CREATE TABLE TestTbl_seq_gzip LIKE TestTbl_seq;

DROP TABLE IF EXISTS TestTbl_seq_bzip;
CREATE TABLE TestTbl_seq_bzip LIKE TestTbl_seq;

DROP TABLE IF EXISTS TestTbl_seq_snap;
CREATE TABLE TestTbl_seq_snap LIKE TestTbl_seq;

DROP TABLE IF EXISTS TestTbl_seq_record_def;
CREATE TABLE TestTbl_seq_record_def LIKE TestTbl_seq;

DROP TABLE IF EXISTS TestTbl_seq_record_gzip;
CREATE TABLE TestTbl_seq_record_gzip LIKE TestTbl_seq;

DROP TABLE IF EXISTS TestTbl_seq_record_bzip;
CREATE TABLE TestTbl_seq_record_bzip LIKE TestTbl_seq;

DROP TABLE IF EXISTS TestTbl_seq_record_snap;
CREATE TABLE TestTbl_seq_record_snap LIKE TestTbl_seq;

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
  string_col string,
  timestamp_col timestamp)
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
  string_col string,
  timestamp_col timestamp)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
  "hbase.columns.mapping" =
  ":key,bools:bool_col,ints:tinyint_col,ints:smallint_col,ints:int_col,ints:bigint_col,floats:float_col,floats:double_col,strings:date_string_col,strings:string_col,strings:timestamp_col"
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
  string_col string,
  timestamp_col timestamp)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
  "hbase.columns.mapping" =
  ":key,bools:bool_col,ints:tinyint_col,ints:smallint_col,ints:int_col,ints:bigint_col,floats:float_col,floats:double_col,strings:date_string_col,strings:string_col,strings:timestamp_col"
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
  string_col string,
  timestamp_col timestamp)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
  "hbase.columns.mapping" =
  ":key,bools:bool_col,ints:tinyint_col,ints:smallint_col,ints:int_col,ints:bigint_col,floats:float_col,floats:double_col,strings:date_string_col,strings:string_col,strings:timestamp_col"
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
  string_col string,
  timestamp_col timestamp)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
  "hbase.columns.mapping" =
  ":key,bools:bool_col,ints:tinyint_col,ints:smallint_col,ints:int_col,ints:bigint_col,floats:float_col,floats:double_col,strings:date_string_col,strings:string_col,strings:timestamp_col"
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
  string_col string,
  timestamp_col timestamp)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
  "hbase.columns.mapping" =
  ":key,bools:bool_col,ints:tinyint_col,ints:smallint_col,ints:int_col,ints:bigint_col,floats:float_col,floats:double_col,strings:date_string_col,strings:string_col,strings:timestamp_col"
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

DROP TABLE IF EXISTS EmptyTable;
CREATE TABLE EmptyTable (
  field string)
partitioned by (f2 int);

DROP TABLE IF EXISTS AllTypesAggMultiFiles;
CREATE TABLE AllTypesAggMultiFiles (
  id int,
  bool_col boolean,
  tinyint_col tinyint,
  smallint_col smallint,
  int_col int,
  bigint_col bigint,
  float_col float,
  double_col double,
  date_string_col string,
  string_col string,
  timestamp_col timestamp)
partitioned by (year int, month int, day int);

DROP TABLE IF EXISTS AllTypesAggMultiFiles_text;
CREATE TABLE AllTypesAggMultiFiles_text (
  id int,
  bool_col boolean,
  tinyint_col tinyint,
  smallint_col smallint,
  int_col int,
  bigint_col bigint,
  float_col float,
  double_col double,
  date_string_col string,
  string_col string,
  timestamp_col timestamp)
partitioned by (year int, month int, day int)
row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS AllTypesAggMultiFiles_rc;
CREATE TABLE AllTypesAggMultiFiles_rc (
  id int,
  bool_col boolean,
  tinyint_col tinyint,
  smallint_col smallint,
  int_col int,
  bigint_col bigint,
  float_col float,
  double_col double,
  date_string_col string,
  string_col string,
  timestamp_col timestamp)
partitioned by (year int, month int, day int)
STORED AS RCFILE;

DROP TABLE IF EXISTS AllTypesAggMultiFiles_seq;
CREATE TABLE AllTypesAggMultiFiles_seq (
  id int,
  bool_col boolean,
  tinyint_col tinyint,
  smallint_col smallint,
  int_col int,
  bigint_col bigint,
  float_col float,
  double_col double,
  date_string_col string,
  string_col string,
  timestamp_col timestamp)
partitioned by (year int, month int, day int)
STORED AS SEQUENCEFILE;

DROP TABLE IF EXISTS AllTypesAggMultiFiles_seq_def;
CREATE TABLE AllTypesAggMultiFiles_seq_def (
  id int,
  bool_col boolean,
  tinyint_col tinyint,
  smallint_col smallint,
  int_col int,
  bigint_col bigint,
  float_col float,
  double_col double,
  date_string_col string,
  string_col string,
  timestamp_col timestamp)
partitioned by (year int, month int, day int)
STORED AS SEQUENCEFILE;

DROP TABLE IF EXISTS AllTypesAggMultiFiles_seq_gzip;
CREATE TABLE AllTypesAggMultiFiles_seq_gzip (
  id int,
  bool_col boolean,
  tinyint_col tinyint,
  smallint_col smallint,
  int_col int,
  bigint_col bigint,
  float_col float,
  double_col double,
  date_string_col string,
  string_col string,
  timestamp_col timestamp)
partitioned by (year int, month int, day int)
STORED AS SEQUENCEFILE;

DROP TABLE IF EXISTS AllTypesAggMultiFiles_seq_bzip;
CREATE TABLE AllTypesAggMultiFiles_seq_bzip (
  id int,
  bool_col boolean,
  tinyint_col tinyint,
  smallint_col smallint,
  int_col int,
  bigint_col bigint,
  float_col float,
  double_col double,
  date_string_col string,
  string_col string,
  timestamp_col timestamp)
partitioned by (year int, month int, day int)
STORED AS SEQUENCEFILE;

DROP TABLE IF EXISTS AllTypesAggMultiFiles_seq_snap;
CREATE TABLE AllTypesAggMultiFiles_seq_snap (
  id int,
  bool_col boolean,
  tinyint_col tinyint,
  smallint_col smallint,
  int_col int,
  bigint_col bigint,
  float_col float,
  double_col double,
  date_string_col string,
  string_col string,
  timestamp_col timestamp)
partitioned by (year int, month int, day int)
STORED AS SEQUENCEFILE;

DROP TABLE IF EXISTS AllTypesAggMultiFiles_seq_record_def;
CREATE TABLE AllTypesAggMultiFiles_seq_record_def (
  id int,
  bool_col boolean,
  tinyint_col tinyint,
  smallint_col smallint,
  int_col int,
  bigint_col bigint,
  float_col float,
  double_col double,
  date_string_col string,
  string_col string,
  timestamp_col timestamp)
partitioned by (year int, month int, day int)
STORED AS SEQUENCEFILE;

DROP TABLE IF EXISTS AllTypesAggMultiFiles_seq_record_gzip;
CREATE TABLE AllTypesAggMultiFiles_seq_record_gzip (
  id int,
  bool_col boolean,
  tinyint_col tinyint,
  smallint_col smallint,
  int_col int,
  bigint_col bigint,
  float_col float,
  double_col double,
  date_string_col string,
  string_col string,
  timestamp_col timestamp)
partitioned by (year int, month int, day int)
STORED AS SEQUENCEFILE;

DROP TABLE IF EXISTS AllTypesAggMultiFiles_seq_record_bzip;
CREATE TABLE AllTypesAggMultiFiles_seq_record_bzip (
  id int,
  bool_col boolean,
  tinyint_col tinyint,
  smallint_col smallint,
  int_col int,
  bigint_col bigint,
  float_col float,
  double_col double,
  date_string_col string,
  string_col string,
  timestamp_col timestamp)
partitioned by (year int, month int, day int)
STORED AS SEQUENCEFILE;

DROP TABLE IF EXISTS AllTypesAggMultiFiles_seq_record_snap;
CREATE TABLE AllTypesAggMultiFiles_seq_record_snap (
  id int,
  bool_col boolean,
  tinyint_col tinyint,
  smallint_col smallint,
  int_col int,
  bigint_col bigint,
  float_col float,
  double_col double,
  date_string_col string,
  string_col string,
  timestamp_col timestamp)
partitioned by (year int, month int, day int)
STORED AS SEQUENCEFILE;

DROP TABLE IF EXISTS AllTypesAggMultiFilesNoPart;
CREATE TABLE AllTypesAggMultiFilesNoPart (
  id int,
  bool_col boolean,
  tinyint_col tinyint,
  smallint_col smallint,
  int_col int,
  bigint_col bigint,
  float_col float,
  double_col double,
  date_string_col string,
  string_col string,
  timestamp_col timestamp);

DROP TABLE IF EXISTS AllTypesAggMultiFilesNoPart_text;
CREATE TABLE AllTypesAggMultiFilesNoPart_text (
  id int,
  bool_col boolean,
  tinyint_col tinyint,
  smallint_col smallint,
  int_col int,
  bigint_col bigint,
  float_col float,
  double_col double,
  date_string_col string,
  string_col string,
  timestamp_col timestamp)
row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS AllTypesAggMultiFilesNoPart_rc;
CREATE TABLE AllTypesAggMultiFilesNoPart_rc (
  id int,
  bool_col boolean,
  tinyint_col tinyint,
  smallint_col smallint,
  int_col int,
  bigint_col bigint,
  float_col float,
  double_col double,
  date_string_col string,
  string_col string,
  timestamp_col timestamp)
STORED AS RCFILE;

DROP TABLE IF EXISTS AllTypesAggMultiFilesNoPart_seq;
CREATE TABLE AllTypesAggMultiFilesNoPart_seq (
  id int,
  bool_col boolean,
  tinyint_col tinyint,
  smallint_col smallint,
  int_col int,
  bigint_col bigint,
  float_col float,
  double_col double,
  date_string_col string,
  string_col string,
  timestamp_col timestamp)
STORED AS SEQUENCEFILE;

DROP TABLE IF EXISTS AllTypesAggMultiFilesNoPart_seq_def;
CREATE TABLE AllTypesAggMultiFilesNoPart_seq_def (
  id int,
  bool_col boolean,
  tinyint_col tinyint,
  smallint_col smallint,
  int_col int,
  bigint_col bigint,
  float_col float,
  double_col double,
  date_string_col string,
  string_col string,
  timestamp_col timestamp)
STORED AS SEQUENCEFILE;

DROP TABLE IF EXISTS AllTypesAggMultiFilesNoPart_seq_gzip;
CREATE TABLE AllTypesAggMultiFilesNoPart_seq_gzip (
  id int,
  bool_col boolean,
  tinyint_col tinyint,
  smallint_col smallint,
  int_col int,
  bigint_col bigint,
  float_col float,
  double_col double,
  date_string_col string,
  string_col string,
  timestamp_col timestamp)
STORED AS SEQUENCEFILE;

DROP TABLE IF EXISTS AllTypesAggMultiFilesNoPart_seq_bzip;
CREATE TABLE AllTypesAggMultiFilesNoPart_seq_bzip (
  id int,
  bool_col boolean,
  tinyint_col tinyint,
  smallint_col smallint,
  int_col int,
  bigint_col bigint,
  float_col float,
  double_col double,
  date_string_col string,
  string_col string,
  timestamp_col timestamp)
STORED AS SEQUENCEFILE;

DROP TABLE IF EXISTS AllTypesAggMultiFilesNoPart_seq_snap;
CREATE TABLE AllTypesAggMultiFilesNoPart_seq_snap (
  id int,
  bool_col boolean,
  tinyint_col tinyint,
  smallint_col smallint,
  int_col int,
  bigint_col bigint,
  float_col float,
  double_col double,
  date_string_col string,
  string_col string,
  timestamp_col timestamp)
STORED AS SEQUENCEFILE;

DROP TABLE IF EXISTS AllTypesAggMultiFilesNoPart_seq_record_def;
CREATE TABLE AllTypesAggMultiFilesNoPart_seq_record_def (
  id int,
  bool_col boolean,
  tinyint_col tinyint,
  smallint_col smallint,
  int_col int,
  bigint_col bigint,
  float_col float,
  double_col double,
  date_string_col string,
  string_col string,
  timestamp_col timestamp)
STORED AS SEQUENCEFILE;

DROP TABLE IF EXISTS AllTypesAggMultiFilesNoPart_seq_record_gzip;
CREATE TABLE AllTypesAggMultiFilesNoPart_seq_record_gzip (
  id int,
  bool_col boolean,
  tinyint_col tinyint,
  smallint_col smallint,
  int_col int,
  bigint_col bigint,
  float_col float,
  double_col double,
  date_string_col string,
  string_col string,
  timestamp_col timestamp)
STORED AS SEQUENCEFILE;

DROP TABLE IF EXISTS AllTypesAggMultiFilesNoPart_seq_record_bzip;
CREATE TABLE AllTypesAggMultiFilesNoPart_seq_record_bzip (
  id int,
  bool_col boolean,
  tinyint_col tinyint,
  smallint_col smallint,
  int_col int,
  bigint_col bigint,
  float_col float,
  double_col double,
  date_string_col string,
  string_col string,
  timestamp_col timestamp)
STORED AS SEQUENCEFILE;

DROP TABLE IF EXISTS AllTypesAggMultiFilesNoPart_seq_record_snap;
CREATE TABLE AllTypesAggMultiFilesNoPart_seq_record_snap (
  id int,
  bool_col boolean,
  tinyint_col tinyint,
  smallint_col smallint,
  int_col int,
  bigint_col bigint,
  float_col float,
  double_col double,
  date_string_col string,
  string_col string,
  timestamp_col timestamp)
STORED AS SEQUENCEFILE;
