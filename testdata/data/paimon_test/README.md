# Paimon Test Tables

This README.md explain the schema and usage of paimon test tables in the folder.
the table data in the folder will be directly copied to hdfs and external paimon
tables will be created accordingly for these tables.

## paimon_non_partitioned
The subset of table ratings in movielens dataset, it is not partitioned.
the schema is:
+-----------+--------+---------+
| userid    | int    |         |
| movieid   | int    |         |
| rating    | float  |         |
| timestamp | bigint |         |
--------------------------------
## paimon_partitioned
The subset of table ratings in movielens dataset, it is partitioned.
the schema is:
+-----------+--------+---------+
| userid    | int    |         |
| movieid   | int    |         |
| rating    | float  |         |
| timestamp | bigint |         |
--------------------------------
## paimon_primitive_alltypes
The table contains all primitive types supported by paimon:
schema is:
+---------------+---------------+---------+
| name          | type          | comment |
+---------------+---------------+---------+
| bool_value    | boolean       |         |
| tiny_value    | tinyint       |         |
| small_value   | smallint      |         |
| int_value     | int           |         |
| big_value     | bigint        |         |
| float_value   | float         |         |
| double_value  | double        |         |
| decimal_value | decimal(10,2) |         |
| char_value    | char(10)      |         |
| varchar_value | varchar(100)  |         |
| binary_value  | binary        |         |
| date_value    | date          |         |
| ts_ltz_value  | timestamp     |         |
| ts_value      | timestamp     |         |
+---------------+---------------+---------+
## paimon_decimal_tbl
The table is used to support decimal related test,with various precision and scale.
the schema is:
+------+----------------+---------+
| name | type           | comment |
+------+----------------+---------+
| d1   | decimal(9,0)   |         |
| d2   | decimal(10,0)  |         |
| d3   | decimal(20,10) |         |
| d4   | decimal(38,38) |         |
| d5   | decimal(10,5)  |         |
+------+----------------+---------+
## paimon_decimal_tbl
The table is used to support decimal related test,with various precision and scale.
the schema is:
+------+----------------+---------+
| name | type           | comment |
+------+----------------+---------+
| d1   | decimal(9,0)   |         |
| d2   | decimal(10,0)  |         |
| d3   | decimal(20,10) |         |
| d4   | decimal(38,38) |         |
| d5   | decimal(10,5)  |         |
+------+----------------+---------+
## alltypes_paimon
the table is table alltypes with paimon format, it is used to support test test_scanner
for paimon format.
the schema is:
+-----------------+-----------+---------------+
| name            | type      | comment       |
+-----------------+-----------+---------------+
| id              | int       | Add a comment |
| bool_col        | boolean   |               |
| tinyint_col     | tinyint   |               |
| smallint_col    | smallint  |               |
| int_col         | int       |               |
| bigint_col      | bigint    |               |
| float_col       | float     |               |
| double_col      | double    |               |
| date_string_col | string    |               |
| string_col      | string    |               |
| timestamp_col   | timestamp |               |
| year            | int       |               |
| month           | int       |               |
+-----------------+-----------+---------------+
## alltypes_structs_paimon
the table is table alltypes_structs with paimon format, it is used to support negative
cases for complex and nested field query suuport.
+------------+---------------------------+---------+
| name       | type                      | comment |
+------------+---------------------------+---------+
| id         | int                       |         |
| struct_val | struct<                   |         |
|            |   bool_col:boolean,       |         |
|            |   tinyint_col:tinyint,    |         |
|            |   smallint_col:smallint,  |         |
|            |   int_col:int,            |         |
|            |   bigint_col:bigint,      |         |
|            |   float_col:float,        |         |
|            |   double_col:double,      |         |
|            |   date_string_col:string, |         |
|            |   string_col:string       |         |
|            | >                         |         |
| year       | int                       |         |
| month      | int                       |         |
----------------------------------------------------
## TODO:
Most of testing tables should be removed later once paimon write is supported
for impala, if these table generation can be easily implemented using DML or
CTAS statement.