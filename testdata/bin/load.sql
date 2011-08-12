LOAD DATA LOCAL INPATH '../testdata/target/AllTypes/090101.txt' OVERWRITE INTO TABLE AllTypes PARTITION(year=2009, month=1);
LOAD DATA LOCAL INPATH '../testdata/target/AllTypes/090201.txt' OVERWRITE INTO TABLE AllTypes PARTITION(year=2009, month=2);
LOAD DATA LOCAL INPATH '../testdata/target/AllTypes/090301.txt' OVERWRITE INTO TABLE AllTypes PARTITION(year=2009, month=3);
LOAD DATA LOCAL INPATH '../testdata/target/AllTypes/090401.txt' OVERWRITE INTO TABLE AllTypes PARTITION(year=2009, month=4);
LOAD DATA LOCAL INPATH '../testdata/target/AllTypes/090501.txt' OVERWRITE INTO TABLE AllTypes PARTITION(year=2009, month=5);
LOAD DATA LOCAL INPATH '../testdata/target/AllTypes/090601.txt' OVERWRITE INTO TABLE AllTypes PARTITION(year=2009, month=6);
LOAD DATA LOCAL INPATH '../testdata/target/AllTypes/090701.txt' OVERWRITE INTO TABLE AllTypes PARTITION(year=2009, month=7);
LOAD DATA LOCAL INPATH '../testdata/target/AllTypes/090801.txt' OVERWRITE INTO TABLE AllTypes PARTITION(year=2009, month=8);
LOAD DATA LOCAL INPATH '../testdata/target/AllTypes/090901.txt' OVERWRITE INTO TABLE AllTypes PARTITION(year=2009, month=9);
LOAD DATA LOCAL INPATH '../testdata/target/AllTypes/091001.txt' OVERWRITE INTO TABLE AllTypes PARTITION(year=2009, month=10);
LOAD DATA LOCAL INPATH '../testdata/target/AllTypes/091101.txt' OVERWRITE INTO TABLE AllTypes PARTITION(year=2009, month=11);
LOAD DATA LOCAL INPATH '../testdata/target/AllTypes/091201.txt' OVERWRITE INTO TABLE AllTypes PARTITION(year=2009, month=12);
LOAD DATA LOCAL INPATH '../testdata/target/AllTypes/100101.txt' OVERWRITE INTO TABLE AllTypes PARTITION(year=2010, month=1);
LOAD DATA LOCAL INPATH '../testdata/target/AllTypes/100201.txt' OVERWRITE INTO TABLE AllTypes PARTITION(year=2010, month=2);
LOAD DATA LOCAL INPATH '../testdata/target/AllTypes/100301.txt' OVERWRITE INTO TABLE AllTypes PARTITION(year=2010, month=3);
LOAD DATA LOCAL INPATH '../testdata/target/AllTypes/100401.txt' OVERWRITE INTO TABLE AllTypes PARTITION(year=2010, month=4);
LOAD DATA LOCAL INPATH '../testdata/target/AllTypes/100501.txt' OVERWRITE INTO TABLE AllTypes PARTITION(year=2010, month=5);
LOAD DATA LOCAL INPATH '../testdata/target/AllTypes/100601.txt' OVERWRITE INTO TABLE AllTypes PARTITION(year=2010, month=6);
LOAD DATA LOCAL INPATH '../testdata/target/AllTypes/100701.txt' OVERWRITE INTO TABLE AllTypes PARTITION(year=2010, month=7);
LOAD DATA LOCAL INPATH '../testdata/target/AllTypes/100801.txt' OVERWRITE INTO TABLE AllTypes PARTITION(year=2010, month=8);
LOAD DATA LOCAL INPATH '../testdata/target/AllTypes/100901.txt' OVERWRITE INTO TABLE AllTypes PARTITION(year=2010, month=9);
LOAD DATA LOCAL INPATH '../testdata/target/AllTypes/101001.txt' OVERWRITE INTO TABLE AllTypes PARTITION(year=2010, month=10);
LOAD DATA LOCAL INPATH '../testdata/target/AllTypes/101101.txt' OVERWRITE INTO TABLE AllTypes PARTITION(year=2010, month=11);
LOAD DATA LOCAL INPATH '../testdata/target/AllTypes/101201.txt' OVERWRITE INTO TABLE AllTypes PARTITION(year=2010, month=12);

LOAD DATA LOCAL INPATH '../testdata/target/AllTypesSmall/090101.txt' OVERWRITE INTO TABLE AllTypesSmall PARTITION(year=2009, month=1);
LOAD DATA LOCAL INPATH '../testdata/target/AllTypesSmall/090201.txt' OVERWRITE INTO TABLE AllTypesSmall PARTITION(year=2009, month=2);
LOAD DATA LOCAL INPATH '../testdata/target/AllTypesSmall/090301.txt' OVERWRITE INTO TABLE AllTypesSmall PARTITION(year=2009, month=3);
LOAD DATA LOCAL INPATH '../testdata/target/AllTypesSmall/090401.txt' OVERWRITE INTO TABLE AllTypesSmall PARTITION(year=2009, month=4);

LOAD DATA LOCAL INPATH '../testdata/AllTypesError/0901.txt' OVERWRITE INTO TABLE AllTypesError PARTITION(year=2009, month=1);
LOAD DATA LOCAL INPATH '../testdata/AllTypesError/0902.txt' OVERWRITE INTO TABLE AllTypesError PARTITION(year=2009, month=2);
LOAD DATA LOCAL INPATH '../testdata/AllTypesError/0903.txt' OVERWRITE INTO TABLE AllTypesError PARTITION(year=2009, month=3);

LOAD DATA LOCAL INPATH '../testdata/AllTypesErrorNoNulls/0901.txt' OVERWRITE INTO TABLE AllTypesErrorNoNulls PARTITION(year=2009, month=1);
LOAD DATA LOCAL INPATH '../testdata/AllTypesErrorNoNulls/0902.txt' OVERWRITE INTO TABLE AllTypesErrorNoNulls PARTITION(year=2009, month=2);
LOAD DATA LOCAL INPATH '../testdata/AllTypesErrorNoNulls/0903.txt' OVERWRITE INTO TABLE AllTypesErrorNoNulls PARTITION(year=2009, month=3);

LOAD DATA LOCAL INPATH '../testdata/target/AllTypesAgg/100101.txt' OVERWRITE INTO TABLE AllTypesAgg PARTITION(year=2010, month=1, day=1);
LOAD DATA LOCAL INPATH '../testdata/target/AllTypesAgg/100102.txt' OVERWRITE INTO TABLE AllTypesAgg PARTITION(year=2010, month=1, day=2);
LOAD DATA LOCAL INPATH '../testdata/target/AllTypesAgg/100103.txt' OVERWRITE INTO TABLE AllTypesAgg PARTITION(year=2010, month=1, day=3);
LOAD DATA LOCAL INPATH '../testdata/target/AllTypesAgg/100104.txt' OVERWRITE INTO TABLE AllTypesAgg PARTITION(year=2010, month=1, day=4);
LOAD DATA LOCAL INPATH '../testdata/target/AllTypesAgg/100105.txt' OVERWRITE INTO TABLE AllTypesAgg PARTITION(year=2010, month=1, day=5);
LOAD DATA LOCAL INPATH '../testdata/target/AllTypesAgg/100106.txt' OVERWRITE INTO TABLE AllTypesAgg PARTITION(year=2010, month=1, day=6);
LOAD DATA LOCAL INPATH '../testdata/target/AllTypesAgg/100107.txt' OVERWRITE INTO TABLE AllTypesAgg PARTITION(year=2010, month=1, day=7);
LOAD DATA LOCAL INPATH '../testdata/target/AllTypesAgg/100108.txt' OVERWRITE INTO TABLE AllTypesAgg PARTITION(year=2010, month=1, day=8);
LOAD DATA LOCAL INPATH '../testdata/target/AllTypesAgg/100109.txt' OVERWRITE INTO TABLE AllTypesAgg PARTITION(year=2010, month=1, day=9);
LOAD DATA LOCAL INPATH '../testdata/target/AllTypesAgg/100110.txt' OVERWRITE INTO TABLE AllTypesAgg PARTITION(year=2010, month=1, day=10);

LOAD DATA LOCAL INPATH '../testdata/target/AllTypesAggNoNulls/100101.txt' OVERWRITE INTO TABLE AllTypesAggNoNulls PARTITION(year=2010, month=1, day=1);
LOAD DATA LOCAL INPATH '../testdata/target/AllTypesAggNoNulls/100102.txt' OVERWRITE INTO TABLE AllTypesAggNoNulls PARTITION(year=2010, month=1, day=2);
LOAD DATA LOCAL INPATH '../testdata/target/AllTypesAggNoNulls/100103.txt' OVERWRITE INTO TABLE AllTypesAggNoNulls PARTITION(year=2010, month=1, day=3);
LOAD DATA LOCAL INPATH '../testdata/target/AllTypesAggNoNulls/100104.txt' OVERWRITE INTO TABLE AllTypesAggNoNulls PARTITION(year=2010, month=1, day=4);
LOAD DATA LOCAL INPATH '../testdata/target/AllTypesAggNoNulls/100105.txt' OVERWRITE INTO TABLE AllTypesAggNoNulls PARTITION(year=2010, month=1, day=5);
LOAD DATA LOCAL INPATH '../testdata/target/AllTypesAggNoNulls/100106.txt' OVERWRITE INTO TABLE AllTypesAggNoNulls PARTITION(year=2010, month=1, day=6);
LOAD DATA LOCAL INPATH '../testdata/target/AllTypesAggNoNulls/100107.txt' OVERWRITE INTO TABLE AllTypesAggNoNulls PARTITION(year=2010, month=1, day=7);
LOAD DATA LOCAL INPATH '../testdata/target/AllTypesAggNoNulls/100108.txt' OVERWRITE INTO TABLE AllTypesAggNoNulls PARTITION(year=2010, month=1, day=8);
LOAD DATA LOCAL INPATH '../testdata/target/AllTypesAggNoNulls/100109.txt' OVERWRITE INTO TABLE AllTypesAggNoNulls PARTITION(year=2010, month=1, day=9);
LOAD DATA LOCAL INPATH '../testdata/target/AllTypesAggNoNulls/100110.txt' OVERWRITE INTO TABLE AllTypesAggNoNulls PARTITION(year=2010, month=1, day=10);

INSERT OVERWRITE TABLE hbasealltypessmall
SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col
FROM alltypessmall;

INSERT OVERWRITE TABLE hbasealltypeserror
SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col
FROM alltypeserror;

INSERT OVERWRITE TABLE hbasealltypeserrornonulls
SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col
FROM alltypeserrornonulls;

INSERT OVERWRITE TABLE hbasealltypesagg
SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col
FROM alltypesagg;
