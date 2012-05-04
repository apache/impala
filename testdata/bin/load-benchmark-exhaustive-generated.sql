--Generated file - It is not recommended to edit this file directly.
SET hive.exec.compress.output=false;

SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.dynamic.partition=true;
FROM grep1GB_text_none INSERT OVERWRITE TABLE grep1GB_sequence_file_none PARTITION(chunk) SELECT *;

SET hive.exec.compress.output=false;

SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.dynamic.partition=true;
FROM grep10GB_text_none INSERT OVERWRITE TABLE grep10GB_sequence_file_none PARTITION(chunk) SELECT *;

SET hive.exec.compress.output=false;

SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.dynamic.partition=true;
FROM UserVisits_web_text_none INSERT OVERWRITE TABLE UserVisits_web_sequence_file_none SELECT *;
FROM Rankings_web_text_none INSERT OVERWRITE TABLE Rankings_web_sequence_file_none SELECT *;

SET hive.exec.compress.output=true;
set mapreduce.output.compression.type=BLOCK;
SET mapreduce.output.compression.codec=com.hadoop.compression.lzo.LzoCodec;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.dynamic.partition=true;
FROM grep1GB_text_none INSERT OVERWRITE TABLE grep1GB_sequence_file_lzip PARTITION(chunk) SELECT *;

SET hive.exec.compress.output=true;
set mapreduce.output.compression.type=BLOCK;
SET mapreduce.output.compression.codec=com.hadoop.compression.lzo.LzoCodec;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.dynamic.partition=true;
FROM grep10GB_text_none INSERT OVERWRITE TABLE grep10GB_sequence_file_lzip PARTITION(chunk) SELECT *;

SET hive.exec.compress.output=true;
set mapreduce.output.compression.type=BLOCK;
SET mapreduce.output.compression.codec=com.hadoop.compression.lzo.LzoCodec;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.dynamic.partition=true;
FROM UserVisits_web_text_none INSERT OVERWRITE TABLE UserVisits_web_sequence_file_lzip SELECT *;
FROM Rankings_web_text_none INSERT OVERWRITE TABLE Rankings_web_sequence_file_lzip SELECT *;

SET hive.exec.compress.output=true;
set mapreduce.output.compression.type=BLOCK;
SET mapreduce.output.compression.codec=com.hadoop.compression.GzipCodec;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.dynamic.partition=true;
FROM grep1GB_text_none INSERT OVERWRITE TABLE grep1GB_sequence_file_gzip PARTITION(chunk) SELECT *;

SET hive.exec.compress.output=true;
set mapreduce.output.compression.type=BLOCK;
SET mapreduce.output.compression.codec=com.hadoop.compression.GzipCodec;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.dynamic.partition=true;
FROM grep10GB_text_none INSERT OVERWRITE TABLE grep10GB_sequence_file_gzip PARTITION(chunk) SELECT *;

SET hive.exec.compress.output=true;
set mapreduce.output.compression.type=BLOCK;
SET mapreduce.output.compression.codec=com.hadoop.compression.GzipCodec;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.dynamic.partition=true;
FROM UserVisits_web_text_none INSERT OVERWRITE TABLE UserVisits_web_sequence_file_gzip SELECT *;
FROM Rankings_web_text_none INSERT OVERWRITE TABLE Rankings_web_sequence_file_gzip SELECT *;

SET hive.exec.compress.output=true;
set mapreduce.output.compression.type=BLOCK;
SET mapreduce.output.compression.codec=com.hadoop.compression.BZip2Codec;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.dynamic.partition=true;
FROM grep1GB_text_none INSERT OVERWRITE TABLE grep1GB_sequence_file_bzip2 PARTITION(chunk) SELECT *;

SET hive.exec.compress.output=true;
set mapreduce.output.compression.type=BLOCK;
SET mapreduce.output.compression.codec=com.hadoop.compression.BZip2Codec;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.dynamic.partition=true;
FROM grep10GB_text_none INSERT OVERWRITE TABLE grep10GB_sequence_file_bzip2 PARTITION(chunk) SELECT *;

SET hive.exec.compress.output=true;
set mapreduce.output.compression.type=BLOCK;
SET mapreduce.output.compression.codec=com.hadoop.compression.BZip2Codec;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.dynamic.partition=true;
FROM UserVisits_web_text_none INSERT OVERWRITE TABLE UserVisits_web_sequence_file_bzip2 SELECT *;
FROM Rankings_web_text_none INSERT OVERWRITE TABLE Rankings_web_sequence_file_bzip2 SELECT *;

SET hive.exec.compress.output=true;
set mapreduce.output.compression.type=BLOCK;
SET mapreduce.output.compression.codec=com.hadoop.compression.SnappyCodec;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.dynamic.partition=true;
FROM grep1GB_text_none INSERT OVERWRITE TABLE grep1GB_sequence_file_snappy PARTITION(chunk) SELECT *;

SET hive.exec.compress.output=true;
set mapreduce.output.compression.type=BLOCK;
SET mapreduce.output.compression.codec=com.hadoop.compression.SnappyCodec;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.dynamic.partition=true;
FROM grep10GB_text_none INSERT OVERWRITE TABLE grep10GB_sequence_file_snappy PARTITION(chunk) SELECT *;

SET hive.exec.compress.output=true;
set mapreduce.output.compression.type=BLOCK;
SET mapreduce.output.compression.codec=com.hadoop.compression.SnappyCodec;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.dynamic.partition=true;
FROM UserVisits_web_text_none INSERT OVERWRITE TABLE UserVisits_web_sequence_file_snappy SELECT *;
FROM Rankings_web_text_none INSERT OVERWRITE TABLE Rankings_web_sequence_file_snappy SELECT *;

