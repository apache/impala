--Generated file - It is not recommended to edit this file directly.
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
FROM UserVisits_web_text_none INSERT OVERWRITE TABLE UserVisits_web_sequence_file_snappy SELECT *;
FROM Rankings_web_text_none INSERT OVERWRITE TABLE Rankings_web_sequence_file_snappy SELECT *;

