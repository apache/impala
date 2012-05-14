SET hive.exec.compress.output=false;

SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.dynamic.partition=true;
FROM grep10GB_text_none INSERT OVERWRITE TABLE grep10GB_sequence_file_none PARTITION(chunk) SELECT *;

SET hive.exec.compress.output=true;
SET mapred.output.compression.type=BLOCK;
SET mapred.output.compression.codec=org.apache.hadoop.io.compress.DefaultCodec;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.dynamic.partition=true;
FROM UserVisits_web_text_none INSERT OVERWRITE TABLE UserVisits_web_sequence_file_default SELECT *;
FROM Rankings_web_text_none INSERT OVERWRITE TABLE Rankings_web_sequence_file_default SELECT *;

SET hive.exec.compress.output=true;
SET mapred.output.compression.type=BLOCK;
SET mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.dynamic.partition=true;
FROM grep1GB_text_none INSERT OVERWRITE TABLE grep1GB_sequence_file_gzip PARTITION(chunk) SELECT *;

SET hive.exec.compress.output=true;
SET mapred.output.compression.type=BLOCK;
SET mapred.output.compression.codec=org.apache.hadoop.io.compress.BZip2Codec;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.dynamic.partition=true;
FROM grep10GB_text_none INSERT OVERWRITE TABLE grep10GB_sequence_file_bzip2 PARTITION(chunk) SELECT *;

SET hive.exec.compress.output=true;
SET mapred.output.compression.type=BLOCK;
SET mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.dynamic.partition=true;
FROM grep10GB_text_none INSERT OVERWRITE TABLE grep10GB_sequence_file_snappy PARTITION(chunk) SELECT *;

