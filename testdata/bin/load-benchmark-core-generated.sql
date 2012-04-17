SET hive.exec.compress.output=true;
SET mapred.output.compression.type=BLOCK;
SET mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.dynamic.partition=true;
FROM grep1GB_text_none INSERT OVERWRITE TABLE grep1GB_sequence_file_snappy PARTITION(chunk) SELECT *;

SET hive.exec.compress.output=true;
SET mapred.output.compression.type=BLOCK;
SET mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.dynamic.partition=true;
FROM UserVisits_web_text_none INSERT OVERWRITE TABLE UserVisits_web_sequence_file_snappy SELECT *;
FROM Rankings_web_text_none INSERT OVERWRITE TABLE Rankings_web_sequence_file_snappy SELECT *;

SET hive.exec.compress.output=false;

SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.dynamic.partition=true;
FROM UserVisits_web_text_none INSERT OVERWRITE TABLE UserVisits_web_sequence_file_none SELECT *;
FROM Rankings_web_text_none INSERT OVERWRITE TABLE Rankings_web_sequence_file_none SELECT *;
FROM grep1GB_text_none INSERT OVERWRITE TABLE grep1GB_rc_file_none PARTITION(chunk) SELECT *;

SET hive.exec.compress.output=false;

SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.dynamic.partition=true;
FROM UserVisits_web_text_none INSERT OVERWRITE TABLE UserVisits_web_rc_file_none SELECT *;
FROM Rankings_web_text_none INSERT OVERWRITE TABLE Rankings_web_rc_file_none SELECT *;

