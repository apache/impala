LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/hive_benchmark/grep1GB/part-00000' OVERWRITE INTO TABLE Grep1GB PARTITION(chunk=0);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/hive_benchmark/grep1GB/part-00001' OVERWRITE INTO TABLE Grep1GB PARTITION(chunk=1);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/hive_benchmark/grep1GB/part-00002' OVERWRITE INTO TABLE Grep1GB PARTITION(chunk=2);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/hive_benchmark/grep1GB/part-00003' OVERWRITE INTO TABLE Grep1GB PARTITION(chunk=3);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/hive_benchmark/grep1GB/part-00004' OVERWRITE INTO TABLE Grep1GB PARTITION(chunk=4);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/hive_benchmark/grep1GB/part-00005' OVERWRITE INTO TABLE Grep1GB PARTITION(chunk=5);

LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/hive_benchmark/grep10GB/part-00000' OVERWRITE INTO TABLE Grep10GB PARTITION(chunk=0);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/hive_benchmark/grep10GB/part-00001' OVERWRITE INTO TABLE Grep10GB PARTITION(chunk=1);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/hive_benchmark/grep10GB/part-00002' OVERWRITE INTO TABLE Grep10GB PARTITION(chunk=2);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/hive_benchmark/grep10GB/part-00003' OVERWRITE INTO TABLE Grep10GB PARTITION(chunk=3);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/hive_benchmark/grep10GB/part-00004' OVERWRITE INTO TABLE Grep10GB PARTITION(chunk=4);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/hive_benchmark/grep10GB/part-00005' OVERWRITE INTO TABLE Grep10GB PARTITION(chunk=5);

LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/hive_benchmark/html1GB/Rankings.dat' OVERWRITE INTO TABLE Rankings;
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/hive_benchmark/html1GB/UserVisits.dat' OVERWRITE INTO TABLE UserVisits;

INSERT OVERWRITE TABLE UserVisits_seq SELECT * from UserVisits;

set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition=true;
SET hive.exec.compress.output=true;
set mapred.output.compression.type=BLOCK;
INSERT OVERWRITE TABLE Grep1GB_seq_snap PARTITION (chunk) select * from Grep1GB;
