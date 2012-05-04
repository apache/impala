LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/hive_benchmark/grep1GB/part-00000' OVERWRITE INTO TABLE Grep1GB_text_none PARTITION(chunk=0);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/hive_benchmark/grep1GB/part-00001' OVERWRITE INTO TABLE Grep1GB_text_none PARTITION(chunk=1);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/hive_benchmark/grep1GB/part-00002' OVERWRITE INTO TABLE Grep1GB_text_none PARTITION(chunk=2);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/hive_benchmark/grep1GB/part-00003' OVERWRITE INTO TABLE Grep1GB_text_none PARTITION(chunk=3);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/hive_benchmark/grep1GB/part-00004' OVERWRITE INTO TABLE Grep1GB_text_none PARTITION(chunk=4);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/hive_benchmark/grep1GB/part-00005' OVERWRITE INTO TABLE Grep1GB_text_none PARTITION(chunk=5);

LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/hive_benchmark/grep10GB/part-00000' OVERWRITE INTO TABLE Grep10GB_text_none PARTITION(chunk=0);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/hive_benchmark/grep10GB/part-00001' OVERWRITE INTO TABLE Grep10GB_text_none PARTITION(chunk=1);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/hive_benchmark/grep10GB/part-00002' OVERWRITE INTO TABLE Grep10GB_text_none PARTITION(chunk=2);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/hive_benchmark/grep10GB/part-00003' OVERWRITE INTO TABLE Grep10GB_text_none PARTITION(chunk=3);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/hive_benchmark/grep10GB/part-00004' OVERWRITE INTO TABLE Grep10GB_text_none PARTITION(chunk=4);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/hive_benchmark/grep10GB/part-00005' OVERWRITE INTO TABLE Grep10GB_text_none PARTITION(chunk=5);

LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/hive_benchmark/html1GB/Rankings.dat' OVERWRITE INTO TABLE Rankings_web_text_none;
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/hive_benchmark/html1GB/UserVisits.dat' OVERWRITE INTO TABLE UserVisits_web_text_none;
