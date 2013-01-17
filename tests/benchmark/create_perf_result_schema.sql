--  Copyright (c) 2012 Cloudera, Inc. All rights reserved.
-- This is a script that creates the perf database schema and populates
-- some of the lookup data.

DROP DATABASE IF EXISTS perf_results;
CREATE DATABASE perf_results;

USE perf_results;

DROP TABLE IF EXISTS ExecutionResults;
CREATE TABLE ExecutionResults (
     result_id BIGINT NOT NULL AUTO_INCREMENT,
     run_info_id BIGINT NOT NULL,
     query_id BIGINT NOT NULL,
     workload_id BIGINT NOT NULL,
     file_type_id BIGINT NOT NULL,
     num_clients INT NOT NULL DEFAULT 1,
     cluster_name char(255),
     executor_name char(255),
     avg_time double NULL,
     stddev double NULL,
     run_date DATETIME,
     version char(255),
     notes TEXT,
     is_official BOOLEAN DEFAULT FALSE, -- Is this an official result
     PRIMARY KEY (result_id)
);

DROP TABLE IF EXISTS Query;
CREATE TABLE Query (
     query_id BIGINT NOT NULL AUTO_INCREMENT,
     name TEXT,
     query TEXT,
     notes TEXT,
     PRIMARY KEY (query_id)
);

DROP TABLE IF EXISTS Workload;
CREATE TABLE Workload (
     workload_id BIGINT NOT NULL AUTO_INCREMENT,
     name char(255),
     scale_factor char(255),
     PRIMARY KEY (workload_id)
);


DROP TABLE IF EXISTS FileType;
CREATE TABLE FileType (
     file_type_id BIGINT NOT NULL AUTO_INCREMENT,
     format char(255),
     compression_codec char(255),
     compression_type char(255),
     PRIMARY KEY (file_type_id)
);

DROP TABLE IF EXISTS RunInfo;
CREATE TABLE RunInfo (
     run_info_id BIGINT NOT NULL AUTO_INCREMENT,
     run_info char(255),
     PRIMARY KEY (run_info_id)
);

-- Populate valid file formats
INSERT INTO FileType (format, compression_codec, compression_type) VALUES('text', 'none', 'none');
INSERT INTO FileType (format, compression_codec, compression_type) VALUES('seq', 'none', 'none');
INSERT INTO FileType (format, compression_codec, compression_type) VALUES('rc', 'none', 'none');
INSERT INTO FileType (format, compression_codec, compression_type) VALUES('parquet', 'none', 'none');

INSERT INTO FileType (format, compression_codec, compression_type) VALUES('seq', 'snap', 'block');
INSERT INTO FileType (format, compression_codec, compression_type) VALUES('seq', 'gzip', 'block');
INSERT INTO FileType (format, compression_codec, compression_type) VALUES('seq', 'def', 'block');

INSERT INTO FileType (format, compression_codec, compression_type) VALUES('seq', 'snap', 'record');
INSERT INTO FileType (format, compression_codec, compression_type) VALUES('seq', 'gzip', 'record');
INSERT INTO FileType (format, compression_codec, compression_type) VALUES('seq', 'def', 'record');

INSERT INTO FileType (format, compression_codec, compression_type) VALUES('rc', 'snap', 'block');
INSERT INTO FileType (format, compression_codec, compression_type) VALUES('rc', 'gzip', 'block');
INSERT INTO FileType (format, compression_codec, compression_type) VALUES('rc', 'def', 'block');


-- Populate known workloads
INSERT INTO Workload (name, scale_factor) VALUES('tpch', '');
INSERT INTO Workload (name, scale_factor) VALUES('tpch', '1000gb');
INSERT INTO Workload (name, scale_factor) VALUES('tpch', '10000gb');


-- Populate known queries
insert into Query (name, query) values ('TPCH-Q3', '');
insert into Query (name, query) values ('TPCH-Q17_QUERY_1', '');
insert into Query (name, query) values ('TPCH-Q17_QUERY_2', '');
insert into Query (name, query) values ('TPCH-Q19', '');
insert into Query (name, query) values ('TPCH-Q12', '');
insert into Query (name, query) values ('TPCH-Q11_QUERY_1', '');
insert into Query (name, query) values ('TPCH-Q11_QUERY_2', '');
insert into Query (name, query) values ('TPCH-Q22_QUERY_1', '');
insert into Query (name, query) values ('TPCH-Q22_QUERY_2', '');
insert into Query (name, query) values ('TPCH-Q16_QUERY_1', '');
insert into Query (name, query) values ('TPCH-Q16_QUERY_2', '');
insert into Query (name, query) values ('TPCH-Q16_QUERY_3', '');
insert into Query (name, query) values ('TPCH-Q13', '');
insert into Query (name, query) values ('TPCH-Q21', '');
insert into Query (name, query) values ('TPCH-Q9', '');
insert into Query (name, query) values ('TPCH-Q2_QUERY_1', '');
insert into Query (name, query) values ('TPCH-Q2_QUERY_2', '');
insert into Query (name, query) values ('TPCH-Q2_QUERY_3', '');
insert into Query (name, query) values ('TPCH-Q20_QUERY_1', '');
insert into Query (name, query) values ('TPCH-Q20_QUERY_2', '');
insert into Query (name, query) values ('TPCH-Q20_QUERY_3', '');
insert into Query (name, query) values ('TPCH-Q20_QUERY_4', '');
insert into Query (name, query) values ('TPCH-Q20_QUERY_5', '');
insert into Query (name, query) values ('TPCH-Q4', '');
insert into Query (name, query) values ('TPCH-Q8', '');
insert into Query (name, query) values ('TPCH-Q1', '');
insert into Query (name, query) values ('TPCH-Q18_QUERY_1', '');
insert into Query (name, query) values ('TPCH-Q18_QUERY_2', '');
insert into Query (name, query) values ('TPCH-Q14', '');
insert into Query (name, query) values ('TPCH-Q10', '');
insert into Query (name, query) values ('TPCH-Q5', '');
insert into Query (name, query) values ('TPCH-Q15_QUERY_1', '');
insert into Query (name, query) values ('TPCH-Q15_QUERY_2', '');
insert into Query (name, query) values ('TPCH-Q15_QUERY_3', '');
insert into Query (name, query) values ('TPCH-Q6', '');
