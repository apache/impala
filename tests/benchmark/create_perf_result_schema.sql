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
     num_iterations INT NOT NULL DEFAULT 1,
     cluster_name char(255),
     executor_name char(255),
     avg_time double NULL,
     stddev double NULL,
     run_date DATETIME,
     version char(255),
     notes TEXT,
     profile TEXT, -- The query runtime profile
     is_official BOOLEAN DEFAULT FALSE, -- True if this an official result
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
