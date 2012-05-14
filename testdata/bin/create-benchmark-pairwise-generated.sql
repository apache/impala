DROP TABLE IF EXISTS grep10GB_sequence_file_none;
CREATE TABLE grep10GB_sequence_file_none (field string) partitioned by (chunk int) stored as sequencefile;

DROP TABLE IF EXISTS Rankings_web_sequence_file_default;
CREATE TABLE Rankings_web_sequence_file_default (
  pageRank int,
  pageURL string,
  avgDuration int)
row format delimited fields terminated by '|' stored as sequencefile;

DROP TABLE IF EXISTS UserVisits_web_sequence_file_default;
CREATE TABLE UserVisits_web_sequence_file_default (
  sourceIP string,
  destURL string,
  visitDate string,
  adRevenue float,
  userAgent string,
  cCode string,
  lCode string,
  sKeyword string,
  avgTimeOnSite int)
row format delimited fields terminated by '|'  stored as sequencefile;

DROP TABLE IF EXISTS grep1GB_sequence_file_gzip;
CREATE TABLE grep1GB_sequence_file_gzip (field string) partitioned by (chunk int) stored as sequencefile;

DROP TABLE IF EXISTS grep10GB_sequence_file_bzip2;
CREATE TABLE grep10GB_sequence_file_bzip2 (field string) partitioned by (chunk int) stored as sequencefile;

DROP TABLE IF EXISTS grep10GB_sequence_file_snappy;
CREATE TABLE grep10GB_sequence_file_snappy (field string) partitioned by (chunk int) stored as sequencefile;

