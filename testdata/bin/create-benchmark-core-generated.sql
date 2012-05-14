DROP TABLE IF EXISTS grep1GB_sequence_file_snappy;
CREATE TABLE grep1GB_sequence_file_snappy (field string) partitioned by (chunk int) stored as sequencefile;

DROP TABLE IF EXISTS Rankings_web_sequence_file_snappy;
CREATE TABLE Rankings_web_sequence_file_snappy (
  pageRank int,
  pageURL string,
  avgDuration int)
row format delimited fields terminated by '|' stored as sequencefile;

DROP TABLE IF EXISTS UserVisits_web_sequence_file_snappy;
CREATE TABLE UserVisits_web_sequence_file_snappy (
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

DROP TABLE IF EXISTS Rankings_web_sequence_file_none;
CREATE TABLE Rankings_web_sequence_file_none (
  pageRank int,
  pageURL string,
  avgDuration int)
row format delimited fields terminated by '|' stored as sequencefile;

DROP TABLE IF EXISTS UserVisits_web_sequence_file_none;
CREATE TABLE UserVisits_web_sequence_file_none (
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

