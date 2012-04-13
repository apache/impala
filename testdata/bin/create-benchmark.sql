DROP TABLE IF EXISTS Grep1GB;
CREATE TABLE Grep1GB (
  field string)
partitioned by (chunk int);

DROP TABLE IF EXISTS Grep10GB;
CREATE TABLE Grep10GB (
  field string)
partitioned by (chunk int);

DROP TABLE IF EXISTS Rankings;
CREATE TABLE Rankings (
  pageRank int,
  pageURL string,
  avgDuration int)
row format delimited fields terminated by '|'  stored as textfile;

DROP TABLE IF EXISTS UserVisits;
CREATE TABLE UserVisits (
  sourceIP string,
  destURL string,
  visitDate string,
  adRevenue float,
  userAgent string,
  cCode string,
  lCode string,
  sKeyword string,
  avgTimeOnSite int)
row format delimited fields terminated by '|'  stored as textfile;

DROP TABLE IF EXISTS UserVisits_seq;
CREATE TABLE UserVisits_seq (
  sourceIP string,
  destURL string,
  visitDate string,
  adRevenue float,
  userAgent string,
  cCode string,
  lCode string,
  sKeyword string,
  avgTimeOnSite int)
STORED AS SEQUENCEFILE;

DROP TABLE IF EXISTS Grep1GB_seq_snap;
CREATE TABLE Grep1GB_seq_snap (
  field string)
partitioned by (chunk int)
STORED AS SEQUENCEFILE;

DROP TABLE IF EXISTS UserVisits_rc;
CREATE TABLE UserVisits_rc (
  sourceIP string,
  destURL string,
  visitDate string,
  adRevenue float,
  userAgent string,
  cCode string,
  lCode string,
  sKeyword string,
  avgTimeOnSite int)
STORED AS RCFILE;

DROP TABLE IF EXISTS UserVisits_rc_bzip;
CREATE TABLE UserVisits_rc_bzip (
  sourceIP string,
  destURL string,
  visitDate string,
  adRevenue float,
  userAgent string,
  cCode string,
  lCode string,
  sKeyword string,
  avgTimeOnSite int)
STORED AS RCFILE;

DROP TABLE IF EXISTS Grep1GB_rc;
CREATE TABLE Grep1GB_rc (
  field string)
partitioned by (chunk int)
STORED AS RCFILE;
