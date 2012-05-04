DROP TABLE IF EXISTS Grep1GB_text_none;
CREATE TABLE Grep1GB_text_none (
  field string)
partitioned by (chunk int);

DROP TABLE IF EXISTS Grep10GB_text_none;
CREATE TABLE Grep10GB_text_none (
  field string)
partitioned by (chunk int);

DROP TABLE IF EXISTS Rankings_web_text_none;
CREATE TABLE Rankings_web_text_none (
  pageRank int,
  pageURL string,
  avgDuration int)
row format delimited fields terminated by '|'  stored as textfile;

DROP TABLE IF EXISTS UserVisits_web_text_none;
CREATE TABLE UserVisits_web_text_none (
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
