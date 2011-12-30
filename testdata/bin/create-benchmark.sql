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

