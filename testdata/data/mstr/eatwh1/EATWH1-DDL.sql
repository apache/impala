CREATE DATABASE IF NOT EXISTS eatwh1;

DROP TABLE IF EXISTS eatwh1.EATWH1_COST_MARKET_CLASS;
CREATE TABLE eatwh1.EATWH1_COST_MARKET_CLASS (
CUR_TRN_DT	STRING,
CLASS_NBR	INT,
MARKET_NBR	INT,
COST_AMT	FLOAT
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh1.EATWH1_COST_MARKET_DEP;
CREATE TABLE eatwh1.EATWH1_COST_MARKET_DEP (
CUR_TRN_DT	STRING,
DEPARTMENT_NBR	INT,
MARKET_NBR	INT,
COST_AMT	FLOAT
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh1.EATWH1_COST_MARKET_DIV;
CREATE TABLE eatwh1.EATWH1_COST_MARKET_DIV (
CUR_TRN_DT	STRING,
DIVISION_NBR	INT,
MARKET_NBR	INT,
COST_AMT	FLOAT
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh1.EATWH1_COST_REGION_CLASS;
CREATE TABLE eatwh1.EATWH1_COST_REGION_CLASS (
CUR_TRN_DT	STRING,
CLASS_NBR	INT,
REGION_NBR	INT,
COST_AMT	FLOAT
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh1.EATWH1_COST_REGION_ITEM;
CREATE TABLE eatwh1.EATWH1_COST_REGION_ITEM (
CUR_TRN_DT	STRING,
CLASS_NBR	INT,
ITEM_NBR	INT,
REGION_NBR	INT,
COST_AMT	FLOAT
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh1.EATWH1_COST_STORE_DEP;
CREATE TABLE eatwh1.EATWH1_COST_STORE_DEP (
CUR_TRN_DT	STRING,
DEPARTMENT_NBR	INT,
STORE_NBR	INT,
COST_AMT	FLOAT
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh1.EATWH1_COST_STORE_ITEM;
CREATE TABLE eatwh1.EATWH1_COST_STORE_ITEM (
CUR_TRN_DT	STRING,
CLASS_NBR	INT,
ITEM_NBR	INT,
STORE_NBR	INT,
COST_AMT	FLOAT
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh1.EATWH1_FT1;
CREATE TABLE eatwh1.EATWH1_FT1 (
STORE_NBR	INT,
M1	FLOAT
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh1.EATWH1_FT10;
CREATE TABLE eatwh1.EATWH1_FT10 (
REGION_NBR	INT,
MONTH_ID	INT,
DEPARTMENT_NBR	INT,
M10	FLOAT
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh1.EATWH1_FT11;
CREATE TABLE eatwh1.EATWH1_FT11 (
STORE_NBR	INT,
M7	FLOAT
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh1.EATWH1_FT12;
CREATE TABLE eatwh1.EATWH1_FT12 (
DEPARTMENT_NBR	INT,
M12	FLOAT
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh1.EATWH1_FT13;
CREATE TABLE eatwh1.EATWH1_FT13 (
DEPARTMENT_NBR	INT,
CLASS_NBR	INT,
M13	FLOAT
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh1.EATWH1_FT14;
CREATE TABLE eatwh1.EATWH1_FT14 (
MARKET_NBR	INT,
M14	FLOAT
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh1.EATWH1_FT15;
CREATE TABLE eatwh1.EATWH1_FT15 (
REGION_NBR	INT,
M14	FLOAT
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh1.EATWH1_FT17;
CREATE TABLE eatwh1.EATWH1_FT17 (
MARKET_NBR	INT,
WEEK_ID	INT,
M8	FLOAT
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh1.EATWH1_FT2;
CREATE TABLE eatwh1.EATWH1_FT2 (
STORE_NBR	INT,
MONTH_ID	INT,
M3	FLOAT
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh1.EATWH1_FT3;
CREATE TABLE eatwh1.EATWH1_FT3 (
STORE_NBR	INT,
CLASS_NBR	INT,
M1	FLOAT
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh1.EATWH1_FT4;
CREATE TABLE eatwh1.EATWH1_FT4 (
STORE_NBR	INT,
CLASS_NBR	INT,
MONTH_ID	INT,
ACCT_MONTH_DESC	string,
M3	FLOAT
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh1.EATWH1_FT5;
CREATE TABLE eatwh1.EATWH1_FT5 (
REGION_NBR	INT,
DEPARTMENT_NBR	INT,
MONTH_ID	INT,
M5	FLOAT
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh1.EATWH1_FT6;
CREATE TABLE eatwh1.EATWH1_FT6 (
REGION_NBR	INT,
DEPARTMENT_NBR	INT,
M6	FLOAT
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh1.EATWH1_FT7;
CREATE TABLE eatwh1.EATWH1_FT7 (
REGION_NBR	INT,
M7	FLOAT
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh1.EATWH1_FT8;
CREATE TABLE eatwh1.EATWH1_FT8 (
STORE_NBR	INT,
WEEK_ID	INT,
M8	FLOAT
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh1.EATWH1_FT9;
CREATE TABLE eatwh1.EATWH1_FT9 (
REGION_NBR	INT,
WEEK_ID	INT,
M8	FLOAT
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh1.EATWH1_INVENTORY_CURR;
CREATE TABLE eatwh1.EATWH1_INVENTORY_CURR (
ITEM_ID	int,
TARGET_QTY	int,
EOH_QTY	int,
ON_ORDER_QTY	int,
UNIT_COST	int,
REORDER_QTY	int,
TOTAL_AMT	int,
LAST_TRANS_ID	string
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh1.EATWH1_INVENTORY_ORDERS;
CREATE TABLE eatwh1.EATWH1_INVENTORY_ORDERS (
ITEM_ID	int,
REGION_ID	int,
QUARTER_ID	int,
UNITS_RECEIVED	int
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh1.EATWH1_INVENTORY_Q1_1997;
CREATE TABLE eatwh1.EATWH1_INVENTORY_Q1_1997 (
ITEM_ID	int,
REGION_ID	int,
QUARTER_ID	int,
BOH_DLL	FLOAT,
EOH_DLL	FLOAT,
BOH_QTY	FLOAT,
EOH_QTY	FLOAT
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh1.EATWH1_INVENTORY_Q1_1998;
CREATE TABLE eatwh1.EATWH1_INVENTORY_Q1_1998 (
ITEM_ID	int,
REGION_ID	int,
QUARTER_ID	int,
BOH_DLL	FLOAT,
EOH_DLL	FLOAT,
BOH_QTY	FLOAT,
EOH_QTY	FLOAT
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh1.EATWH1_INVENTORY_Q2_1997;
CREATE TABLE eatwh1.EATWH1_INVENTORY_Q2_1997 (
ITEM_ID	int,
REGION_ID	int,
QUARTER_ID	int,
BOH_DLL	FLOAT,
EOH_DLL	FLOAT,
BOH_QTY	FLOAT,
EOH_QTY	FLOAT
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh1.EATWH1_INVENTORY_Q2_1998;
CREATE TABLE eatwh1.EATWH1_INVENTORY_Q2_1998 (
ITEM_ID	int,
REGION_ID	int,
QUARTER_ID	int,
BOH_DLL	FLOAT,
EOH_DLL	FLOAT,
BOH_QTY	FLOAT,
EOH_QTY	FLOAT
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh1.EATWH1_INVENTORY_Q3_1997;
CREATE TABLE eatwh1.EATWH1_INVENTORY_Q3_1997 (
ITEM_ID	int,
REGION_ID	int,
QUARTER_ID	int,
BOH_DLL	FLOAT,
EOH_DLL	FLOAT,
BOH_QTY	FLOAT,
EOH_QTY	FLOAT
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh1.EATWH1_INVENTORY_Q3_1998;
CREATE TABLE eatwh1.EATWH1_INVENTORY_Q3_1998 (
ITEM_ID	int,
REGION_ID	int,
QUARTER_ID	int,
BOH_DLL	FLOAT,
EOH_DLL	FLOAT,
BOH_QTY	FLOAT,
EOH_QTY	FLOAT
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh1.EATWH1_INVENTORY_Q4_1997;
CREATE TABLE eatwh1.EATWH1_INVENTORY_Q4_1997 (
ITEM_ID	int,
REGION_ID	int,
QUARTER_ID	int,
BOH_DLL	FLOAT,
EOH_DLL	FLOAT,
BOH_QTY	FLOAT,
EOH_QTY	FLOAT
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh1.EATWH1_INVENTORY_Q4_1998;
CREATE TABLE eatwh1.EATWH1_INVENTORY_Q4_1998 (
ITEM_ID	int,
REGION_ID	int,
QUARTER_ID	int,
BOH_DLL	FLOAT,
EOH_DLL	FLOAT,
BOH_QTY	FLOAT,
EOH_QTY	FLOAT
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh1.EATWH1_LOOKUP_CLASS;
CREATE TABLE eatwh1.EATWH1_LOOKUP_CLASS (
CLASS_NBR	INT,
CLASS_DESC	string,
DEPARTMENT_NBR	INT,
DIVISION_NBR	INT
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh1.EATWH1_LOOKUP_COLOR;
CREATE TABLE eatwh1.EATWH1_LOOKUP_COLOR (
ITEM_COLOR_ID	INT,
COLOR_DESC	string
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh1.EATWH1_LOOKUP_DAY;
CREATE TABLE eatwh1.EATWH1_LOOKUP_DAY (
CUR_TRN_DT	STRING,
WEEK_ID	INT,
WEEK_NBR	INT,
WEEK_MONTH_NBR	INT,
DAY_MONTH_NBR	INT,
WEEKDAY_NBR	INT,
MONTH_NBR	INT,
MONTH_ID	INT,
QUARTER_ID	INT,
SEASON_ID	INT,
YEAR_ID	INT
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh1.EATWH1_LOOKUP_DEMOG;
CREATE TABLE eatwh1.EATWH1_LOOKUP_DEMOG (
DEMOG_ID	INT,
DEMOG_DESC	string
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh1.EATWH1_LOOKUP_DEPARTMENT;
CREATE TABLE eatwh1.EATWH1_LOOKUP_DEPARTMENT (
DEPARTMENT_NBR	INT,
DEPARTMENT_DESC	string,
DIVISION_NBR	INT
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh1.EATWH1_LOOKUP_DIVISION;
CREATE TABLE eatwh1.EATWH1_LOOKUP_DIVISION (
DIVISION_NBR	INT,
DIVISION_DESC	string
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh1.EATWH1_LOOKUP_ITEM;
CREATE TABLE eatwh1.EATWH1_LOOKUP_ITEM (
CLASS_NBR	INT,
ITEM_NBR	INT,
CLASS_DESC	string,
ITEM_DESC	string,
DEPARTMENT_NBR	INT,
DIVISION_NBR	INT,
LONG_DESC	string
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh1.EATWH1_LOOKUP_MANAGER;
CREATE TABLE eatwh1.EATWH1_LOOKUP_MANAGER (
MANAGER_ID	INT,
MANAGER_DESC	string
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh1.EATWH1_LOOKUP_MARKET;
CREATE TABLE eatwh1.EATWH1_LOOKUP_MARKET (
MARKET_NBR	INT,
MARKET_DESC	string,
REGION_NBR	INT
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh1.EATWH1_LOOKUP_MONTH;
CREATE TABLE eatwh1.EATWH1_LOOKUP_MONTH (
MONTH_ID	INT,
MONTH_NBR	INT,
MONTH_DESC	string,
ACCT_MONTH_DESC	string,
QUARTER_ID	INT
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh1.EATWH1_LOOKUP_PRICEZONE;
CREATE TABLE eatwh1.EATWH1_LOOKUP_PRICEZONE (
PRICEZONE_ID	INT,
MARKET_NBR	INT,
DEPARTMENT_NBR	INT
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh1.EATWH1_LOOKUP_QUAL;
CREATE TABLE eatwh1.EATWH1_LOOKUP_QUAL (
CLASS_NBR	INT,
ITEM_NBR	INT,
ON_ORDER	FLOAT,
STORE_NBR	INT
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh1.EATWH1_LOOKUP_QUARTER;
CREATE TABLE eatwh1.EATWH1_LOOKUP_QUARTER (
QUARTER_ID	INT,
QUARTER_NBR	int,
QUARTER_DESC	string,
SEASON_ID	INT
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh1.EATWH1_LOOKUP_REGION;
CREATE TABLE eatwh1.EATWH1_LOOKUP_REGION (
REGION_NBR	INT,
REGION_DESC	string
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh1.EATWH1_LOOKUP_SEASON;
CREATE TABLE eatwh1.EATWH1_LOOKUP_SEASON (
SEASON_ID	INT,
SEASON_NBR	int,
SEASON_DESC	string,
YEAR_ID	INT
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh1.EATWH1_LOOKUP_SIZE;
CREATE TABLE eatwh1.EATWH1_LOOKUP_SIZE (
ITEM_SIZE	FLOAT,
SIZE_DESC	string
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh1.EATWH1_LOOKUP_STATE;
CREATE TABLE eatwh1.EATWH1_LOOKUP_STATE (
STATE_NBR	int,
STATE_DESC	string
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh1.EATWH1_LOOKUP_STORE;
CREATE TABLE eatwh1.EATWH1_LOOKUP_STORE (
STORE_NBR	INT,
STORE_DESC	string,
MARKET_NBR	INT,
REGION_NBR	INT,
STATE_NBR	INT
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh1.EATWH1_LOOKUP_STYLE;
CREATE TABLE eatwh1.EATWH1_LOOKUP_STYLE (
ITEM_STYLE	INT,
STYLE_DESC	string
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh1.EATWH1_LOOKUP_TYPE;
CREATE TABLE eatwh1.EATWH1_LOOKUP_TYPE (
STORE_TYPE	INT,
TYPE_DESC	string
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh1.EATWH1_LOOKUP_WEEK;
CREATE TABLE eatwh1.EATWH1_LOOKUP_WEEK (
WEEK_ID	INT,
WEEK_NBR	int,
WEEK_DESC	string,
MONTH_ID	INT
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh1.EATWH1_LOOKUP_WEEKDAY;
CREATE TABLE eatwh1.EATWH1_LOOKUP_WEEKDAY (
WEEKDAY_NBR	INT,
WEEKDAY_DESC	string
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh1.EATWH1_LOOKUP_YEAR;
CREATE TABLE eatwh1.EATWH1_LOOKUP_YEAR (
YEAR_ID	INT,
YEAR_NBR	int,
YEAR_DESC	string
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh1.EATWH1_LU_CALL_CTR;
CREATE TABLE eatwh1.EATWH1_LU_CALL_CTR (
CALL_CTR_ID	int,
CENTER_NAME	string,
COUNTRY_ID	int,
REGION_ID	int,
DIST_CTR_ID	int,
MANAGER_ID	int
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh1.EATWH1_LU_CATALOG;
CREATE TABLE eatwh1.EATWH1_LU_CATALOG (
CAT_ID	int,
CAT_DESC	string,
CAT_URL	string,
CAT_SHIP_COUNT	int
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh1.EATWH1_LU_CATEGORY;
CREATE TABLE eatwh1.EATWH1_LU_CATEGORY (
CATEGORY_ID	int,
CATEGORY_DESC	string
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh1.EATWH1_LU_COUNTRY;
CREATE TABLE eatwh1.EATWH1_LU_COUNTRY (
COUNTRY_ID	int,
COUNTRY_NAME	string
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh1.EATWH1_LU_CUST_CITY;
CREATE TABLE eatwh1.EATWH1_LU_CUST_CITY (
CUST_CITY_ID	int,
CUST_CITY_NAME	string,
CUST_REGION_ID	int
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh1.EATWH1_LU_CUST_REGION;
CREATE TABLE eatwh1.EATWH1_LU_CUST_REGION (
CUST_REGION_ID	int,
CUST_REGION_NAME	string
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh1.EATWH1_LU_CUSTOMER;
CREATE TABLE eatwh1.EATWH1_LU_CUSTOMER (
CUSTOMER_ID	int,
CUST_FIRST_NAME	string,
CUST_LAST_NAME	string,
CUST_BIRTHSTRING	STRING,
ADDRESS	string,
INCOME_ID	int,
EMAIL	string,
CUST_CITY_ID	int
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh1.EATWH1_LU_DATE;
CREATE TABLE eatwh1.EATWH1_LU_DATE (
DATE_ID STRING,
MONTH_ID int,
QUARTER_ID int,
YEAR_ID int
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh1.EATWH1_LU_DIST_CTR;
CREATE TABLE eatwh1.EATWH1_LU_DIST_CTR (
DIST_CTR_ID	int,
DIST_CTR_NAME	string,
COUNTRY_ID	int
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh1.EATWH1_LU_EMPLOYEE;
CREATE TABLE eatwh1.EATWH1_LU_EMPLOYEE (
EMP_ID	int,
EMP_LAST_NAME	string,
EMP_FIRST_NAME	string,
EMP_SSN	int,
CALL_CTR_ID	int,
DIST_CTR_ID	int,
COUNTRY_ID	int,
MANAGER_ID	int,
SALARY	int,
BIRTH_DATE	STRING,
HIRE_DATE	STRING
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh1.EATWH1_LU_INCOME;
CREATE TABLE eatwh1.EATWH1_LU_INCOME (
INCOME_ID	int,
BRACKET_DESC	string
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh1.EATWH1_LU_ITEM;
CREATE TABLE eatwh1.EATWH1_LU_ITEM (
ITEM_ID	int,
ITEM_NAME	string,
SUPPLIER_ID	int,
ITEM_FOREIGN_NAME	string,
ITEM_URL	string,
SUBCAT_ID	int,
DISC_CD	int,
ITEM_LONG_DESC	string,
WARRANTY	string,
UNIT_PRICE	FLOAT
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh1.EATWH1_LU_MANAGER;
CREATE TABLE eatwh1.EATWH1_LU_MANAGER (
MANAGER_ID	int,
MGR_LAST_NAME	string,
MGR_FIRST_NAME	string
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh1.EATWH1_LU_MONTH;
CREATE TABLE eatwh1.EATWH1_LU_MONTH (
MONTH_ID	int,
MONTH_DESC	string,
MONTH_OF_YEAR	int,
QUARTER_ID	int,
YEAR_ID	int
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh1.EATWH1_LU_MONTH_OF_YEAR;
CREATE TABLE eatwh1.EATWH1_LU_MONTH_OF_YEAR (
MONTH_OF_YEAR	int,
MONTH_OF_YEAR_NAME	string
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh1.EATWH1_LU_ORDER;
CREATE TABLE eatwh1.EATWH1_LU_ORDER (
ORDER_ID	int,
CUSTOMER_ID	int,
PYMT_TYPE	int,
SHIPPER_ID	int
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh1.EATWH1_LU_PROMO_TYPE;
CREATE TABLE eatwh1.EATWH1_LU_PROMO_TYPE (
PROMO_TYPE_ID	int,
PROMO_TYPE_DESC	string
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh1.EATWH1_LU_PROMOTION;
CREATE TABLE eatwh1.EATWH1_LU_PROMOTION (
PROMO_SALE_ID	int,
PROMO_SALE	string,
PROMO_TYPE_ID	int
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh1.EATWH1_LU_PYMT_TYPE;
CREATE TABLE eatwh1.EATWH1_LU_PYMT_TYPE (
PYMT_TYPE	int,
PYMT_DESC	string
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh1.EATWH1_LU_QUARTER;
CREATE TABLE eatwh1.EATWH1_LU_QUARTER (
QUARTER_ID	int,
QUARTER_DESC	string,
YEAR_ID	int
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh1.EATWH1_LU_REGION;
CREATE TABLE eatwh1.EATWH1_LU_REGION (
REGION_ID	int,
REGION_NAME	string,
COUNTRY_ID	int
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh1.EATWH1_LU_SHIPPER;
CREATE TABLE eatwh1.EATWH1_LU_SHIPPER (
SHIPPER_ID	int,
SHIPPER_DESC	string,
CONTRACT_NBR	int
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh1.EATWH1_LU_SUBCATEG;
CREATE TABLE eatwh1.EATWH1_LU_SUBCATEG (
SUBCAT_ID	int,
SUBCAT_DESC	string,
CATEGORY_ID	int
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh1.EATWH1_LU_SUPPLIER;
CREATE TABLE eatwh1.EATWH1_LU_SUPPLIER (
SUPPLIER_ID	int,
SUPPLIER_NAME	string,
CONTACT_LAST_NAME	string,
CONTACT_FIRST_NAME	string
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh1.EATWH1_LU_YEAR;
CREATE TABLE eatwh1.EATWH1_LU_YEAR (
YEAR_ID	int,
YEAR_DATE	STRING,
YEAR_DURATION	int,
PREV_YEAR_ID	int
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh1.EATWH1_MARKET_CLASS;
CREATE TABLE eatwh1.EATWH1_MARKET_CLASS (
CUR_TRN_DT	STRING,
CLASS_NBR	INT,
MARKET_NBR	INT,
REG_SLS_DLR	FLOAT,
REG_SLS_QTY	FLOAT,
PML_SLS_DLR	FLOAT,
PML_SLS_QTY	FLOAT,
CLE_SLS_DLR	FLOAT,
CLE_SLS_QTY	FLOAT,
PLN_SLS_DLR	FLOAT,
TOT_SLS_DLR	FLOAT,
TOT_SLS_QTY	FLOAT,
BGN_REG_STK_DLR	FLOAT,
BGN_CLE_STK_DLR	FLOAT,
BGN_REG_STK_QTY	FLOAT,
BGN_CLE_STK_QTY	FLOAT,
END_REG_STK_DLR	FLOAT,
END_CLE_STK_DLR	FLOAT,
END_REG_STK_QTY	FLOAT,
END_CLE_STK_QTY	FLOAT
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh1.EATWH1_MARKET_DEPARTMENT;
CREATE TABLE eatwh1.EATWH1_MARKET_DEPARTMENT (
CUR_TRN_DT	STRING,
DEPARTMENT_NBR	INT,
MARKET_NBR	INT,
REG_SLS_DLR	FLOAT,
REG_SLS_QTY	FLOAT,
PML_SLS_DLR	FLOAT,
PML_SLS_QTY	FLOAT,
CLE_SLS_DLR	FLOAT,
CLE_SLS_QTY	FLOAT,
PLN_SLS_DLR	FLOAT,
TOT_SLS_DLR	FLOAT,
TOT_SLS_QTY	FLOAT,
BGN_REG_STK_DLR	FLOAT,
BGN_CLE_STK_DLR	FLOAT,
BGN_REG_STK_QTY	FLOAT,
BGN_CLE_STK_QTY	FLOAT,
END_REG_STK_DLR	FLOAT,
END_CLE_STK_DLR	FLOAT,
END_REG_STK_QTY	FLOAT,
END_CLE_STK_QTY	FLOAT
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh1.EATWH1_MARKET_DIVISION;
CREATE TABLE eatwh1.EATWH1_MARKET_DIVISION (
CUR_TRN_DT	STRING,
DIVISION_NBR	INT,
MARKET_NBR	INT,
REG_SLS_DLR	FLOAT,
REG_SLS_QTY	FLOAT,
PML_SLS_DLR	FLOAT,
PML_SLS_QTY	FLOAT,
CLE_SLS_DLR	FLOAT,
CLE_SLS_QTY	FLOAT,
PLN_SLS_DLR	FLOAT,
TOT_SLS_DLR	FLOAT,
TOT_SLS_QTY	FLOAT,
BGN_REG_STK_DLR	FLOAT,
BGN_CLE_STK_DLR	FLOAT,
BGN_REG_STK_QTY	FLOAT,
BGN_CLE_STK_QTY	FLOAT,
END_REG_STK_DLR	FLOAT,
END_CLE_STK_DLR	FLOAT,
END_REG_STK_QTY	FLOAT,
END_CLE_STK_QTY	FLOAT
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh1.EATWH1_MARKET_ITEM;
CREATE TABLE eatwh1.EATWH1_MARKET_ITEM (
CUR_TRN_DT	STRING,
CLASS_NBR	INT,
ITEM_NBR	INT,
MARKET_NBR	INT,
REG_SLS_DLR	FLOAT,
REG_SLS_QTY	FLOAT,
PML_SLS_DLR	FLOAT,
PML_SLS_QTY	FLOAT,
CLE_SLS_DLR	FLOAT,
CLE_SLS_QTY	FLOAT,
PLN_SLS_DLR	FLOAT,
TOT_SLS_DLR	FLOAT,
TOT_SLS_QTY	FLOAT,
BGN_REG_STK_DLR	FLOAT,
BGN_CLE_STK_DLR	FLOAT,
BGN_REG_STK_QTY	FLOAT,
BGN_CLE_STK_QTY	FLOAT,
END_REG_STK_DLR	FLOAT,
END_CLE_STK_DLR	FLOAT,
END_REG_STK_QTY	FLOAT,
END_CLE_STK_QTY	FLOAT
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh1.EATWH1_MNTH_CATEGORY_SLS;
CREATE TABLE eatwh1.EATWH1_MNTH_CATEGORY_SLS (
CATEGORY_ID	int,
MONTH_ID	int,
TOT_UNIT_SALES	FLOAT,
TOT_DOLLAR_SALES	FLOAT
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh1.EATWH1_MSI_STATS_LOG;
CREATE TABLE eatwh1.EATWH1_MSI_STATS_LOG (
SESSIONID	string,
PROJECTID	int,
EVENTTIME	STRING,
EVENT	INT
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh1.EATWH1_MSI_STATS_PROP;
CREATE TABLE eatwh1.EATWH1_MSI_STATS_PROP (
PROP_NAME	string,
PROP_VAL	string
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh1.EATWH1_ORDER_DETAIL;
CREATE TABLE eatwh1.EATWH1_ORDER_DETAIL (
ORDER_ID int,
CALL_CTR_ID int,
ORDER_DATE STRING,
ITEM_ID	int,
QTY_SOLD FLOAT,
UNIT_PRICE FLOAT,
PROMOTION_ID int,
DISCOUNT FLOAT
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh1.EATWH1_ORDER_FACT;
CREATE TABLE eatwh1.EATWH1_ORDER_FACT (
ORDER_ID int,
EMP_ID int,
ORDER_DATE STRING,
ORDER_AMT FLOAT,
FREIGHT FLOAT,
SHIP_DATE STRING,
QTY_SOLD FLOAT,
RUSH_ORDER string
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh1.EATWH1_P_DAY_DEC_94;
CREATE TABLE eatwh1.EATWH1_P_DAY_DEC_94 (
DAY	STRING,
CLASS_ID	int,
ITEM_ID	int,
STORE_ID	int,
TOT_SLS_DLR	FLOAT,
TOT_SLS_QTY	FLOAT
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh1.EATWH1_P_MONTH_DEC_93;
CREATE TABLE eatwh1.EATWH1_P_MONTH_DEC_93 (
MONTH	int,
CLASS_ID	int,
ITEM_ID	int,
STORE_ID	int,
SALES_DLR	FLOAT,
SALES_QTY	FLOAT
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh1.EATWH1_P_WEEK_DEC_94_1;
CREATE TABLE eatwh1.EATWH1_P_WEEK_DEC_94_1 (
WEEK	int,
CLASS_ID	int,
ITEM_ID	int,
STORE_ID	int,
SALES_DLR	FLOAT,
SALES_QTY	FLOAT
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh1.EATWH1_P_WEEK_DEC_94_2;
CREATE TABLE eatwh1.EATWH1_P_WEEK_DEC_94_2 (
WEEK	int,
CLASS_ID	int,
ITEM_ID	int,
STORE_ID	int,
SALES_DLR	FLOAT,
SALES_QTY	FLOAT
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh1.EATWH1_P_WEEK_DEC_94_3;
CREATE TABLE eatwh1.EATWH1_P_WEEK_DEC_94_3 (
WEEK	int,
CLASS_ID	int,
ITEM_ID	int,
STORE_ID	int,
SALES_DLR	FLOAT,
SALES_QTY	FLOAT
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh1.EATWH1_PMT_INVENTORY;
CREATE TABLE eatwh1.EATWH1_PMT_INVENTORY (
QUARTER_ID	int,
PBTNAME	string
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh1.EATWH1_PRODUCT_DETAIL;
CREATE TABLE eatwh1.EATWH1_PRODUCT_DETAIL (
ITEM_ID	int,
QUARTER_ID	int,
QTY_PER_UNIT	int,
DISCOUNT_PRICE	FLOAT,
UNIT_COST	FLOAT
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh1.EATWH1_PROMOTIONS;
CREATE TABLE eatwh1.EATWH1_PROMOTIONS (
ITEM_ID	int,
QUARTER_ID	int,
DISCOUNT	FLOAT,
PROMO_SALE_ID	int
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh1.EATWH1_QTR_CATEGORY_SLS;
CREATE TABLE eatwh1.EATWH1_QTR_CATEGORY_SLS (
CATEGORY_ID	int,
QUARTER_ID	int,
TOT_UNIT_SALES	FLOAT,
TOT_DOLLAR_SALES	FLOAT
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh1.EATWH1_REGION_CLASS;
CREATE TABLE eatwh1.EATWH1_REGION_CLASS (
CUR_TRN_DT	STRING,
CLASS_NBR	INT,
REGION_NBR	INT,
REG_SLS_DLR	FLOAT,
REG_SLS_QTY	FLOAT,
PML_SLS_DLR	FLOAT,
PML_SLS_QTY	FLOAT,
CLE_SLS_DLR	FLOAT,
CLE_SLS_QTY	FLOAT,
PLN_SLS_DLR	FLOAT,
TOT_SLS_DLR	FLOAT,
TOT_SLS_QTY	FLOAT,
BGN_REG_STK_DLR	FLOAT,
BGN_CLE_STK_DLR	FLOAT,
BGN_REG_STK_QTY	FLOAT,
BGN_CLE_STK_QTY	FLOAT,
END_REG_STK_DLR	FLOAT,
END_CLE_STK_DLR	FLOAT,
END_REG_STK_QTY	FLOAT,
END_CLE_STK_QTY	FLOAT
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh1.EATWH1_REGION_DEPARTMENT;
CREATE TABLE eatwh1.EATWH1_REGION_DEPARTMENT (
CUR_TRN_DT	STRING,
DEPARTMENT_NBR	INT,
REGION_NBR	INT,
REG_SLS_DLR	FLOAT,
REG_SLS_QTY	FLOAT,
PML_SLS_DLR	FLOAT,
PML_SLS_QTY	FLOAT,
CLE_SLS_DLR	FLOAT,
CLE_SLS_QTY	FLOAT,
PLN_SLS_DLR	FLOAT,
TOT_SLS_DLR	FLOAT,
TOT_SLS_QTY	FLOAT,
BGN_REG_STK_DLR	FLOAT,
BGN_CLE_STK_DLR	FLOAT,
BGN_REG_STK_QTY	FLOAT,
BGN_CLE_STK_QTY	FLOAT,
END_REG_STK_DLR	FLOAT,
END_CLE_STK_DLR	FLOAT,
END_REG_STK_QTY	FLOAT,
END_CLE_STK_QTY	FLOAT
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh1.EATWH1_REGION_DIVISION;
CREATE TABLE eatwh1.EATWH1_REGION_DIVISION (
CUR_TRN_DT	STRING,
DIVISION_NBR	INT,
REGION_NBR	INT,
REG_SLS_DLR	FLOAT,
REG_SLS_QTY	FLOAT,
PML_SLS_DLR	FLOAT,
PML_SLS_QTY	FLOAT,
CLE_SLS_DLR	FLOAT,
CLE_SLS_QTY	FLOAT,
PLN_SLS_DLR	FLOAT,
TOT_SLS_DLR	FLOAT,
TOT_SLS_QTY	FLOAT,
BGN_REG_STK_DLR	FLOAT,
BGN_CLE_STK_DLR	FLOAT,
BGN_REG_STK_QTY	FLOAT,
BGN_CLE_STK_QTY	FLOAT,
END_REG_STK_DLR	FLOAT,
END_CLE_STK_DLR	FLOAT,
END_REG_STK_QTY	FLOAT,
END_CLE_STK_QTY	FLOAT
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh1.EATWH1_REGION_ITEM;
CREATE TABLE eatwh1.EATWH1_REGION_ITEM (
CUR_TRN_DT	STRING,
CLASS_NBR	INT,
ITEM_NBR	INT,
REGION_NBR	INT,
REG_SLS_DLR	FLOAT,
REG_SLS_QTY	FLOAT,
PML_SLS_DLR	FLOAT,
PML_SLS_QTY	FLOAT,
CLE_SLS_DLR	FLOAT,
CLE_SLS_QTY	FLOAT,
PLN_SLS_DLR	FLOAT,
TOT_SLS_DLR	FLOAT,
TOT_SLS_QTY	FLOAT,
BGN_REG_STK_DLR	FLOAT,
BGN_CLE_STK_DLR	FLOAT,
BGN_REG_STK_QTY	FLOAT,
BGN_CLE_STK_QTY	FLOAT,
END_REG_STK_DLR	FLOAT,
END_CLE_STK_DLR	FLOAT,
END_REG_STK_QTY	FLOAT,
END_CLE_STK_QTY	FLOAT
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh1.EATWH1_REL_CAT_ITEM;
CREATE TABLE eatwh1.EATWH1_REL_CAT_ITEM (
ITEM_ID	int,
CAT_ID	int
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh1.EATWH1_RELATE_ITEM_COLOR;
CREATE TABLE eatwh1.EATWH1_RELATE_ITEM_COLOR (
CLASS_NBR	INT,
ITEM_NBR	INT,
ITEM_COLOR_ID	INT
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh1.EATWH1_RELATE_ITEM_SIZE;
CREATE TABLE eatwh1.EATWH1_RELATE_ITEM_SIZE (
CLASS_NBR	INT,
ITEM_NBR	INT,
ITEM_SIZE	INT
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh1.EATWH1_RELATE_ITEM_STYLE;
CREATE TABLE eatwh1.EATWH1_RELATE_ITEM_STYLE (
CLASS_NBR	INT,
ITEM_NBR	INT,
ITEM_STYLE	INT
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh1.EATWH1_RELATE_LY_LW;
CREATE TABLE eatwh1.EATWH1_RELATE_LY_LW (
CUR_TRN_DT	STRING,
LAST_YEAR_DATE	STRING,
LAST_MONTH_DATE	STRING,
LAST_WEEK_DATE	STRING
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh1.EATWH1_RELATE_MTD;
CREATE TABLE eatwh1.EATWH1_RELATE_MTD (
CUR_TRN_DT	STRING,
MONTH_TO_DATE	STRING
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh1.EATWH1_RELATE_REGION_MAN;
CREATE TABLE eatwh1.EATWH1_RELATE_REGION_MAN (
REGION_NBR	INT,
MANAGER_ID	INT
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh1.EATWH1_RELATE_STORE_DEMOG;
CREATE TABLE eatwh1.EATWH1_RELATE_STORE_DEMOG (
STORE_NBR	INT,
DEMOG_ID	INT
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh1.EATWH1_RELATE_STORE_TYPE;
CREATE TABLE eatwh1.EATWH1_RELATE_STORE_TYPE (
STORE_NBR	INT,
STORE_TYPE	INT
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh1.EATWH1_RUSH_ORDER;
CREATE TABLE eatwh1.EATWH1_RUSH_ORDER (
ORDER_ID	int,
CUSTOMER_ID	int,
EMP_ID	int,
ORDER_DATE	STRING,
ORDER_AMT	FLOAT,
RUSH_CHARGE	FLOAT,
SHIP_CHARGE	FLOAT,
AMT_PAID	FLOAT
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh1.EATWH1_SEPERATE_FACT;
CREATE TABLE eatwh1.EATWH1_SEPERATE_FACT (
YEAR_ID	INT,
YEAR_DESC	string,
TOTALQTY	FLOAT
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh1.EATWH1_STORE_CLASS;
CREATE TABLE eatwh1.EATWH1_STORE_CLASS (
CUR_TRN_DT	STRING,
CLASS_NBR	INT,
STORE_NBR	INT,
REG_SLS_DLR	FLOAT,
REG_SLS_QTY	FLOAT,
PML_SLS_DLR	FLOAT,
PML_SLS_QTY	FLOAT,
CLE_SLS_DLR	FLOAT,
CLE_SLS_QTY	FLOAT,
PLN_SLS_DLR	FLOAT,
TOT_SLS_DLR	FLOAT,
TOT_SLS_QTY	FLOAT,
BGN_REG_STK_DLR	FLOAT,
BGN_CLE_STK_DLR	FLOAT,
BGN_REG_STK_QTY	FLOAT,
BGN_CLE_STK_QTY	FLOAT,
END_REG_STK_DLR	FLOAT,
END_CLE_STK_DLR	FLOAT,
END_REG_STK_QTY	FLOAT,
END_CLE_STK_QTY	FLOAT,
QUAL	INT
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh1.EATWH1_STORE_DEPARTMENT;
CREATE TABLE eatwh1.EATWH1_STORE_DEPARTMENT (
CUR_TRN_DT	STRING,
DEPARTMENT_NBR	INT,
STORE_NBR	INT,
REG_SLS_DLR	FLOAT,
REG_SLS_QTY	FLOAT,
PML_SLS_DLR	FLOAT,
PML_SLS_QTY	FLOAT,
CLE_SLS_DLR	FLOAT,
CLE_SLS_QTY	FLOAT,
PLN_SLS_DLR	FLOAT,
TOT_SLS_DLR	FLOAT,
TOT_SLS_QTY	FLOAT,
BGN_REG_STK_DLR	FLOAT,
BGN_CLE_STK_DLR	FLOAT,
BGN_REG_STK_QTY	FLOAT,
BGN_CLE_STK_QTY	FLOAT,
END_REG_STK_DLR	FLOAT,
END_CLE_STK_DLR	FLOAT,
END_REG_STK_QTY	FLOAT,
END_CLE_STK_QTY	FLOAT
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh1.EATWH1_STORE_DIVISION;
CREATE TABLE eatwh1.EATWH1_STORE_DIVISION (
CUR_TRN_DT	STRING,
DIVISION_NBR	INT,
STORE_NBR	INT,
REG_SLS_DLR	FLOAT,
REG_SLS_QTY	FLOAT,
PML_SLS_DLR	FLOAT,
PML_SLS_QTY	FLOAT,
CLE_SLS_DLR	FLOAT,
CLE_SLS_QTY	FLOAT,
PLN_SLS_DLR	FLOAT,
TOT_SLS_DLR	FLOAT,
TOT_SLS_QTY	FLOAT,
BGN_REG_STK_DLR	FLOAT,
BGN_CLE_STK_DLR	FLOAT,
BGN_REG_STK_QTY	FLOAT,
BGN_CLE_STK_QTY	FLOAT,
END_REG_STK_DLR	FLOAT,
END_CLE_STK_DLR	FLOAT,
END_REG_STK_QTY	FLOAT,
END_CLE_STK_QTY	FLOAT
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh1.EATWH1_STORE_ITEM_93;
CREATE TABLE eatwh1.EATWH1_STORE_ITEM_93 (
CUR_TRN_DT	STRING,
CLASS_NBR	INT,
ITEM_NBR	INT,
STORE_NBR	INT,
REG_SLS_DLR	FLOAT,
REG_SLS_QTY	INT,
PML_SLS_DLR	FLOAT,
PML_SLS_QTY	INT,
CLE_SLS_DLR	FLOAT,
CLE_SLS_QTY	INT,
PLN_SLS_DLR	FLOAT,
TOT_SLS_DLR	FLOAT,
TOT_SLS_QTY	INT,
BGN_REG_STK_DLR	FLOAT,
BGN_CLE_STK_DLR	FLOAT,
BGN_REG_STK_QTY	INT,
BGN_CLE_STK_QTY	INT,
END_REG_STK_DLR	FLOAT,
END_CLE_STK_DLR	FLOAT,
END_REG_STK_QTY	INT,
END_CLE_STK_QTY	INT
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh1.EATWH1_STORE_ITEM_93_HETERO;
CREATE TABLE eatwh1.EATWH1_STORE_ITEM_93_HETERO (
CUR_TRN_DT	STRING,
CLASS_NBR	INT,
ITEM_NBR	INT,
STORE_NBR	INT,
DLR_93	FLOAT
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh1.EATWH1_STORE_ITEM_94;
CREATE TABLE eatwh1.EATWH1_STORE_ITEM_94 (
CUR_TRN_DT	STRING,
CLASS_NBR	INT,
ITEM_NBR	INT,
STORE_NBR	INT,
REG_SLS_DLR	FLOAT,
REG_SLS_QTY	INT,
PML_SLS_DLR	FLOAT,
PML_SLS_QTY	INT,
CLE_SLS_DLR	FLOAT,
CLE_SLS_QTY	INT,
PLN_SLS_DLR	FLOAT,
TOT_SLS_DLR	FLOAT,
TOT_SLS_QTY	INT,
BGN_REG_STK_DLR	FLOAT,
BGN_CLE_STK_DLR	FLOAT,
BGN_REG_STK_QTY	INT,
BGN_CLE_STK_QTY	INT,
END_REG_STK_DLR	FLOAT,
END_CLE_STK_DLR	FLOAT,
END_REG_STK_QTY	INT,
END_CLE_STK_QTY	INT
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh1.EATWH1_STORE_ITEM_94_HETERO;
CREATE TABLE eatwh1.EATWH1_STORE_ITEM_94_HETERO (
CUR_TRN_DT	STRING,
CLASS_NBR	INT,
ITEM_NBR	INT,
STORE_NBR	INT,
DLR_94	FLOAT
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh1.EATWH1_STORE_ITEM_ORDERS;
CREATE TABLE eatwh1.EATWH1_STORE_ITEM_ORDERS (
CLASS_NBR	INT,
ITEM_NBR	INT,
STORE_NBR	INT,
ON_ORDER	FLOAT
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh1.EATWH1_STORE_ITEM_PTMAP;
CREATE TABLE eatwh1.EATWH1_STORE_ITEM_PTMAP (
YEAR_ID	INT,
PBTNAME	string,
DDBSOURCE	FLOAT
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh1.EATWH1_TE_TRANS_STATISTICS;
CREATE TABLE eatwh1.EATWH1_TE_TRANS_STATISTICS (
SERVER_ID	string,
MACHINE	string,
TRANS_ID	string,
TRANS_TYPE	string,
TRANS_PARA	string,
TRANS_RESULT	string,
START_TIME	FLOAT,
END_TIME	FLOAT,
CUR_TRANS	int
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh1.EATWH1_TRANS_DATE_LW_LY;
CREATE TABLE eatwh1.EATWH1_TRANS_DATE_LW_LY (
DATE_ID STRING,
LW_DATE_ID STRING,
LY_DATE_ID STRING
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh1.EATWH1_TRANS_DATE_MTD;
CREATE TABLE eatwh1.EATWH1_TRANS_DATE_MTD (
DATE_ID STRING,
MTD_DATE_ID STRING
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh1.EATWH1_TRANS_MONTH_LY;
CREATE TABLE eatwh1.EATWH1_TRANS_MONTH_LY (
MONTH_ID	int,
LY_MONTH_ID	int
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh1.EATWH1_TRANS_QUARTER_LY;
CREATE TABLE eatwh1.EATWH1_TRANS_QUARTER_LY (
QUARTER_ID	int,
LY_QUARTER_ID	int
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh1.EATWH1_TRANS_YEAR_LY;
CREATE TABLE eatwh1.EATWH1_TRANS_YEAR_LY (
YEAR_ID	int,
LY_YEAR_ID	int
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh1.EATWH1_YR_CATEGORY_SLS;
CREATE TABLE eatwh1.EATWH1_YR_CATEGORY_SLS (
CATEGORY_ID	int,
YEAR_ID	int,
TOT_UNIT_SALES	FLOAT,
TOT_DOLLAR_SALES	FLOAT
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;
