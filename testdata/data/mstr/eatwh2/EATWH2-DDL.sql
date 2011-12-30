CREATE DATABASE IF NOT EXISTS eatwh2;

DROP TABLE IF EXISTS eatwh2.EATWH2_BIG_ORDER_FACT;
CREATE TABLE eatwh2.EATWH2_BIG_ORDER_FACT (
CREDIT_CARD_ID	FLOAT,
TRANSACTION_AMOUNT	FLOAT,
Credit_Card_Order_ID	FLOAT,
Big_Float_Revenue	INT,
Big_SmallFloat_Revenue	INT
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh2.EATWH2_CITY_CTR_SLS;
CREATE TABLE eatwh2.EATWH2_CITY_CTR_SLS (
CUST_CITY_ID	INT,
CALL_CTR_ID	INT,
TOT_DOLLAR_SALES	FLOAT,
TOT_UNIT_SALES	FLOAT,
TOT_COST	FLOAT
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh2.EATWH2_CITY_MNTH_SLS;
CREATE TABLE eatwh2.EATWH2_CITY_MNTH_SLS (
CUST_CITY_ID	INT,
MONTH_ID	INT,
TOT_DOLLAR_SALES	FLOAT,
TOT_UNIT_SALES	FLOAT,
TOT_COST	FLOAT
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh2.EATWH2_CITY_SUBCATEG_MNTH_SLS;
CREATE TABLE eatwh2.EATWH2_CITY_SUBCATEG_MNTH_SLS (
CUST_CITY_ID	INT,
SUBCAT_ID	INT,
MONTH_ID	INT,
TOT_DOLLAR_SALES	FLOAT,
TOT_UNIT_SALES	FLOAT,
TOT_COST	FLOAT
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh2.EATWH2_CITY_SUBCATEG_SLS;
CREATE TABLE eatwh2.EATWH2_CITY_SUBCATEG_SLS (
CUST_CITY_ID	INT,
SUBCAT_ID	INT,
TOT_DOLLAR_SALES	FLOAT,
TOT_UNIT_SALES	FLOAT,
TOT_COST	FLOAT
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh2.EATWH2_CUSTOMER_SLS;
CREATE TABLE eatwh2.EATWH2_CUSTOMER_SLS (
CUSTOMER_ID	INT,
TOT_DOLLAR_SALES	FLOAT,
TOT_UNIT_SALES	FLOAT,
TOT_COST	FLOAT
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh2.EATWH2_DAY_CTR_SLS;
CREATE TABLE eatwh2.EATWH2_DAY_CTR_SLS (
DAY_DATE	string,
CALL_CTR_ID	INT,
TOT_DOLLAR_SALES	FLOAT,
TOT_UNIT_SALES	FLOAT,
TOT_COST	FLOAT
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh2.EATWH2_INVENTORY_CURR;
CREATE TABLE eatwh2.EATWH2_INVENTORY_CURR (
ITEM_ID	INT,
TARGET_QTY	FLOAT,
EOH_QTY	FLOAT,
ON_ORDER_QTY	FLOAT,
UNIT_COST	FLOAT,
REORDER_QTY	FLOAT,
TOTAL_AMT	FLOAT,
LAST_TRANS_ID	STRING
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh2.EATWH2_INVENTORY_ORDERS;
CREATE TABLE eatwh2.EATWH2_INVENTORY_ORDERS (
MONTH_ID	INT,
ITEM_ID	INT,
UNITS_RECEIVED	FLOAT
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh2.EATWH2_INVENTORY_Q1_2000;
CREATE TABLE eatwh2.EATWH2_INVENTORY_Q1_2000 (
MONTH_ID	INT,
ITEM_ID	INT,
BOH_QTY	FLOAT,
EOH_QTY	FLOAT
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh2.EATWH2_INVENTORY_Q1_2001;
CREATE TABLE eatwh2.EATWH2_INVENTORY_Q1_2001 (
MONTH_ID	INT,
ITEM_ID	INT,
BOH_QTY	FLOAT,
EOH_QTY	FLOAT
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh2.EATWH2_INVENTORY_Q2_2000;
CREATE TABLE eatwh2.EATWH2_INVENTORY_Q2_2000 (
MONTH_ID	INT,
ITEM_ID	INT,
BOH_QTY	FLOAT,
EOH_QTY	FLOAT
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh2.EATWH2_INVENTORY_Q2_2001;
CREATE TABLE eatwh2.EATWH2_INVENTORY_Q2_2001 (
MONTH_ID	INT,
ITEM_ID	INT,
BOH_QTY	FLOAT,
EOH_QTY	FLOAT
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh2.EATWH2_INVENTORY_Q3_2000;
CREATE TABLE eatwh2.EATWH2_INVENTORY_Q3_2000 (
MONTH_ID	INT,
ITEM_ID	INT,
BOH_QTY	FLOAT,
EOH_QTY	FLOAT
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh2.EATWH2_INVENTORY_Q3_2001;
CREATE TABLE eatwh2.EATWH2_INVENTORY_Q3_2001 (
MONTH_ID	INT,
ITEM_ID	INT,
BOH_QTY	FLOAT,
EOH_QTY	FLOAT
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh2.EATWH2_INVENTORY_Q4_2000;
CREATE TABLE eatwh2.EATWH2_INVENTORY_Q4_2000 (
MONTH_ID	INT,
ITEM_ID	INT,
BOH_QTY	FLOAT,
EOH_QTY	FLOAT
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh2.EATWH2_INVENTORY_Q4_2001;
CREATE TABLE eatwh2.EATWH2_INVENTORY_Q4_2001 (
MONTH_ID	INT,
ITEM_ID	INT,
BOH_QTY	FLOAT,
EOH_QTY	FLOAT
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh2.EATWH2_ITEM_EMP_SLS;
CREATE TABLE eatwh2.EATWH2_ITEM_EMP_SLS (
ITEM_ID	INT,
EMP_ID	INT,
TOT_DOLLAR_SALES	FLOAT,
TOT_UNIT_SALES	FLOAT,
TOT_COST	FLOAT
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh2.EATWH2_ITEM_MNTH_CTR_SLS;
CREATE TABLE eatwh2.EATWH2_ITEM_MNTH_CTR_SLS (
ITEM_ID	INT,
MONTH_ID	INT,
CALL_CTR_ID	INT,
TOT_DOLLAR_SALES	FLOAT,
TOT_UNIT_SALES	FLOAT,
TOT_COST	FLOAT
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh2.EATWH2_ITEM_MNTH_SLS;
CREATE TABLE eatwh2.EATWH2_ITEM_MNTH_SLS (
ITEM_ID	INT,
MONTH_ID	INT,
TOT_DOLLAR_SALES	FLOAT,
TOT_UNIT_SALES	FLOAT,
TOT_COST	FLOAT
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh2.EATWH2_LU_BRAND;
CREATE TABLE eatwh2.EATWH2_LU_BRAND (
BRAND_ID	INT,
BRAND_DESC	STRING
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh2.EATWH2_LU_CALL_CTR;
CREATE TABLE eatwh2.EATWH2_LU_CALL_CTR (
CALL_CTR_ID	INT,
CENTER_NAME	STRING,
REGION_ID	INT,
MANAGER_ID	INT,
COUNTRY_ID	INT,
DIST_CTR_ID	INT
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh2.EATWH2_LU_CATALOG;
CREATE TABLE eatwh2.EATWH2_LU_CATALOG (
CAT_ID	INT,
CAT_DESC	STRING,
CAT_URL	STRING,
CAT_SHIP_COUNT	INT
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh2.EATWH2_LU_CATEGORY;
CREATE TABLE eatwh2.EATWH2_LU_CATEGORY (
CATEGORY_ID	INT,
CATEGORY_DESC	STRING
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh2.EATWH2_LU_COUNTRY;
CREATE TABLE eatwh2.EATWH2_LU_COUNTRY (
COUNTRY_ID	INT,
COUNTRY_NAME	STRING
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh2.EATWH2_LU_COUNTRY_MANAGER;
CREATE TABLE eatwh2.EATWH2_LU_COUNTRY_MANAGER (
COUNTRY_ID	SMALLINT,
COUNTRY_MANAGER_ID	SMALLINT,
COUNTRY_MANAGER_NAME	STRING,
EFF_DT	string,
END_DT	string
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh2.EATWH2_LU_CUST_CITY;
CREATE TABLE eatwh2.EATWH2_LU_CUST_CITY (
CUST_CITY_ID	INT,
CUST_CITY_NAME	STRING,
CUST_STATE_ID	INT
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh2.EATWH2_LU_CUST_REGION;
CREATE TABLE eatwh2.EATWH2_LU_CUST_REGION (
CUST_REGION_ID	INT,
CUST_REGION_NAME	STRING
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh2.EATWH2_LU_CUST_STATE;
CREATE TABLE eatwh2.EATWH2_LU_CUST_STATE (
CUST_STATE_ID	INT,
CUST_STATE_NAME	STRING,
CUST_REGION_ID	INT
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh2.EATWH2_LU_CUSTOMER;
CREATE TABLE eatwh2.EATWH2_LU_CUSTOMER (
CUSTOMER_ID	INT,
CUST_LAST_NAME	STRING,
CUST_FIRST_NAME	STRING,
CUST_BIRTHDATE	string,
EMAIL	STRING,
ADDRESS	STRING,
ZIPCODE	STRING,
INCOME_ID	INT,
CUST_CITY_ID	INT
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh2.EATWH2_LU_DAY;
CREATE TABLE eatwh2.EATWH2_LU_DAY (
DAY_DATE	string,
MONTH_ID	INT,
QUARTER_ID	INT,
YEAR_ID	INT,
PREV_DAY_DATE	string,
LM_DAY_DATE	string,
LQ_DAY_DATE	string,
LY_DAY_DATE	string
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh2.EATWH2_LU_DIST_CTR;
CREATE TABLE eatwh2.EATWH2_LU_DIST_CTR (
COUNTRY_ID	INT,
DIST_CTR_ID	INT,
DIST_CTR_NAME	STRING
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh2.EATWH2_LU_EMPLOYEE;
CREATE TABLE eatwh2.EATWH2_LU_EMPLOYEE (
EMP_ID	INT,
EMP_LAST_NAME	STRING,
EMP_FIRST_NAME	STRING,
EMP_SSN	STRING,
BIRTH_DATE	string,
HIRE_DATE	string,
SALARY	INT,
COUNTRY_ID	INT,
DIST_CTR_ID	INT,
MANAGER_ID	INT,
CALL_CTR_ID	INT
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh2.EATWH2_LU_INCOME;
CREATE TABLE eatwh2.EATWH2_LU_INCOME (
INCOME_ID	INT,
BRACKET_DESC	STRING
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh2.EATWH2_LU_ITEM;
CREATE TABLE eatwh2.EATWH2_LU_ITEM (
ITEM_ID	INT,
ITEM_NAME	STRING,
ITEM_LONG_DESC	STRING,
ITEM_FOREIGN_NAME	STRING,
ITEM_URL	STRING,
DISC_CD	INT,
WARRANTY	STRING,
UNIT_PRICE	FLOAT,
UNIT_COST	FLOAT,
SUBCAT_ID	INT,
SUPPLIER_ID	INT,
BRAND_ID	INT
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh2.EATWH2_LU_MANAGER;
CREATE TABLE eatwh2.EATWH2_LU_MANAGER (
MANAGER_ID	INT,
MGR_LAST_NAME	STRING,
MGR_FIRST_NAME	STRING,
EMAIL	STRING,
ADDRESS_DISPLAY	STRING,
DEVICE_ID	STRING
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh2.EATWH2_LU_MONTH;
CREATE TABLE eatwh2.EATWH2_LU_MONTH (
MONTH_ID	INT,
MONTH_DATE	string,
MONTH_DESC	STRING,
MONTH_OF_YEAR	INT,
QUARTER_ID	INT,
YEAR_ID	INT,
MONTH_DURATION	INT,
PREV_MONTH_ID	INT,
LQ_MONTH_ID	INT,
LY_MONTH_ID	INT
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh2.EATWH2_LU_MONTH_OF_YEAR;
CREATE TABLE eatwh2.EATWH2_LU_MONTH_OF_YEAR (
MONTH_OF_YEAR	INT,
MONTH_OF_YEAR_NAME	STRING
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh2.EATWH2_LU_ORDER;
CREATE TABLE eatwh2.EATWH2_LU_ORDER (
ORDER_ID	INT,
CUSTOMER_ID	INT,
PYMT_TYPE	INT,
SHIPPER_ID	INT
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh2.EATWH2_LU_PROMO_TYPE;
CREATE TABLE eatwh2.EATWH2_LU_PROMO_TYPE (
PROMO_TYPE_ID	INT,
PROMO_TYPE_DESC	STRING
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh2.EATWH2_LU_PROMOTION;
CREATE TABLE eatwh2.EATWH2_LU_PROMOTION (
PROMO_SALE_ID	INT,
PROMO_SALE	STRING,
PROMO_TYPE_ID	INT
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh2.EATWH2_LU_PYMT_TYPE;
CREATE TABLE eatwh2.EATWH2_LU_PYMT_TYPE (
PYMT_TYPE	INT,
PYMT_DESC	STRING
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh2.EATWH2_LU_QUARTER;
CREATE TABLE eatwh2.EATWH2_LU_QUARTER (
QUARTER_ID	INT,
QUARTER_DATE	string,
QUARTER_DESC	STRING,
YEAR_ID	INT,
QUARTER_DURATION	INT,
PREV_QUARTER_ID	INT,
LY_QUARTER_ID	INT
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh2.EATWH2_LU_REGION;
CREATE TABLE eatwh2.EATWH2_LU_REGION (
REGION_ID	INT,
REGION_NAME	STRING,
COUNTRY_ID	INT
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh2.EATWH2_LU_SHIPPER;
CREATE TABLE eatwh2.EATWH2_LU_SHIPPER (
SHIPPER_ID	INT,
SHIPPER_DESC	STRING,
CONTRACT_NBR	INT
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh2.EATWH2_LU_SUBCATEG;
CREATE TABLE eatwh2.EATWH2_LU_SUBCATEG (
SUBCAT_ID	INT,
SUBCAT_DESC	STRING,
CATEGORY_ID	INT
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh2.EATWH2_LU_SUPPLIER;
CREATE TABLE eatwh2.EATWH2_LU_SUPPLIER (
SUPPLIER_ID	INT,
SUPPLIER_NAME	STRING,
CONTACT_LAST_NAME	STRING,
CONTACT_FIRST_NAME	STRING
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh2.EATWH2_LU_YEAR;
CREATE TABLE eatwh2.EATWH2_LU_YEAR (
YEAR_ID	INT,
YEAR_DATE	string,
YEAR_DURATION	INT,
PREV_YEAR_ID	INT
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh2.EATWH2_MNTH_CATEGORY_SLS;
CREATE TABLE eatwh2.EATWH2_MNTH_CATEGORY_SLS (
MONTH_ID	INT,
CATEGORY_ID	INT,
TOT_DOLLAR_SALES	FLOAT,
TOT_UNIT_SALES	FLOAT,
TOT_COST	FLOAT
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh2.EATWH2_MSI_STATS_LOG;
CREATE TABLE eatwh2.EATWH2_MSI_STATS_LOG (
SESSIONID	STRING,
PROJECTID	INT,
EVENTTIME	string,
EVENT	INT
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh2.EATWH2_MSI_STATS_PROP;
CREATE TABLE eatwh2.EATWH2_MSI_STATS_PROP (
PROP_NAME	STRING,
PROP_VAL	STRING
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh2.EATWH2_MTD_DAY;
CREATE TABLE eatwh2.EATWH2_MTD_DAY (
DAY_DATE	string,
MTD_DAY_DATE	string
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh2.EATWH2_ORDER_DETAIL;
CREATE TABLE eatwh2.EATWH2_ORDER_DETAIL (
ORDER_ID	INT,
ITEM_ID	INT,
ORDER_DATE	string,
EMP_ID	INT,
PROMOTION_ID	INT,
QTY_SOLD	FLOAT,
UNIT_PRICE	FLOAT,
UNIT_COST	FLOAT,
DISCOUNT	FLOAT
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh2.EATWH2_ORDER_FACT;
CREATE TABLE eatwh2.EATWH2_ORDER_FACT (
ORDER_ID	INT,
ORDER_DATE	string,
EMP_ID	INT,
ORDER_AMT	FLOAT,
ORDER_COST	FLOAT,
QTY_SOLD	FLOAT,
FREIGHT	FLOAT,
SHIP_DATE	string,
RUSH_ORDER	STRING
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh2.EATWH2_PMT_INVENTORY;
CREATE TABLE eatwh2.EATWH2_PMT_INVENTORY (
QUARTER_ID	INT,
PBTNAME	STRING
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh2.EATWH2_PROMOTIONS;
CREATE TABLE eatwh2.EATWH2_PROMOTIONS (
ITEM_ID	INT,
DAY_DATE	string,
PROMO_SALE_ID	INT,
DISCOUNT	FLOAT
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh2.EATWH2_QTD_DAY;
CREATE TABLE eatwh2.EATWH2_QTD_DAY (
DAY_DATE	string,
QTD_DAY_DATE	string
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh2.EATWH2_QTR_CATEGORY_SLS;
CREATE TABLE eatwh2.EATWH2_QTR_CATEGORY_SLS (
QUARTER_ID	INT,
CATEGORY_ID	INT,
TOT_DOLLAR_SALES	FLOAT,
TOT_UNIT_SALES	FLOAT,
TOT_COST	FLOAT
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh2.EATWH2_REL_CAT_ITEM;
CREATE TABLE eatwh2.EATWH2_REL_CAT_ITEM (
CAT_ID	INT,
ITEM_ID	INT
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh2.EATWH2_REL_GROUPS;
CREATE TABLE eatwh2.EATWH2_REL_GROUPS (
GROUP_ID	INT,
PARENT_GROUP_ID	INT
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh2.EATWH2_REL_USERS_GROUPS;
CREATE TABLE eatwh2.EATWH2_REL_USERS_GROUPS (
GROUP_ID	INT,
USER_ID	INT
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh2.EATWH2_RELATE_PROJECTS_CATEGORIES;
CREATE TABLE eatwh2.EATWH2_RELATE_PROJECTS_CATEGORIES (
PROJECT_ID	INT,
CATEGORY_ID	INT,
SUBCATEGORY_ID	INT,
RANKS	INT
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh2.EATWH2_RUSH_ORDER;
CREATE TABLE eatwh2.EATWH2_RUSH_ORDER (
ORDER_ID	INT,
RUSH_CHARGE	FLOAT
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh2.EATWH2_STATE_REGION_MNTH_SLS;
CREATE TABLE eatwh2.EATWH2_STATE_REGION_MNTH_SLS (
CUST_STATE_ID	INT,
REGION_ID	INT,
MONTH_ID	INT,
TOT_DOLLAR_SALES	FLOAT,
TOT_UNIT_SALES	FLOAT,
TOT_COST	FLOAT
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh2.EATWH2_STATE_SUBCATEG_MNTH_SLS;
CREATE TABLE eatwh2.EATWH2_STATE_SUBCATEG_MNTH_SLS (
CUST_STATE_ID	INT,
SUBCAT_ID	INT,
MONTH_ID	INT,
TOT_DOLLAR_SALES	FLOAT,
TOT_UNIT_SALES	FLOAT,
TOT_COST	FLOAT
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh2.EATWH2_STATE_SUBCATEG_REGION_SLS;
CREATE TABLE eatwh2.EATWH2_STATE_SUBCATEG_REGION_SLS (
CUST_STATE_ID	INT,
SUBCAT_ID	INT,
REGION_ID	INT,
TOT_DOLLAR_SALES	FLOAT,
TOT_UNIT_SALES	FLOAT,
TOT_COST	FLOAT
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh2.EATWH2_SUBCATEG_MNTH_CTR_SLS;
CREATE TABLE eatwh2.EATWH2_SUBCATEG_MNTH_CTR_SLS (
SUBCAT_ID	INT,
MONTH_ID	INT,
CALL_CTR_ID	INT,
TOT_DOLLAR_SALES	FLOAT,
TOT_UNIT_SALES	FLOAT,
TOT_COST	FLOAT
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh2.EATWH2_TE_TRANS_STATISTICS;
CREATE TABLE eatwh2.EATWH2_TE_TRANS_STATISTICS (
SERVER_ID	STRING,
MACHINE	STRING,
TRANS_ID	STRING,
TRANS_TYPE	STRING,
TRANS_PARA	STRING,
TRANS_RESULT	STRING,
START_TIME	FLOAT,
END_TIME	FLOAT,
CUR_TRANS	INT
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh2.EATWH2_YR_CATEGORY_SLS;
CREATE TABLE eatwh2.EATWH2_YR_CATEGORY_SLS (
YEAR_ID	INT,
CATEGORY_ID	INT,
TOT_DOLLAR_SALES	FLOAT,
TOT_UNIT_SALES	FLOAT,
TOT_COST	FLOAT
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh2.EATWH2_YTD_DAY;
CREATE TABLE eatwh2.EATWH2_YTD_DAY (
DAY_DATE	string,
YTD_DAY_DATE	string
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;

DROP TABLE IF EXISTS eatwh2.EATWH2_YTM_MONTH;
CREATE TABLE eatwh2.EATWH2_YTM_MONTH (
MONTH_ID	INT,
YTM_MONTH_ID	INT
) row format delimited fields terminated by ','  escaped by '\\' stored as textfile;
