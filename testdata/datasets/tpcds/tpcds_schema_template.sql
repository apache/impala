# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
# For details on this file format please see hive-benchmark_schema_template.sql
====
---- DATASET
tpcds
---- BASE_TABLE_NAME
customer_demographics
---- COLUMNS
cd_demo_sk                bigint
cd_gender                 string
cd_marital_status         string
cd_education_status       string
cd_purchase_estimate      int
cd_credit_rating          string
cd_dep_count              int
cd_dep_employed_count     int
cd_dep_college_count      int
---- ROW_FORMAT
delimited fields terminated by '|'
---- DEPENDENT_LOAD
INSERT OVERWRITE TABLE {db_name}{db_suffix}.{table_name}
SELECT * FROM {db_name}.{table_name};
---- LOAD
LOAD DATA LOCAL INPATH
'{impala_home}/testdata/impala-data/{db_name}/customer_demographics/'
OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name};
====
---- DATASET
tpcds
---- BASE_TABLE_NAME
date_dim
---- COLUMNS
d_date_sk                 bigint
d_date_id                 string
d_date                    string
d_month_seq               int
d_week_seq                int
d_quarter_seq             int
d_year                    int
d_dow                     int
d_moy                     int
d_dom                     int
d_qoy                     int
d_fy_year                 int
d_fy_quarter_seq          int
d_fy_week_seq             int
d_day_name                string
d_quarter_name            string
d_holiday                 string
d_weekend                 string
d_following_holiday       string
d_first_dom               int
d_last_dom                int
d_same_day_ly             int
d_same_day_lq             int
d_current_day             string
d_current_week            string
d_current_month           string
d_current_quarter         string
d_current_year            string
---- ROW_FORMAT
delimited fields terminated by '|'
---- DEPENDENT_LOAD
INSERT OVERWRITE TABLE {db_name}{db_suffix}.{table_name}
SELECT * FROM {db_name}.{table_name};
---- LOAD
LOAD DATA LOCAL INPATH '{impala_home}/testdata/impala-data/{db_name}/date_dim/'
OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name};
====
---- DATASET
tpcds
---- BASE_TABLE_NAME
time_dim
---- COLUMNS
t_time_sk                 bigint
t_time_id                 string
t_time                    int
t_hour                    int
t_minute                  int
t_second                  int
t_am_pm                   string
t_shift                   string
t_sub_shift               string
t_meal_time               string
---- ROW_FORMAT
delimited fields terminated by '|'
---- DEPENDENT_LOAD
INSERT OVERWRITE TABLE {db_name}{db_suffix}.{table_name}
SELECT * FROM {db_name}.{table_name};
---- LOAD
LOAD DATA LOCAL INPATH '{impala_home}/testdata/impala-data/{db_name}/time_dim/'
OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name};
====
---- DATASET
tpcds
---- BASE_TABLE_NAME
item
---- COLUMNS
i_item_sk                 bigint
i_item_id                 string
i_rec_start_date          string
i_rec_end_date            string
i_item_desc               string
i_current_price           decimal(7,2)
i_wholesale_cost          decimal(7,2)
i_brand_id                int
i_brand                   string
i_class_id                int
i_class                   string
i_category_id             int
i_category                string
i_manufact_id             int
i_manufact                string
i_size                    string
i_formulation             string
i_color                   string
i_units                   string
i_container               string
i_manager_id              int
i_product_name            string
---- ROW_FORMAT
delimited fields terminated by '|'
---- DEPENDENT_LOAD
INSERT OVERWRITE TABLE {db_name}{db_suffix}.{table_name}
SELECT * FROM {db_name}.{table_name};
---- LOAD
LOAD DATA LOCAL INPATH '{impala_home}/testdata/impala-data/{db_name}/item/'
OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name};
====
---- DATASET
tpcds
---- BASE_TABLE_NAME
store
---- COLUMNS
s_store_sk                bigint
s_store_id                string
s_rec_start_date          string
s_rec_end_date            string
s_closed_date_sk          int
s_store_name              string
s_number_employees        int
s_floor_space             int
s_hours                   string
s_manager                 string
s_market_id               int
s_geography_class         string
s_market_desc             string
s_market_manager          string
s_division_id             int
s_division_name           string
s_company_id              int
s_company_name            string
s_street_number           string
s_street_name             string
s_street_type             string
s_suite_number            string
s_city                    string
s_county                  string
s_state                   string
s_zip                     string
s_country                 string
s_gmt_offset              decimal(5,2)
s_tax_precentage          decimal(5,2)
---- ROW_FORMAT
delimited fields terminated by '|'
---- DEPENDENT_LOAD
INSERT OVERWRITE TABLE {db_name}{db_suffix}.{table_name}
SELECT * FROM {db_name}.{table_name};
---- LOAD
LOAD DATA LOCAL INPATH '{impala_home}/testdata/impala-data/{db_name}/store/'
OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name};
====
---- DATASET
tpcds
---- BASE_TABLE_NAME
customer
---- COLUMNS
c_customer_sk             bigint
c_customer_id             string
c_current_cdemo_sk        int
c_current_hdemo_sk        int
c_current_addr_sk         int
c_first_shipto_date_sk    int
c_first_sales_date_sk     int
c_salutation              string
c_first_name              string
c_last_name               string
c_preferred_cust_flag     string
c_birth_day               int
c_birth_month             int
c_birth_year              int
c_birth_country           string
c_login                   string
c_email_address           string
c_last_review_date        string
---- ROW_FORMAT
delimited fields terminated by '|'
---- DEPENDENT_LOAD
INSERT OVERWRITE TABLE {db_name}{db_suffix}.{table_name}
SELECT * FROM {db_name}.{table_name};
---- LOAD
LOAD DATA LOCAL INPATH '{impala_home}/testdata/impala-data/{db_name}/customer/'
OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name};
====
---- DATASET
tpcds
---- BASE_TABLE_NAME
promotion
---- COLUMNS
p_promo_sk                bigint
p_promo_id                string
p_start_date_sk           int
p_end_date_sk             int
p_item_sk                 int
p_cost                    decimal(15,2)
p_response_target         int
p_promo_name              string
p_channel_dmail           string
p_channel_email           string
p_channel_catalog         string
p_channel_tv              string
p_channel_radio           string
p_channel_press           string
p_channel_event           string
p_channel_demo            string
p_channel_details         string
p_purpose                 string
p_discount_active         string
---- ROW_FORMAT
delimited fields terminated by '|'
---- DEPENDENT_LOAD
INSERT OVERWRITE TABLE {db_name}{db_suffix}.{table_name}
SELECT * FROM {db_name}.{table_name};
---- LOAD
LOAD DATA LOCAL INPATH '{impala_home}/testdata/impala-data/{db_name}/promotion/'
OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name};
====
---- DATASET
tpcds
---- BASE_TABLE_NAME
household_demographics
---- COLUMNS
hd_demo_sk                bigint
hd_income_band_sk         int
hd_buy_potential          string
hd_dep_count              int
hd_vehicle_count          int
---- ROW_FORMAT
delimited fields terminated by '|'
---- DEPENDENT_LOAD
INSERT OVERWRITE TABLE {db_name}{db_suffix}.{table_name}
SELECT * FROM {db_name}.{table_name};
---- LOAD
LOAD DATA LOCAL INPATH
'{impala_home}/testdata/impala-data/{db_name}/household_demographics/'
OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name};
====
---- DATASET
tpcds
---- BASE_TABLE_NAME
customer_address
---- COLUMNS
ca_address_sk             bigint
ca_address_id             string
ca_street_number          string
ca_street_name            string
ca_street_type            string
ca_suite_number           string
ca_city                   string
ca_county                 string
ca_state                  string
ca_zip                    string
ca_country                string
ca_gmt_offset             decimal(5,2)
ca_location_type          string
---- ROW_FORMAT
delimited fields terminated by '|'
---- DEPENDENT_LOAD
INSERT OVERWRITE TABLE {db_name}{db_suffix}.{table_name}
SELECT * FROM {db_name}.{table_name};
---- LOAD
LOAD DATA LOCAL INPATH '{impala_home}/testdata/impala-data/{db_name}/customer_address/'
OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name};
====
---- DATASET
tpcds
---- BASE_TABLE_NAME
store_sales_unpartitioned
---- COLUMNS
ss_sold_date_sk           bigint
ss_sold_time_sk           bigint
ss_item_sk                bigint
ss_customer_sk            bigint
ss_cdemo_sk               bigint
ss_hdemo_sk               bigint
ss_addr_sk                bigint
ss_store_sk               bigint
ss_promo_sk               bigint
ss_ticket_number          int
ss_quantity               int
ss_wholesale_cost         decimal(7,2)
ss_list_price             decimal(7,2)
ss_sales_price            decimal(7,2)
ss_ext_discount_amt       decimal(7,2)
ss_ext_sales_price        decimal(7,2)
ss_ext_wholesale_cost     decimal(7,2)
ss_ext_list_price         decimal(7,2)
ss_ext_tax                decimal(7,2)
ss_coupon_amt             decimal(7,2)
ss_net_paid               decimal(7,2)
ss_net_paid_inc_tax       decimal(7,2)
ss_net_profit             decimal(7,2)
---- ROW_FORMAT
delimited fields terminated by '|'
---- DEPENDENT_LOAD
INSERT OVERWRITE TABLE {db_name}{db_suffix}.{table_name}
SELECT * FROM {db_name}.{table_name};
---- LOAD
LOAD DATA LOCAL INPATH '{impala_home}/testdata/impala-data/{db_name}/store_sales/'
OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name};
====
---- DATASET
tpcds
---- BASE_TABLE_NAME
store_sales
---- COLUMNS
ss_sold_time_sk           bigint
ss_item_sk                bigint
ss_customer_sk            bigint
ss_cdemo_sk               bigint
ss_hdemo_sk               bigint
ss_addr_sk                bigint
ss_store_sk               bigint
ss_promo_sk               bigint
ss_ticket_number          int
ss_quantity               int
ss_wholesale_cost         decimal(7,2)
ss_list_price             decimal(7,2)
ss_sales_price            decimal(7,2)
ss_ext_discount_amt       decimal(7,2)
ss_ext_sales_price        decimal(7,2)
ss_ext_wholesale_cost     decimal(7,2)
ss_ext_list_price         decimal(7,2)
ss_ext_tax                decimal(7,2)
ss_coupon_amt             decimal(7,2)
ss_net_paid               decimal(7,2)
ss_net_paid_inc_tax       decimal(7,2)
ss_net_profit             decimal(7,2)
---- PARTITION_COLUMNS
ss_sold_date_sk bigint
---- ROW_FORMAT
delimited fields terminated by '|'
---- DEPENDENT_LOAD
-- Split the load into multiple steps to reduce total memory usage for larger
-- scale factors. TODO: Dynamically scale this based on the scale factor?
INSERT OVERWRITE TABLE {db_name}{db_suffix}.{table_name} PARTITION (ss_sold_date_sk)
SELECT ss_sold_time_sk,
  ss_item_sk,
  ss_customer_sk,
  ss_cdemo_sk,
  ss_hdemo_sk,
  ss_addr_sk,
  ss_store_sk,
  ss_promo_sk,
  ss_ticket_number,
  ss_quantity,
  ss_wholesale_cost,
  ss_list_price,
  ss_sales_price,
  ss_ext_discount_amt,
  ss_ext_sales_price,
  ss_ext_wholesale_cost,
  ss_ext_list_price,
  ss_ext_tax,
  ss_coupon_amt,
  ss_net_paid,
  ss_net_paid_inc_tax,
  ss_net_profit,
  ss_sold_date_sk
FROM {db_name}.{table_name}
WHERE ss_sold_date_sk < 2451272;

INSERT OVERWRITE TABLE {db_name}{db_suffix}.{table_name} PARTITION (ss_sold_date_sk)
SELECT ss_sold_time_sk,
  ss_item_sk,
  ss_customer_sk,
  ss_cdemo_sk,
  ss_hdemo_sk,
  ss_addr_sk,
  ss_store_sk,
  ss_promo_sk,
  ss_ticket_number,
  ss_quantity,
  ss_wholesale_cost,
  ss_list_price,
  ss_sales_price,
  ss_ext_discount_amt,
  ss_ext_sales_price,
  ss_ext_wholesale_cost,
  ss_ext_list_price,
  ss_ext_tax,
  ss_coupon_amt,
  ss_net_paid,
  ss_net_paid_inc_tax,
  ss_net_profit,
  ss_sold_date_sk
FROM {db_name}.{table_name}
WHERE 2451272 <= ss_sold_date_sk and ss_sold_date_sk < 2451728;

INSERT OVERWRITE TABLE {db_name}{db_suffix}.{table_name} PARTITION (ss_sold_date_sk)
SELECT ss_sold_time_sk,
  ss_item_sk,
  ss_customer_sk,
  ss_cdemo_sk,
  ss_hdemo_sk,
  ss_addr_sk,
  ss_store_sk,
  ss_promo_sk,
  ss_ticket_number,
  ss_quantity,
  ss_wholesale_cost,
  ss_list_price,
  ss_sales_price,
  ss_ext_discount_amt,
  ss_ext_sales_price,
  ss_ext_wholesale_cost,
  ss_ext_list_price,
  ss_ext_tax,
  ss_coupon_amt,
  ss_net_paid,
  ss_net_paid_inc_tax,
  ss_net_profit,
  ss_sold_date_sk
FROM {db_name}.{table_name}
WHERE 2451728 <= ss_sold_date_sk and ss_sold_date_sk < 2452184;

INSERT OVERWRITE TABLE {db_name}{db_suffix}.{table_name} PARTITION (ss_sold_date_sk)
SELECT ss_sold_time_sk,
  ss_item_sk,
  ss_customer_sk,
  ss_cdemo_sk,
  ss_hdemo_sk,
  ss_addr_sk,
  ss_store_sk,
  ss_promo_sk,
  ss_ticket_number,
  ss_quantity,
  ss_wholesale_cost,
  ss_list_price,
  ss_sales_price,
  ss_ext_discount_amt,
  ss_ext_sales_price,
  ss_ext_wholesale_cost,
  ss_ext_list_price,
  ss_ext_tax,
  ss_coupon_amt,
  ss_net_paid,
  ss_net_paid_inc_tax,
  ss_net_profit,
  ss_sold_date_sk
FROM {db_name}.{table_name}
WHERE 2452184 <= ss_sold_date_sk;
---- LOAD
USE {db_name};

set hive.exec.max.dynamic.partitions.pernode=10000;
set hive.exec.max.dynamic.partitions=10000;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition=true;

insert overwrite table {table_name} partition(ss_sold_date_sk)
select ss_sold_time_sk,
  ss_item_sk,
  ss_customer_sk,
  ss_cdemo_sk,
  ss_hdemo_sk,
  ss_addr_sk,
  ss_store_sk,
  ss_promo_sk,
  ss_ticket_number,
  ss_quantity,
  ss_wholesale_cost,
  ss_list_price,
  ss_sales_price,
  ss_ext_discount_amt,
  ss_ext_sales_price,
  ss_ext_wholesale_cost,
  ss_ext_list_price,
  ss_ext_tax,
  ss_coupon_amt,
  ss_net_paid,
  ss_net_paid_inc_tax,
  ss_net_profit,
  ss_sold_date_sk
from store_sales_unpartitioned
distribute by ss_sold_date_sk;
---- LOAD_LOCAL
USE {db_name};

set hive.exec.max.dynamic.partitions.pernode=10000;
set hive.exec.max.dynamic.partitions=10000;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition=true;

insert overwrite table store_sales partition(ss_sold_date_sk)
select ss.ss_sold_time_sk,
  ss.ss_item_sk,
  ss.ss_customer_sk,
  ss.ss_cdemo_sk,
  ss.ss_hdemo_sk,
  ss.ss_addr_sk,
  ss.ss_store_sk,
  ss.ss_promo_sk,
  ss.ss_ticket_number,
  ss.ss_quantity,
  ss.ss_wholesale_cost,
  ss.ss_list_price,
  ss.ss_sales_price,
  ss.ss_ext_discount_amt,
  ss.ss_ext_sales_price,
  ss.ss_ext_wholesale_cost,
  ss.ss_ext_list_price,
  ss.ss_ext_tax,
  ss.ss_coupon_amt,
  ss.ss_net_paid,
  ss.ss_net_paid_inc_tax,
  ss.ss_net_profit,
  ss.ss_sold_date_sk
from date_dim d
join store_sales_unpartitioned ss
  on (ss.ss_sold_date_sk = d.d_date_sk)
-- The filter below reduced the number of partitions generated for local testing. This
-- filter reduces the number of partitions from ~1800 to 120. We are doing a join with
-- date_dim in order to select the 1st and 15th days of the month. No data in date_dim
-- ends up in store_sales.
where (d.d_date like '%-01' or d.d_date like '%-15')
distribute by ss_sold_date_sk;
====
