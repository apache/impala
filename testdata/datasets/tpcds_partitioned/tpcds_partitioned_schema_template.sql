# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
# For details on this file format please see ../README
====
---- DATASET
tpcds_partitioned
---- BASE_TABLE_NAME
time_dim
---- COLUMNS
t_time_sk                 int
t_time_id                 string
t_time                    int
t_hour                    int
t_minute                  int
t_second                  int
t_am_pm                   string
t_shift                   string
t_sub_shift               string
t_meal_time               string
primary key (t_time_sk) DISABLE NOVALIDATE RELY
---- DEPENDENT_LOAD
INSERT OVERWRITE TABLE {db_name}{db_suffix}.{table_name}
SELECT * FROM tpcds.{table_name};
====
---- DATASET
tpcds_partitioned
---- BASE_TABLE_NAME
date_dim
---- COLUMNS
d_date_sk                 int
d_date_id                 string
d_date                    date
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
primary key (d_date_sk) DISABLE NOVALIDATE RELY
---- DEPENDENT_LOAD
INSERT OVERWRITE TABLE {db_name}{db_suffix}.{table_name}
SELECT * FROM tpcds.{table_name};
====
---- DATASET
tpcds_partitioned
---- BASE_TABLE_NAME
reason
---- COLUMNS
r_reason_sk           int
r_reason_id           string
r_reason_desc         string
primary key (r_reason_sk) DISABLE NOVALIDATE RELY
---- DEPENDENT_LOAD
INSERT OVERWRITE TABLE {db_name}{db_suffix}.{table_name}
SELECT * FROM tpcds.{table_name};
====
---- DATASET
tpcds_partitioned
---- BASE_TABLE_NAME
customer_address
---- COLUMNS
ca_address_sk             int
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
primary key (ca_address_sk) DISABLE NOVALIDATE RELY
---- DEPENDENT_LOAD
INSERT OVERWRITE TABLE {db_name}{db_suffix}.{table_name}
SELECT * FROM tpcds.{table_name};
====
---- DATASET
tpcds_partitioned
---- BASE_TABLE_NAME
income_band
---- COLUMNS
ib_income_band_sk         int
ib_lower_bound            int
ib_upper_bound            int
primary key (ib_income_band_sk) DISABLE NOVALIDATE RELY
---- DEPENDENT_LOAD
INSERT OVERWRITE TABLE {db_name}{db_suffix}.{table_name}
SELECT * FROM tpcds.{table_name};
====
---- DATASET
tpcds_partitioned
---- BASE_TABLE_NAME
household_demographics
---- COLUMNS
hd_demo_sk                int
hd_income_band_sk         int
hd_buy_potential          string
hd_dep_count              int
hd_vehicle_count          int
primary key (hd_demo_sk) DISABLE NOVALIDATE RELY
foreign key (hd_income_band_sk) references {db_name}{db_suffix}.income_band (ib_income_band_sk) DISABLE NOVALIDATE RELY
---- DEPENDENT_LOAD
INSERT OVERWRITE TABLE {db_name}{db_suffix}.{table_name}
SELECT * FROM tpcds.{table_name};
====
---- DATASET
tpcds_partitioned
---- BASE_TABLE_NAME
customer_demographics
---- COLUMNS
cd_demo_sk                int
cd_gender                 string
cd_marital_status         string
cd_education_status       string
cd_purchase_estimate      int
cd_credit_rating          string
cd_dep_count              int
cd_dep_employed_count     int
cd_dep_college_count      int
primary key (cd_demo_sk) DISABLE NOVALIDATE RELY
---- DEPENDENT_LOAD
INSERT OVERWRITE TABLE {db_name}{db_suffix}.{table_name}
SELECT * FROM tpcds.{table_name};
====
---- DATASET
tpcds_partitioned
---- BASE_TABLE_NAME
ship_mode
---- COLUMNS
sm_ship_mode_sk           int
sm_ship_mode_id           string
sm_type                   string
sm_code                   string
sm_carrier                string
sm_contract               string
primary key (sm_ship_mode_sk) DISABLE NOVALIDATE RELY
---- DEPENDENT_LOAD
INSERT OVERWRITE TABLE {db_name}{db_suffix}.{table_name}
SELECT * FROM tpcds.{table_name};
====
---- DATASET
tpcds_partitioned
---- BASE_TABLE_NAME
item
---- COLUMNS
i_item_sk                 int
i_item_id                 string
i_rec_start_date          date
i_rec_end_date            date
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
primary key (i_item_sk) DISABLE NOVALIDATE RELY
---- DEPENDENT_LOAD
INSERT OVERWRITE TABLE {db_name}{db_suffix}.{table_name}
(
  i_item_sk,
  i_item_id,
  i_rec_start_date,
  i_rec_end_date,
  i_item_desc,
  i_current_price,
  i_wholesale_cost,
  i_brand_id,
  i_brand,
  i_class_id,
  i_class,
  i_category_id,
  i_category,
  i_manufact_id,
  i_manufact,
  i_size,
  i_formulation,
  i_color,
  i_units,
  i_container,
  i_manager_id,
  i_product_name
)
SELECT
  CAST(i_item_sk AS INT),
  i_item_id,
  i_rec_start_date,
  i_rec_end_date,
  i_item_desc,
  i_current_price,
  i_wholesale_cost,
  i_brand_id,
  i_brand,
  i_class_id,
  i_class,
  i_category_id,
  i_category,
  i_manufact_id,
  i_manufact,
  i_size,
  i_formulation,
  i_color,
  i_units,
  i_container,
  i_manager_id,
  i_product_name
FROM tpcds.{table_name};
====
---- DATASET
tpcds_partitioned
---- BASE_TABLE_NAME
warehouse
---- COLUMNS
w_warehouse_sk            int
w_warehouse_id            string
w_warehouse_name          string
w_warehouse_sq_ft         int
w_street_number           string
w_street_name             string
w_street_type             string
w_suite_number            string
w_city                    string
w_county                  string
w_state                   string
w_zip                     string
w_country                 string
w_gmt_offset              decimal(5,2)
primary key (w_warehouse_sk) DISABLE NOVALIDATE RELY
---- DEPENDENT_LOAD
INSERT OVERWRITE TABLE {db_name}{db_suffix}.{table_name}
SELECT * FROM tpcds.{table_name};
====
---- DATASET
tpcds_partitioned
---- BASE_TABLE_NAME
call_center
---- COLUMNS
cc_call_center_sk         int
cc_call_center_id         string
cc_rec_start_date         date
cc_rec_end_date           date
cc_closed_date_sk         int
cc_open_date_sk           int
cc_name                   string
cc_class                  string
cc_employees              int
cc_sq_ft                  int
cc_hours                  string
cc_manager                string
cc_mkt_id                 int
cc_mkt_class              string
cc_mkt_desc               string
cc_market_manager         string
cc_division               int
cc_division_name          string
cc_company                int
cc_company_name           string
cc_street_number          string
cc_street_name            string
cc_street_type            string
cc_suite_number           string
cc_city                   string
cc_county                 string
cc_state                  string
cc_zip                    string
cc_country                string
cc_gmt_offset             decimal(5,2)
cc_tax_percentage         decimal(5,2)
primary key (cc_call_center_sk) DISABLE NOVALIDATE RELY
foreign key (cc_closed_date_sk) references {db_name}{db_suffix}.date_dim (d_date_sk) DISABLE NOVALIDATE RELY
foreign key (cc_open_date_sk) references {db_name}{db_suffix}.date_dim (d_date_sk) DISABLE NOVALIDATE RELY
---- DEPENDENT_LOAD
INSERT OVERWRITE TABLE {db_name}{db_suffix}.{table_name}
SELECT * FROM tpcds.{table_name};
====
---- DATASET
tpcds_partitioned
---- BASE_TABLE_NAME
customer
---- COLUMNS
c_customer_sk             int
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
c_last_review_date_sk     int
primary key (c_customer_sk) DISABLE NOVALIDATE RELY
foreign key (c_current_addr_sk) references {db_name}{db_suffix}.customer_address (ca_address_sk) DISABLE NOVALIDATE RELY
foreign key (c_current_cdemo_sk) references {db_name}{db_suffix}.customer_demographics (cd_demo_sk) DISABLE NOVALIDATE RELY
foreign key (c_current_hdemo_sk) references {db_name}{db_suffix}.household_demographics (hd_demo_sk) DISABLE NOVALIDATE RELY
foreign key (c_first_sales_date_sk) references {db_name}{db_suffix}.date_dim (d_date_sk) DISABLE NOVALIDATE RELY
foreign key (c_first_shipto_date_sk) references {db_name}{db_suffix}.date_dim (d_date_sk) DISABLE NOVALIDATE RELY
foreign key (c_last_review_date_sk) references {db_name}{db_suffix}.date_dim (d_date_sk) DISABLE NOVALIDATE RELY
---- DEPENDENT_LOAD
INSERT OVERWRITE TABLE {db_name}{db_suffix}.{table_name}
(
  c_customer_sk,
  c_customer_id,
  c_current_cdemo_sk,
  c_current_hdemo_sk,
  c_current_addr_sk,
  c_first_shipto_date_sk,
  c_first_sales_date_sk,
  c_salutation,
  c_first_name,
  c_last_name,
  c_preferred_cust_flag,
  c_birth_day,
  c_birth_month,
  c_birth_year,
  c_birth_country,
  c_login,
  c_email_address,
  c_last_review_date_sk
)
SELECT
  c_customer_sk,
  c_customer_id,
  c_current_cdemo_sk,
  c_current_hdemo_sk,
  c_current_addr_sk,
  c_first_shipto_date_sk,
  c_first_sales_date_sk,
  c_salutation,
  c_first_name,
  c_last_name,
  c_preferred_cust_flag,
  c_birth_day,
  c_birth_month,
  c_birth_year,
  c_birth_country,
  c_login,
  c_email_address,
  CAST(c_last_review_date AS INT)
FROM tpcds.{table_name};
====
---- DATASET
tpcds_partitioned
---- BASE_TABLE_NAME
promotion
---- COLUMNS
p_promo_sk                int
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
primary key (p_promo_sk) DISABLE NOVALIDATE RELY
foreign key (p_start_date_sk) references {db_name}{db_suffix}.date_dim (d_date_sk) DISABLE NOVALIDATE RELY
foreign key (p_end_date_sk) references {db_name}{db_suffix}.date_dim (d_date_sk) DISABLE NOVALIDATE RELY
foreign key (p_item_sk) references {db_name}{db_suffix}.item (i_item_sk) DISABLE NOVALIDATE RELY
---- DEPENDENT_LOAD
INSERT OVERWRITE TABLE {db_name}{db_suffix}.{table_name}
(
  p_promo_sk,
  p_promo_id,
  p_start_date_sk,
  p_end_date_sk,
  p_item_sk,
  p_cost,
  p_response_target,
  p_promo_name,
  p_channel_dmail,
  p_channel_email,
  p_channel_catalog,
  p_channel_tv,
  p_channel_radio,
  p_channel_press,
  p_channel_event,
  p_channel_demo,
  p_channel_details,
  p_purpose,
  p_discount_active
)
SELECT
  p_promo_sk,
  p_promo_id,
  p_start_date_sk,
  p_end_date_sk,
  CAST(p_item_sk AS INT),
  p_cost,
  p_response_target,
  p_promo_name,
  p_channel_dmail,
  p_channel_email,
  p_channel_catalog,
  p_channel_tv,
  p_channel_radio,
  p_channel_press,
  p_channel_event,
  p_channel_demo,
  p_channel_details,
  p_purpose,
  p_discount_active
FROM tpcds.{table_name};
====
---- DATASET
tpcds_partitioned
---- BASE_TABLE_NAME
store
---- COLUMNS
s_store_sk                int
s_store_id                string
s_rec_start_date          date
s_rec_end_date            date
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
primary key (s_store_sk) DISABLE NOVALIDATE RELY
foreign key (s_closed_date_sk) references {db_name}{db_suffix}.date_dim (d_date_sk) DISABLE NOVALIDATE RELY
---- DEPENDENT_LOAD
INSERT OVERWRITE TABLE {db_name}{db_suffix}.{table_name}
SELECT * FROM tpcds.{table_name};
====
---- DATASET
tpcds_partitioned
---- BASE_TABLE_NAME
catalog_page
---- COLUMNS
cp_catalog_page_sk        int
cp_catalog_page_id        string
cp_start_date_sk          int
cp_end_date_sk            int
cp_department             string
cp_catalog_number         int
cp_catalog_page_number    int
cp_description            string
cp_type                   string
primary key (cp_catalog_page_sk) DISABLE NOVALIDATE RELY
foreign key (cp_start_date_sk) references {db_name}{db_suffix}.date_dim (d_date_sk) DISABLE NOVALIDATE RELY
foreign key (cp_end_date_sk) references {db_name}{db_suffix}.date_dim (d_date_sk) DISABLE NOVALIDATE RELY
---- DEPENDENT_LOAD
INSERT OVERWRITE TABLE {db_name}{db_suffix}.{table_name}
SELECT * FROM tpcds.{table_name};
====
---- DATASET
tpcds_partitioned
---- BASE_TABLE_NAME
web_page
---- COLUMNS
wp_web_page_sk            int
wp_web_page_id            string
wp_rec_start_date         date
wp_rec_end_date           date
wp_creation_date_sk       int
wp_access_date_sk         int
wp_autogen_flag           string
wp_customer_sk            int
wp_url                    string
wp_type                   string
wp_char_count             int
wp_link_count             int
wp_image_count            int
wp_max_ad_count           int
primary key (wp_web_page_sk) DISABLE NOVALIDATE RELY
foreign key (wp_creation_date_sk) references {db_name}{db_suffix}.date_dim (d_date_sk) DISABLE NOVALIDATE RELY
foreign key (wp_access_date_sk) references {db_name}{db_suffix}.date_dim (d_date_sk) DISABLE NOVALIDATE RELY
foreign key (wp_customer_sk) references {db_name}{db_suffix}.customer (c_customer_sk) DISABLE NOVALIDATE RELY
---- DEPENDENT_LOAD
INSERT OVERWRITE TABLE {db_name}{db_suffix}.{table_name}
SELECT * FROM tpcds.{table_name};
====
---- DATASET
tpcds_partitioned
---- BASE_TABLE_NAME
web_site
---- COLUMNS
web_site_sk           int
web_site_id           string
web_rec_start_date    date
web_rec_end_date      date
web_name              string
web_open_date_sk      int
web_close_date_sk     int
web_class             string
web_manager           string
web_mkt_id            int
web_mkt_class         string
web_mkt_desc          string
web_market_manager    string
web_company_id        int
web_company_name      string
web_street_number     string
web_street_name       string
web_street_type       string
web_suite_number      string
web_city              string
web_county            string
web_state             string
web_zip               string
web_country           string
web_gmt_offset        decimal(5,2)
web_tax_percentage    decimal(5,2)
primary key (web_site_sk) DISABLE NOVALIDATE RELY
foreign key (web_open_date_sk) references {db_name}{db_suffix}.date_dim (d_date_sk) DISABLE NOVALIDATE RELY
foreign key (web_close_date_sk) references {db_name}{db_suffix}.date_dim (d_date_sk) DISABLE NOVALIDATE RELY
---- DEPENDENT_LOAD
INSERT OVERWRITE TABLE {db_name}{db_suffix}.{table_name}
SELECT * FROM tpcds.{table_name};
====
---- DATASET
tpcds_partitioned
---- BASE_TABLE_NAME
inventory
---- COLUMNS
inv_item_sk                int
inv_warehouse_sk           int
inv_quantity_on_hand       int
primary key (inv_item_sk, inv_warehouse_sk) DISABLE NOVALIDATE RELY
foreign key (inv_date_sk) references {db_name}{db_suffix}.date_dim (d_date_sk) DISABLE NOVALIDATE RELY
foreign key (inv_item_sk) references {db_name}{db_suffix}.item (i_item_sk) DISABLE NOVALIDATE RELY
foreign key (inv_warehouse_sk) references {db_name}{db_suffix}.warehouse (w_warehouse_sk) DISABLE NOVALIDATE RELY
---- PARTITION_COLUMNS
inv_date_sk                int
---- DEPENDENT_LOAD
-- The following query options are set to optimize small scale fact tables with 3 impalads.
-- Small MAX_SCAN_RANGE_LENGTH will split 1 file into few scan ranges that can be
-- distributed across 3 impalads in minicluster. Without this, only 1 fragment at 1
-- impalad does the reading and writing.
SET MAX_SCAN_RANGE_LENGTH=1mb;
-- MT_DOP=4 is to increase number of scanner and writer to 12 at max for single insert
-- overwrite query.
SET MT_DOP=4;

INSERT OVERWRITE TABLE {db_name}{db_suffix}.{table_name}
(
  inv_item_sk,
  inv_warehouse_sk,
  inv_quantity_on_hand
)
PARTITION (inv_date_sk)
SELECT
  CAST(inv_item_sk AS INT),
  inv_warehouse_sk,
  inv_quantity_on_hand,
  inv_date_sk
FROM tpcds.{table_name};
====
---- DATASET
tpcds_partitioned
---- BASE_TABLE_NAME
catalog_sales
---- COLUMNS
cs_sold_time_sk           int
cs_ship_date_sk           int
cs_bill_customer_sk       int
cs_bill_cdemo_sk          int
cs_bill_hdemo_sk          int
cs_bill_addr_sk           int
cs_ship_customer_sk       int
cs_ship_cdemo_sk          int
cs_ship_hdemo_sk          int
cs_ship_addr_sk           int
cs_call_center_sk         int
cs_catalog_page_sk        int
cs_ship_mode_sk           int
cs_warehouse_sk           int
cs_item_sk                int
cs_promo_sk               int
cs_order_number           bigint
cs_quantity               int
cs_wholesale_cost         decimal(7,2)
cs_list_price             decimal(7,2)
cs_sales_price            decimal(7,2)
cs_ext_discount_amt       decimal(7,2)
cs_ext_sales_price        decimal(7,2)
cs_ext_wholesale_cost     decimal(7,2)
cs_ext_list_price         decimal(7,2)
cs_ext_tax                decimal(7,2)
cs_coupon_amt             decimal(7,2)
cs_ext_ship_cost          decimal(7,2)
cs_net_paid               decimal(7,2)
cs_net_paid_inc_tax       decimal(7,2)
cs_net_paid_inc_ship      decimal(7,2)
cs_net_paid_inc_ship_tax  decimal(7,2)
cs_net_profit             decimal(7,2)
primary key (cs_item_sk, cs_order_number) DISABLE NOVALIDATE RELY
foreign key (cs_sold_date_sk) references {db_name}{db_suffix}.date_dim (d_date_sk) DISABLE NOVALIDATE RELY
foreign key (cs_sold_time_sk) references {db_name}{db_suffix}.time_dim (t_time_sk) DISABLE NOVALIDATE RELY
foreign key (cs_ship_date_sk) references {db_name}{db_suffix}.date_dim (d_date_sk) DISABLE NOVALIDATE RELY
foreign key (cs_bill_customer_sk) references {db_name}{db_suffix}.customer (c_customer_sk) DISABLE NOVALIDATE RELY
foreign key (cs_bill_cdemo_sk) references {db_name}{db_suffix}.customer_demographics (cd_demo_sk) DISABLE NOVALIDATE RELY
foreign key (cs_bill_hdemo_sk) references {db_name}{db_suffix}.household_demographics (hd_demo_sk) DISABLE NOVALIDATE RELY
foreign key (cs_bill_addr_sk) references {db_name}{db_suffix}.customer_address (ca_address_sk) DISABLE NOVALIDATE RELY
foreign key (cs_ship_customer_sk) references {db_name}{db_suffix}.customer (c_customer_sk) DISABLE NOVALIDATE RELY
foreign key (cs_ship_cdemo_sk) references {db_name}{db_suffix}.customer_demographics (cd_demo_sk) DISABLE NOVALIDATE RELY
foreign key (cs_ship_hdemo_sk) references {db_name}{db_suffix}.household_demographics (hd_demo_sk) DISABLE NOVALIDATE RELY
foreign key (cs_ship_addr_sk) references {db_name}{db_suffix}.customer_address (ca_address_sk) DISABLE NOVALIDATE RELY
foreign key (cs_call_center_sk) references {db_name}{db_suffix}.call_center (cc_call_center_sk) DISABLE NOVALIDATE RELY
foreign key (cs_catalog_page_sk) references {db_name}{db_suffix}.catalog_page (cp_catalog_page_sk) DISABLE NOVALIDATE RELY
foreign key (cs_ship_mode_sk) references {db_name}{db_suffix}.ship_mode (sm_ship_mode_sk) DISABLE NOVALIDATE RELY
foreign key (cs_warehouse_sk) references {db_name}{db_suffix}.warehouse (w_warehouse_sk) DISABLE NOVALIDATE RELY
foreign key (cs_item_sk) references {db_name}{db_suffix}.item (i_item_sk) DISABLE NOVALIDATE RELY
foreign key (cs_promo_sk) references {db_name}{db_suffix}.promotion (p_promo_sk) DISABLE NOVALIDATE RELY
---- PARTITION_COLUMNS
cs_sold_date_sk           int
---- DEPENDENT_LOAD
INSERT OVERWRITE TABLE {db_name}{db_suffix}.{table_name}
(
  cs_sold_time_sk,
  cs_ship_date_sk,
  cs_bill_customer_sk,
  cs_bill_cdemo_sk,
  cs_bill_hdemo_sk,
  cs_bill_addr_sk,
  cs_ship_customer_sk,
  cs_ship_cdemo_sk,
  cs_ship_hdemo_sk,
  cs_ship_addr_sk,
  cs_call_center_sk,
  cs_catalog_page_sk,
  cs_ship_mode_sk,
  cs_warehouse_sk,
  cs_item_sk,
  cs_promo_sk,
  cs_order_number,
  cs_quantity,
  cs_wholesale_cost,
  cs_list_price,
  cs_sales_price,
  cs_ext_discount_amt,
  cs_ext_sales_price,
  cs_ext_wholesale_cost,
  cs_ext_list_price,
  cs_ext_tax,
  cs_coupon_amt,
  cs_ext_ship_cost,
  cs_net_paid,
  cs_net_paid_inc_tax,
  cs_net_paid_inc_ship,
  cs_net_paid_inc_ship_tax,
  cs_net_profit
)
PARTITION (cs_sold_date_sk)
SELECT
  cs_sold_time_sk,
  cs_ship_date_sk,
  cs_bill_customer_sk,
  cs_bill_cdemo_sk,
  cs_bill_hdemo_sk,
  cs_bill_addr_sk,
  cs_ship_customer_sk,
  cs_ship_cdemo_sk,
  cs_ship_hdemo_sk,
  cs_ship_addr_sk,
  cs_call_center_sk,
  cs_catalog_page_sk,
  cs_ship_mode_sk,
  cs_warehouse_sk,
  CAST(cs_item_sk AS INT),
  cs_promo_sk,
  cs_order_number,
  cs_quantity,
  cs_wholesale_cost,
  cs_list_price,
  cs_sales_price,
  cs_ext_discount_amt,
  cs_ext_sales_price,
  cs_ext_wholesale_cost,
  cs_ext_list_price,
  cs_ext_tax,
  cs_coupon_amt,
  cs_ext_ship_cost,
  cs_net_paid,
  cs_net_paid_inc_tax,
  cs_net_paid_inc_ship,
  cs_net_paid_inc_ship_tax,
  cs_net_profit,
  cs_sold_date_sk
FROM tpcds.{table_name};
====
---- DATASET
tpcds_partitioned
---- BASE_TABLE_NAME
catalog_returns
---- COLUMNS
cr_returned_time_sk       int
cr_item_sk                int
cr_refunded_customer_sk   int
cr_refunded_cdemo_sk      int
cr_refunded_hdemo_sk      int
cr_refunded_addr_sk       int
cr_returning_customer_sk  int
cr_returning_cdemo_sk     int
cr_returning_hdemo_sk     int
cr_returning_addr_sk      int
cr_call_center_sk         int
cr_catalog_page_sk        int
cr_ship_mode_sk           int
cr_warehouse_sk           int
cr_reason_sk              int
cr_order_number           bigint
cr_return_quantity        int
cr_return_amount          decimal(7,2)
cr_return_tax             decimal(7,2)
cr_return_amt_inc_tax     decimal(7,2)
cr_fee                    decimal(7,2)
cr_return_ship_cost       decimal(7,2)
cr_refunded_cash          decimal(7,2)
cr_reversed_charge        decimal(7,2)
cr_store_credit           decimal(7,2)
cr_net_loss               decimal(7,2)
primary key (cr_item_sk, cr_order_number) DISABLE NOVALIDATE RELY
foreign key (cr_returned_date_sk) references {db_name}{db_suffix}.date_dim (d_date_sk) DISABLE NOVALIDATE RELY
foreign key (cr_returned_time_sk) references {db_name}{db_suffix}.time_dim (t_time_sk) DISABLE NOVALIDATE RELY
foreign key (cr_item_sk) references {db_name}{db_suffix}.item (i_item_sk) DISABLE NOVALIDATE RELY
foreign key (cr_refunded_customer_sk) references {db_name}{db_suffix}.customer (c_customer_sk) DISABLE NOVALIDATE RELY
foreign key (cr_refunded_cdemo_sk) references {db_name}{db_suffix}.customer_demographics (cd_demo_sk) DISABLE NOVALIDATE RELY
foreign key (cr_refunded_hdemo_sk) references {db_name}{db_suffix}.household_demographics (hd_demo_sk) DISABLE NOVALIDATE RELY
foreign key (cr_refunded_addr_sk) references {db_name}{db_suffix}.customer_address (ca_address_sk) DISABLE NOVALIDATE RELY
foreign key (cr_returning_customer_sk) references {db_name}{db_suffix}.customer (c_customer_sk) DISABLE NOVALIDATE RELY
foreign key (cr_returning_cdemo_sk) references {db_name}{db_suffix}.customer_demographics (cd_demo_sk) DISABLE NOVALIDATE RELY
foreign key (cr_returning_hdemo_sk) references {db_name}{db_suffix}.household_demographics (hd_demo_sk) DISABLE NOVALIDATE RELY
foreign key (cr_returning_addr_sk) references {db_name}{db_suffix}.customer_address (ca_address_sk) DISABLE NOVALIDATE RELY
foreign key (cr_call_center_sk) references {db_name}{db_suffix}.call_center (cc_call_center_sk) DISABLE NOVALIDATE RELY
foreign key (cr_catalog_page_sk) references {db_name}{db_suffix}.catalog_page (cp_catalog_page_sk) DISABLE NOVALIDATE RELY
foreign key (cr_ship_mode_sk) references {db_name}{db_suffix}.ship_mode (sm_ship_mode_sk) DISABLE NOVALIDATE RELY
foreign key (cr_warehouse_sk) references {db_name}{db_suffix}.warehouse (w_warehouse_sk) DISABLE NOVALIDATE RELY
foreign key (cr_reason_sk) references {db_name}{db_suffix}.reason (r_reason_sk) DISABLE NOVALIDATE RELY
foreign key (cr_item_sk, cr_order_number) references {db_name}{db_suffix}.catalog_sales (cs_item_sk, cs_order_number) DISABLE NOVALIDATE RELY
---- PARTITION_COLUMNS
cr_returned_date_sk       int
---- DEPENDENT_LOAD
INSERT OVERWRITE TABLE {db_name}{db_suffix}.{table_name}
(
  cr_returned_time_sk,
  cr_item_sk,
  cr_refunded_customer_sk,
  cr_refunded_cdemo_sk,
  cr_refunded_hdemo_sk,
  cr_refunded_addr_sk,
  cr_returning_customer_sk,
  cr_returning_cdemo_sk,
  cr_returning_hdemo_sk,
  cr_returning_addr_sk,
  cr_call_center_sk,
  cr_catalog_page_sk,
  cr_ship_mode_sk,
  cr_warehouse_sk,
  cr_reason_sk,
  cr_order_number,
  cr_return_quantity,
  cr_return_amount,
  cr_return_tax,
  cr_return_amt_inc_tax,
  cr_fee,
  cr_return_ship_cost,
  cr_refunded_cash,
  cr_reversed_charge,
  cr_store_credit,
  cr_net_loss
)
PARTITION (cr_returned_date_sk)
SELECT
  cr_returned_time_sk,
  CAST(cr_item_sk AS INT),
  cr_refunded_customer_sk,
  cr_refunded_cdemo_sk,
  cr_refunded_hdemo_sk,
  cr_refunded_addr_sk,
  cr_returning_customer_sk,
  cr_returning_cdemo_sk,
  cr_returning_hdemo_sk,
  cr_returning_addr_sk,
  cr_call_center_sk,
  cr_catalog_page_sk,
  cr_ship_mode_sk,
  cr_warehouse_sk,
  cr_reason_sk,
  cr_order_number,
  cr_return_quantity,
  cr_return_amount,
  cr_return_tax,
  cr_return_amt_inc_tax,
  cr_fee,
  cr_return_ship_cost,
  cr_refunded_cash,
  cr_reversed_charge,
  cr_store_credit,
  cr_net_loss,
  cr_returned_date_sk
FROM tpcds.{table_name};
====
---- DATASET
tpcds_partitioned
---- BASE_TABLE_NAME
store_sales
---- COLUMNS
ss_sold_time_sk           int
ss_item_sk                int
ss_customer_sk            int
ss_cdemo_sk               int
ss_hdemo_sk               int
ss_addr_sk                int
ss_store_sk               int
ss_promo_sk               int
ss_ticket_number          bigint
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
primary key (ss_item_sk, ss_ticket_number) DISABLE NOVALIDATE RELY
foreign key (ss_sold_date_sk) references {db_name}{db_suffix}.date_dim (d_date_sk) DISABLE NOVALIDATE RELY
foreign key (ss_sold_time_sk) references {db_name}{db_suffix}.time_dim (t_time_sk) DISABLE NOVALIDATE RELY
foreign key (ss_item_sk) references {db_name}{db_suffix}.item (i_item_sk) DISABLE NOVALIDATE RELY
foreign key (ss_customer_sk) references {db_name}{db_suffix}.customer (c_customer_sk) DISABLE NOVALIDATE RELY
foreign key (ss_cdemo_sk) references {db_name}{db_suffix}.customer_demographics (cd_demo_sk) DISABLE NOVALIDATE RELY
foreign key (ss_hdemo_sk) references {db_name}{db_suffix}.household_demographics (hd_demo_sk) DISABLE NOVALIDATE RELY
foreign key (ss_addr_sk) references {db_name}{db_suffix}.customer_address (ca_address_sk) DISABLE NOVALIDATE RELY
foreign key (ss_store_sk) references {db_name}{db_suffix}.store (s_store_sk) DISABLE NOVALIDATE RELY
foreign key (ss_promo_sk) references {db_name}{db_suffix}.promotion (p_promo_sk) DISABLE NOVALIDATE RELY
---- PARTITION_COLUMNS
ss_sold_date_sk int
---- DEPENDENT_LOAD
INSERT OVERWRITE TABLE {db_name}{db_suffix}.{table_name}
(
  ss_sold_time_sk,
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
  ss_net_profit
)
PARTITION (ss_sold_date_sk)
SELECT
  ss_sold_time_sk,
  CAST(ss_item_sk AS INT),
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
FROM tpcds.{table_name};
====
---- DATASET
tpcds_partitioned
---- BASE_TABLE_NAME
store_returns
---- COLUMNS
sr_return_time_sk         int
sr_item_sk                int
sr_customer_sk            int
sr_cdemo_sk               int
sr_hdemo_sk               int
sr_addr_sk                int
sr_store_sk               int
sr_reason_sk              int
sr_ticket_number          bigint
sr_return_quantity        int
sr_return_amt             decimal(7,2)
sr_return_tax             decimal(7,2)
sr_return_amt_inc_tax     decimal(7,2)
sr_fee                    decimal(7,2)
sr_return_ship_cost       decimal(7,2)
sr_refunded_cash          decimal(7,2)
sr_reversed_charge        decimal(7,2)
sr_store_credit           decimal(7,2)
sr_net_loss               decimal(7,2)
primary key (sr_item_sk, sr_ticket_number) DISABLE NOVALIDATE RELY
foreign key (sr_returned_date_sk) references {db_name}{db_suffix}.date_dim (d_date_sk) DISABLE NOVALIDATE RELY
foreign key (sr_return_time_sk) references {db_name}{db_suffix}.time_dim (t_time_sk) DISABLE NOVALIDATE RELY
foreign key (sr_item_sk) references {db_name}{db_suffix}.item (i_item_sk) DISABLE NOVALIDATE RELY
foreign key (sr_customer_sk) references {db_name}{db_suffix}.customer (c_customer_sk) DISABLE NOVALIDATE RELY
foreign key (sr_cdemo_sk) references {db_name}{db_suffix}.customer_demographics (cd_demo_sk) DISABLE NOVALIDATE RELY
foreign key (sr_hdemo_sk) references {db_name}{db_suffix}.household_demographics (hd_demo_sk) DISABLE NOVALIDATE RELY
foreign key (sr_addr_sk) references {db_name}{db_suffix}.customer_address (ca_address_sk) DISABLE NOVALIDATE RELY
foreign key (sr_store_sk) references {db_name}{db_suffix}.store (s_store_sk) DISABLE NOVALIDATE RELY
foreign key (sr_reason_sk) references {db_name}{db_suffix}.reason (r_reason_sk) DISABLE NOVALIDATE RELY
foreign key (sr_item_sk, sr_ticket_number) references {db_name}{db_suffix}.store_sales (ss_item_sk, ss_ticket_number) DISABLE NOVALIDATE RELY
---- PARTITION_COLUMNS
sr_returned_date_sk       int
---- DEPENDENT_LOAD
INSERT OVERWRITE TABLE {db_name}{db_suffix}.{table_name}
(
  sr_return_time_sk,
  sr_item_sk,
  sr_customer_sk,
  sr_cdemo_sk,
  sr_hdemo_sk,
  sr_addr_sk,
  sr_store_sk,
  sr_reason_sk,
  sr_ticket_number,
  sr_return_quantity,
  sr_return_amt,
  sr_return_tax,
  sr_return_amt_inc_tax,
  sr_fee,
  sr_return_ship_cost,
  sr_refunded_cash,
  sr_reversed_charge,
  sr_store_credit,
  sr_net_loss
)
PARTITION (sr_returned_date_sk)
SELECT
  sr_return_time_sk,
  CAST(sr_item_sk AS INT),
  sr_customer_sk,
  sr_cdemo_sk,
  sr_hdemo_sk,
  sr_addr_sk,
  sr_store_sk,
  sr_reason_sk,
  sr_ticket_number,
  sr_return_quantity,
  sr_return_amt,
  sr_return_tax,
  sr_return_amt_inc_tax,
  sr_fee,
  sr_return_ship_cost,
  sr_refunded_cash,
  sr_reversed_charge,
  sr_store_credit,
  sr_net_loss,
  sr_returned_date_sk
FROM tpcds.{table_name};
====
---- DATASET
tpcds_partitioned
---- BASE_TABLE_NAME
web_sales
---- COLUMNS
ws_sold_time_sk           int
ws_ship_date_sk           int
ws_item_sk                int
ws_bill_customer_sk       int
ws_bill_cdemo_sk          int
ws_bill_hdemo_sk          int
ws_bill_addr_sk           int
ws_ship_customer_sk       int
ws_ship_cdemo_sk          int
ws_ship_hdemo_sk          int
ws_ship_addr_sk           int
ws_web_page_sk            int
ws_web_site_sk            int
ws_ship_mode_sk           int
ws_warehouse_sk           int
ws_promo_sk               int
ws_order_number           bigint
ws_quantity               int
ws_wholesale_cost         decimal(7,2)
ws_list_price             decimal(7,2)
ws_sales_price            decimal(7,2)
ws_ext_discount_amt       decimal(7,2)
ws_ext_sales_price        decimal(7,2)
ws_ext_wholesale_cost     decimal(7,2)
ws_ext_list_price         decimal(7,2)
ws_ext_tax                decimal(7,2)
ws_coupon_amt             decimal(7,2)
ws_ext_ship_cost          decimal(7,2)
ws_net_paid               decimal(7,2)
ws_net_paid_inc_tax       decimal(7,2)
ws_net_paid_inc_ship      decimal(7,2)
ws_net_paid_inc_ship_tax  decimal(7,2)
ws_net_profit             decimal(7,2)
primary key (ws_item_sk, ws_order_number) DISABLE NOVALIDATE RELY
foreign key (ws_sold_date_sk) references {db_name}{db_suffix}.date_dim (d_date_sk) DISABLE NOVALIDATE RELY
foreign key (ws_sold_time_sk) references {db_name}{db_suffix}.time_dim (t_time_sk) DISABLE NOVALIDATE RELY
foreign key (ws_ship_date_sk) references {db_name}{db_suffix}.date_dim (d_date_sk) DISABLE NOVALIDATE RELY
foreign key (ws_item_sk) references {db_name}{db_suffix}.item (i_item_sk) DISABLE NOVALIDATE RELY
foreign key (ws_bill_customer_sk) references {db_name}{db_suffix}.customer (c_customer_sk) DISABLE NOVALIDATE RELY
foreign key (ws_bill_cdemo_sk) references {db_name}{db_suffix}.customer_demographics (cd_demo_sk) DISABLE NOVALIDATE RELY
foreign key (ws_bill_hdemo_sk) references {db_name}{db_suffix}.household_demographics (hd_demo_sk) DISABLE NOVALIDATE RELY
foreign key (ws_bill_addr_sk) references {db_name}{db_suffix}.customer_address (ca_address_sk) DISABLE NOVALIDATE RELY
foreign key (ws_ship_customer_sk) references {db_name}{db_suffix}.customer (c_customer_sk) DISABLE NOVALIDATE RELY
foreign key (ws_ship_cdemo_sk) references {db_name}{db_suffix}.customer_demographics (cd_demo_sk) DISABLE NOVALIDATE RELY
foreign key (ws_ship_hdemo_sk) references {db_name}{db_suffix}.household_demographics (hd_demo_sk) DISABLE NOVALIDATE RELY
foreign key (ws_ship_addr_sk) references {db_name}{db_suffix}.customer_address (ca_address_sk) DISABLE NOVALIDATE RELY
foreign key (ws_web_page_sk) references {db_name}{db_suffix}.web_page (wp_web_page_sk) DISABLE NOVALIDATE RELY
foreign key (ws_web_site_sk) references {db_name}{db_suffix}.web_site (web_site_sk) DISABLE NOVALIDATE RELY
foreign key (ws_ship_mode_sk) references {db_name}{db_suffix}.ship_mode (sm_ship_mode_sk) DISABLE NOVALIDATE RELY
foreign key (ws_warehouse_sk) references {db_name}{db_suffix}.warehouse (w_warehouse_sk) DISABLE NOVALIDATE RELY
foreign key (ws_promo_sk) references {db_name}{db_suffix}.promotion (p_promo_sk) DISABLE NOVALIDATE RELY
---- PARTITION_COLUMNS
ws_sold_date_sk           int
---- DEPENDENT_LOAD
INSERT OVERWRITE TABLE {db_name}{db_suffix}.{table_name}
(
  ws_sold_time_sk,
  ws_ship_date_sk,
  ws_item_sk,
  ws_bill_customer_sk,
  ws_bill_cdemo_sk,
  ws_bill_hdemo_sk,
  ws_bill_addr_sk,
  ws_ship_customer_sk,
  ws_ship_cdemo_sk,
  ws_ship_hdemo_sk,
  ws_ship_addr_sk,
  ws_web_page_sk,
  ws_web_site_sk,
  ws_ship_mode_sk,
  ws_warehouse_sk,
  ws_promo_sk,
  ws_order_number,
  ws_quantity,
  ws_wholesale_cost,
  ws_list_price,
  ws_sales_price,
  ws_ext_discount_amt,
  ws_ext_sales_price,
  ws_ext_wholesale_cost,
  ws_ext_list_price,
  ws_ext_tax,
  ws_coupon_amt,
  ws_ext_ship_cost,
  ws_net_paid,
  ws_net_paid_inc_tax,
  ws_net_paid_inc_ship,
  ws_net_paid_inc_ship_tax,
  ws_net_profit
)
PARTITION (ws_sold_date_sk)
SELECT
  ws_sold_time_sk,
  ws_ship_date_sk,
  CAST(ws_item_sk AS INT),
  ws_bill_customer_sk,
  ws_bill_cdemo_sk,
  ws_bill_hdemo_sk,
  ws_bill_addr_sk,
  ws_ship_customer_sk,
  ws_ship_cdemo_sk,
  ws_ship_hdemo_sk,
  ws_ship_addr_sk,
  ws_web_page_sk,
  ws_web_site_sk,
  ws_ship_mode_sk,
  ws_warehouse_sk,
  ws_promo_sk,
  ws_order_number,
  ws_quantity,
  ws_wholesale_cost,
  ws_list_price,
  ws_sales_price,
  ws_ext_discount_amt,
  ws_ext_sales_price,
  ws_ext_wholesale_cost,
  ws_ext_list_price,
  ws_ext_tax,
  ws_coupon_amt,
  ws_ext_ship_cost,
  ws_net_paid,
  ws_net_paid_inc_tax,
  ws_net_paid_inc_ship,
  ws_net_paid_inc_ship_tax,
  ws_net_profit,
  ws_sold_date_sk
FROM tpcds.{table_name};
====
---- DATASET
tpcds_partitioned
---- BASE_TABLE_NAME
web_returns
---- COLUMNS
wr_returned_time_sk       int
wr_item_sk                int
wr_refunded_customer_sk   int
wr_refunded_cdemo_sk      int
wr_refunded_hdemo_sk      int
wr_refunded_addr_sk       int
wr_returning_customer_sk  int
wr_returning_cdemo_sk     int
wr_returning_hdemo_sk     int
wr_returning_addr_sk      int
wr_web_page_sk            int
wr_reason_sk              int
wr_order_number           bigint
wr_return_quantity        int
wr_return_amt             decimal(7,2)
wr_return_tax             decimal(7,2)
wr_return_amt_inc_tax     decimal(7,2)
wr_fee                    decimal(7,2)
wr_return_ship_cost       decimal(7,2)
wr_refunded_cash          decimal(7,2)
wr_reversed_charge        decimal(7,2)
wr_account_credit         decimal(7,2)
wr_net_loss               decimal(7,2)
primary key (wr_item_sk, wr_order_number) DISABLE NOVALIDATE RELY
foreign key (wr_returned_date_sk) references {db_name}{db_suffix}.date_dim (d_date_sk) DISABLE NOVALIDATE RELY
foreign key (wr_returned_time_sk) references {db_name}{db_suffix}.time_dim (t_time_sk) DISABLE NOVALIDATE RELY
foreign key (wr_item_sk) references {db_name}{db_suffix}.item (i_item_sk) DISABLE NOVALIDATE RELY
foreign key (wr_refunded_customer_sk) references {db_name}{db_suffix}.customer (c_customer_sk) DISABLE NOVALIDATE RELY
foreign key (wr_refunded_cdemo_sk) references {db_name}{db_suffix}.customer_demographics (cd_demo_sk) DISABLE NOVALIDATE RELY
foreign key (wr_refunded_hdemo_sk) references {db_name}{db_suffix}.household_demographics (hd_demo_sk) DISABLE NOVALIDATE RELY
foreign key (wr_refunded_addr_sk) references {db_name}{db_suffix}.customer_address (ca_address_sk) DISABLE NOVALIDATE RELY
foreign key (wr_returning_customer_sk) references {db_name}{db_suffix}.customer (c_customer_sk) DISABLE NOVALIDATE RELY
foreign key (wr_returning_cdemo_sk) references {db_name}{db_suffix}.customer_demographics (cd_demo_sk) DISABLE NOVALIDATE RELY
foreign key (wr_returning_hdemo_sk) references {db_name}{db_suffix}.household_demographics (hd_demo_sk) DISABLE NOVALIDATE RELY
foreign key (wr_returning_addr_sk) references {db_name}{db_suffix}.customer_address (ca_address_sk) DISABLE NOVALIDATE RELY
foreign key (wr_web_page_sk) references {db_name}{db_suffix}.web_page (wp_web_page_sk) DISABLE NOVALIDATE RELY
foreign key (wr_reason_sk) references {db_name}{db_suffix}.reason (r_reason_sk) DISABLE NOVALIDATE RELY
foreign key (wr_item_sk, wr_order_number) references {db_name}{db_suffix}.web_sales (ws_item_sk, ws_order_number) DISABLE NOVALIDATE RELY
---- PARTITION_COLUMNS
wr_returned_date_sk       int
---- DEPENDENT_LOAD
INSERT OVERWRITE TABLE {db_name}{db_suffix}.{table_name}
(
  wr_returned_time_sk,
  wr_item_sk,
  wr_refunded_customer_sk,
  wr_refunded_cdemo_sk,
  wr_refunded_hdemo_sk,
  wr_refunded_addr_sk,
  wr_returning_customer_sk,
  wr_returning_cdemo_sk,
  wr_returning_hdemo_sk,
  wr_returning_addr_sk,
  wr_web_page_sk,
  wr_reason_sk,
  wr_order_number,
  wr_return_quantity,
  wr_return_amt,
  wr_return_tax,
  wr_return_amt_inc_tax,
  wr_fee,
  wr_return_ship_cost,
  wr_refunded_cash,
  wr_reversed_charge,
  wr_account_credit,
  wr_net_loss
)
PARTITION (wr_returned_date_sk)
SELECT
  wr_returned_time_sk,
  CAST(wr_item_sk AS INT),
  wr_refunded_customer_sk,
  wr_refunded_cdemo_sk,
  wr_refunded_hdemo_sk,
  wr_refunded_addr_sk,
  wr_returning_customer_sk,
  wr_returning_cdemo_sk,
  wr_returning_hdemo_sk,
  wr_returning_addr_sk,
  wr_web_page_sk,
  wr_reason_sk,
  wr_order_number,
  wr_return_quantity,
  wr_return_amt,
  wr_return_tax,
  wr_return_amt_inc_tax,
  wr_fee,
  wr_return_ship_cost,
  wr_refunded_cash,
  wr_reversed_charge,
  wr_account_credit,
  wr_net_loss,
  wr_returned_date_sk
FROM tpcds.{table_name};
====
