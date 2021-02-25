---- Licensed to the Apache Software Foundation (ASF) under one
---- or more contributor license agreements.  See the NOTICE file
---- distributed with this work for additional information
---- regarding copyright ownership.  The ASF licenses this file
---- to you under the Apache License, Version 2.0 (the
---- "License"); you may not use this file except in compliance
---- with the License.  You may obtain a copy of the License at
----
----   http://www.apache.org/licenses/LICENSE-2.0
----
---- Unless required by applicable law or agreed to in writing,
---- software distributed under the License is distributed on an
---- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
---- KIND, either express or implied.  See the License for the
---- specific language governing permissions and limitations
---- under the License.

---- Template SQL statements to create and load TPCDS tables in KUDU.
---- TODO: Use range partitioning for some tables
---- TODO: Remove the 'kudu.master_addresses' from TBLPROPERTIES once CM properly sets
---- the 'kudu_masters' startup option in Impala.
---- TODO: Fix the primary key column order
---- TODO: Use different number of buckets for fact and dimension tables

---- STORE_SALES
CREATE TABLE IF NOT EXISTS {target_db_name}.store_sales (
  ss_ticket_number BIGINT,
  ss_item_sk BIGINT,
  ss_sold_date_sk BIGINT,
  ss_sold_time_sk BIGINT,
  ss_customer_sk BIGINT,
  ss_cdemo_sk BIGINT,
  ss_hdemo_sk BIGINT,
  ss_addr_sk BIGINT,
  ss_store_sk BIGINT,
  ss_promo_sk BIGINT,
  ss_quantity BIGINT,
  ss_wholesale_cost DECIMAL(7,2),
  ss_list_price DECIMAL(7,2),
  ss_sales_price DECIMAL(7,2),
  ss_ext_discount_amt DECIMAL(7,2),
  ss_ext_sales_price DECIMAL(7,2),
  ss_ext_wholesale_cost DECIMAL(7,2),
  ss_ext_list_price DECIMAL(7,2),
  ss_ext_tax DECIMAL(7,2),
  ss_coupon_amt DECIMAL(7,2),
  ss_net_paid DECIMAL(7,2),
  ss_net_paid_inc_tax DECIMAL(7,2),
  ss_net_profit DECIMAL(7,2),
  PRIMARY KEY (ss_ticket_number, ss_item_sk)
)
PARTITION BY HASH (ss_ticket_number,ss_item_sk) PARTITIONS {buckets}
STORED AS KUDU
TBLPROPERTIES ('kudu.master_addresses' = '{kudu_master}:7051');

INSERT INTO {target_db_name}.store_sales
SELECT
  ss_ticket_number,
  ss_item_sk,
  ss_sold_date_sk,
  ss_sold_time_sk,
  ss_customer_sk,
  ss_cdemo_sk,
  ss_hdemo_sk,
  ss_addr_sk,
  ss_store_sk,
  ss_promo_sk,
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
  ss_net_paid_inc_tax,ss_net_profit
FROM {source_db_name}.store_sales;

---- WEB_SALES
CREATE TABLE IF NOT EXISTS {target_db_name}.web_sales (
  ws_order_number BIGINT,
  ws_item_sk BIGINT,
  ws_sold_date_sk BIGINT,
  ws_sold_time_sk BIGINT,
  ws_ship_date_sk BIGINT,
  ws_bill_customer_sk BIGINT,
  ws_bill_cdemo_sk BIGINT,
  ws_bill_hdemo_sk BIGINT,
  ws_bill_addr_sk BIGINT,
  ws_ship_customer_sk BIGINT,
  ws_ship_cdemo_sk BIGINT,
  ws_ship_hdemo_sk BIGINT,
  ws_ship_addr_sk BIGINT,
  ws_web_page_sk BIGINT,
  ws_web_site_sk BIGINT,
  ws_ship_mode_sk BIGINT,
  ws_warehouse_sk BIGINT,
  ws_promo_sk BIGINT,
  ws_quantity BIGINT,
  ws_wholesale_cost DECIMAL(7,2),
  ws_list_price DECIMAL(7,2),
  ws_sales_price DECIMAL(7,2),
  ws_ext_discount_amt DECIMAL(7,2),
  ws_ext_sales_price DECIMAL(7,2),
  ws_ext_wholesale_cost DECIMAL(7,2),
  ws_ext_list_price DECIMAL(7,2),
  ws_ext_tax DECIMAL(7,2),
  ws_coupon_amt DECIMAL(7,2),
  ws_ext_ship_cost DECIMAL(7,2),
  ws_net_paid DECIMAL(7,2),
  ws_net_paid_inc_tax DECIMAL(7,2),
  ws_net_paid_inc_ship DECIMAL(7,2),
  ws_net_paid_inc_ship_tax DECIMAL(7,2),
  ws_net_profit DECIMAL(7,2),
  PRIMARY KEY (ws_order_number, ws_item_sk)
)
PARTITION BY HASH (ws_order_number,ws_item_sk) PARTITIONS {buckets}
STORED AS KUDU
TBLPROPERTIES ('kudu.master_addresses' = '{kudu_master}:7051');

INSERT INTO {target_db_name}.web_sales
SELECT
  ws_order_number,
  ws_item_sk,
  ws_sold_date_sk,
  ws_sold_time_sk,
  ws_ship_date_sk,
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
FROM {source_db_name}.web_sales;

---- CATALOG_SALES
CREATE TABLE IF NOT EXISTS {target_db_name}.catalog_sales (
  cs_order_number BIGINT,
  cs_item_sk BIGINT,
  cs_sold_date_sk BIGINT,
  cs_sold_time_sk BIGINT,
  cs_ship_date_sk BIGINT,
  cs_bill_customer_sk BIGINT,
  cs_bill_cdemo_sk BIGINT,
  cs_bill_hdemo_sk BIGINT,
  cs_bill_addr_sk BIGINT,
  cs_ship_customer_sk BIGINT,
  cs_ship_cdemo_sk BIGINT,
  cs_ship_hdemo_sk BIGINT,
  cs_ship_addr_sk BIGINT,
  cs_call_center_sk BIGINT,
  cs_catalog_page_sk BIGINT,
  cs_ship_mode_sk BIGINT,
  cs_warehouse_sk BIGINT,
  cs_promo_sk BIGINT,
  cs_quantity BIGINT,
  cs_wholesale_cost DECIMAL(7,2),
  cs_list_price DECIMAL(7,2),
  cs_sales_price DECIMAL(7,2),
  cs_ext_discount_amt DECIMAL(7,2),
  cs_ext_sales_price DECIMAL(7,2),
  cs_ext_wholesale_cost DECIMAL(7,2),
  cs_ext_list_price DECIMAL(7,2),
  cs_ext_tax DECIMAL(7,2),
  cs_coupon_amt DECIMAL(7,2),
  cs_ext_ship_cost DECIMAL(7,2),
  cs_net_paid DECIMAL(7,2),
  cs_net_paid_inc_tax DECIMAL(7,2),
  cs_net_paid_inc_ship DECIMAL(7,2),
  cs_net_paid_inc_ship_tax DECIMAL(7,2),
  cs_net_profit DECIMAL(7,2),
  PRIMARY KEY (cs_order_number, cs_item_sk)
)
PARTITION BY HASH (cs_order_number,cs_item_sk) PARTITIONS {buckets}
STORED AS KUDU
TBLPROPERTIES ('kudu.master_addresses' = '{kudu_master}:7051');

INSERT INTO {target_db_name}.catalog_sales
SELECT
  cs_order_number,
  cs_item_sk,
  cs_sold_date_sk,
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
  cs_promo_sk,
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
FROM {source_db_name}.catalog_sales;

---- STORE_RETURNS
CREATE TABLE IF NOT EXISTS {target_db_name}.store_returns (
  sr_ticket_number BIGINT,
  sr_item_sk BIGINT,
  sr_returned_date_sk BIGINT,
  sr_return_time_sk BIGINT,
  sr_customer_sk BIGINT,
  sr_cdemo_sk BIGINT,
  sr_hdemo_sk BIGINT,
  sr_addr_sk BIGINT,
  sr_store_sk BIGINT,
  sr_reason_sk BIGINT,
  sr_return_quantity BIGINT,
  sr_return_amt DECIMAL(7,2),
  sr_return_tax DECIMAL(7,2),
  sr_return_amt_inc_tax DECIMAL(7,2),
  sr_fee DECIMAL(7,2),
  sr_return_ship_cost DECIMAL(7,2),
  sr_refunded_cash DECIMAL(7,2),
  sr_reversed_charge DECIMAL(7,2),
  sr_store_credit DECIMAL(7,2),
  sr_net_loss DECIMAL(7,2),
  PRIMARY KEY (sr_ticket_number, sr_item_sk)
)
PARTITION BY HASH (sr_ticket_number,sr_item_sk) PARTITIONS {buckets}
STORED AS KUDU
TBLPROPERTIES ('kudu.master_addresses'='{kudu_master}:7051');

INSERT INTO {target_db_name}.store_returns
SELECT
  sr_ticket_number,
  sr_item_sk,
  sr_returned_date_sk,
  sr_return_time_sk,
  sr_customer_sk,
  sr_cdemo_sk,
  sr_hdemo_sk,
  sr_addr_sk,
  sr_store_sk,
  sr_reason_sk,
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
FROM {source_db_name}.store_returns;

---- WEB_RETURNS
CREATE TABLE IF NOT EXISTS {target_db_name}.web_returns (
  wr_order_number BIGINT,
  wr_item_sk BIGINT,
  wr_returned_date_sk BIGINT,
  wr_returned_time_sk BIGINT,
  wr_refunded_customer_sk BIGINT,
  wr_refunded_cdemo_sk BIGINT,
  wr_refunded_hdemo_sk BIGINT,
  wr_refunded_addr_sk BIGINT,
  wr_returning_customer_sk BIGINT,
  wr_returning_cdemo_sk BIGINT,
  wr_returning_hdemo_sk BIGINT,
  wr_returning_addr_sk BIGINT,
  wr_web_page_sk BIGINT,
  wr_reason_sk BIGINT,
  wr_return_quantity BIGINT,
  wr_return_amt DECIMAL(7,2),
  wr_return_tax DECIMAL(7,2),
  wr_return_amt_inc_tax DECIMAL(7,2),
  wr_fee DECIMAL(7,2),
  wr_return_ship_cost DECIMAL(7,2),
  wr_refunded_cash DECIMAL(7,2),
  wr_reversed_charge DECIMAL(7,2),
  wr_account_credit DECIMAL(7,2),
  wr_net_loss DECIMAL(7,2),
  PRIMARY KEY (wr_order_number, wr_item_sk)
)
PARTITION BY HASH (wr_order_number,wr_item_sk) PARTITIONS {buckets}
STORED AS KUDU
TBLPROPERTIES ('kudu.master_addresses'='{kudu_master}:7051');

INSERT INTO {target_db_name}.web_returns
SELECT
  wr_order_number,
  wr_item_sk,
  wr_returned_date_sk,
  wr_returned_time_sk,
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
FROM {source_db_name}.web_returns;

---- CATALOG_RETURNS
CREATE TABLE IF NOT EXISTS {target_db_name}.catalog_returns (
  cr_order_number BIGINT,
  cr_item_sk BIGINT,
  cr_returned_date_sk BIGINT,
  cr_returned_time_sk BIGINT,
  cr_refunded_customer_sk BIGINT,
  cr_refunded_cdemo_sk BIGINT,
  cr_refunded_hdemo_sk BIGINT,
  cr_refunded_addr_sk BIGINT,
  cr_returning_customer_sk BIGINT,
  cr_returning_cdemo_sk BIGINT,
  cr_returning_hdemo_sk BIGINT,
  cr_returning_addr_sk BIGINT,
  cr_call_center_sk BIGINT,
  cr_catalog_page_sk BIGINT,
  cr_ship_mode_sk BIGINT,
  cr_warehouse_sk BIGINT,
  cr_reason_sk BIGINT,
  cr_return_quantity BIGINT,
  cr_return_amount DECIMAL(7,2),
  cr_return_tax DECIMAL(7,2),
  cr_return_amt_inc_tax DECIMAL(7,2),
  cr_fee DECIMAL(7,2),
  cr_return_ship_cost DECIMAL(7,2),
  cr_refunded_cash DECIMAL(7,2),
  cr_reversed_charge DECIMAL(7,2),
  cr_store_credit DECIMAL(7,2),
  cr_net_loss DECIMAL(7,2),
  PRIMARY KEY (cr_order_number, cr_item_sk)
)
PARTITION BY HASH (cr_order_number,cr_item_sk) PARTITIONS {buckets}
STORED AS KUDU
TBLPROPERTIES ('kudu.master_addresses'='{kudu_master}:7051');

INSERT INTO {target_db_name}.catalog_returns
SELECT
  cr_order_number,
  cr_item_sk,
  cr_returned_date_sk,
  cr_returned_time_sk,
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
FROM {source_db_name}.catalog_returns;

---- INVENTORY
CREATE TABLE IF NOT EXISTS {target_db_name}.inventory (
  inv_date_sk BIGINT,
  inv_item_sk BIGINT,
  inv_warehouse_sk BIGINT,
  inv_quantity_on_hand BIGINT,
  PRIMARY KEY (inv_date_sk, inv_item_sk, inv_warehouse_sk)
)
PARTITION BY HASH (inv_item_sk,inv_date_sk,inv_warehouse_sk) PARTITIONS {buckets}
STORED AS KUDU
TBLPROPERTIES ('kudu.master_addresses'='{kudu_master}:7051');

INSERT INTO {target_db_name}.inventory SELECT * FROM {source_db_name}.inventory;

---- CUSTOMER

CREATE TABLE {target_db_name}.customer (
  c_customer_sk BIGINT PRIMARY KEY,
  c_customer_id STRING,
  c_current_cdemo_sk BIGINT,
  c_current_hdemo_sk BIGINT,
  c_current_addr_sk BIGINT,
  c_first_shipto_date_sk BIGINT,
  c_first_sales_date_sk BIGINT,
  c_salutation STRING,
  c_first_name STRING,
  c_last_name STRING,
  c_preferred_cust_flag STRING,
  c_birth_day INT,
  c_birth_month INT,
  c_birth_year INT,
  c_birth_country STRING,
  c_login STRING,
  c_email_address STRING,
  c_last_review_date STRING
)
PARTITION BY HASH (c_customer_sk) PARTITIONS {buckets}
STORED AS KUDU
TBLPROPERTIES ('kudu.master_addresses'='{kudu_master}:7051');

INSERT INTO {target_db_name}.customer SELECT * FROM {source_db_name}.customer;

---- CUSTOMER_ADDRESS
CREATE TABLE IF NOT EXISTS {target_db_name}.customer_address (
  ca_address_sk BIGINT PRIMARY KEY,
  ca_address_id STRING,
  ca_street_number STRING,
  ca_street_name STRING,
  ca_street_type STRING,
  ca_suite_number STRING,
  ca_city STRING,
  ca_county STRING,
  ca_state STRING,
  ca_zip STRING,
  ca_country STRING,
  ca_gmt_offset DECIMAL(5,2),
  ca_location_type STRING
)
PARTITION BY HASH (ca_address_sk) PARTITIONS {buckets}
STORED AS KUDU
TBLPROPERTIES ('kudu.master_addresses'='{kudu_master}:7051');

INSERT INTO {target_db_name}.customer_address
SELECT * FROM {source_db_name}.customer_address;

---- CUSTOMER_DEMOGRAPHICS
CREATE TABLE IF NOT EXISTS {target_db_name}.customer_demographics (
  cd_demo_sk BIGINT PRIMARY KEY,
  cd_gender STRING,
  cd_marital_status STRING,
  cd_education_status STRING,
  cd_purchase_estimate BIGINT,
  cd_credit_rating STRING,
  cd_dep_count BIGINT,
  cd_dep_employed_count BIGINT,
  cd_dep_college_count BIGINT
)
PARTITION BY HASH (cd_demo_sk) PARTITIONS {buckets}
STORED AS KUDU
TBLPROPERTIES ('kudu.master_addresses'='{kudu_master}:7051');

INSERT INTO {target_db_name}.customer_demographics
SELECT * FROM {source_db_name}.customer_demographics;

---- DATE_DIM
CREATE TABLE IF NOT EXISTS {target_db_name}.date_dim (
  d_date_sk BIGINT PRIMARY KEY,
  d_date_id STRING,
  d_date STRING,
  d_month_seq BIGINT,
  d_week_seq BIGINT,
  d_quarter_seq BIGINT,
  d_year BIGINT,
  d_dow BIGINT,
  d_moy BIGINT,
  d_dom BIGINT,
  d_qoy BIGINT,
  d_fy_year BIGINT,
  d_fy_quarter_seq BIGINT,
  d_fy_week_seq BIGINT,
  d_day_name STRING,
  d_quarter_name STRING,
  d_holiday STRING,
  d_weekend STRING,
  d_following_holiday STRING,
  d_first_dom BIGINT,
  d_last_dom BIGINT,
  d_same_day_ly BIGINT,
  d_same_day_lq BIGINT,
  d_current_day STRING,
  d_current_week STRING,
  d_current_month STRING,
  d_current_quarter STRING,
  d_current_year STRING
)
PARTITION BY HASH (d_date_sk) PARTITIONS {buckets}
STORED AS KUDU
TBLPROPERTIES ('kudu.master_addresses'='{kudu_master}:7051');

INSERT INTO {target_db_name}.date_dim SELECT * FROM {source_db_name}.date_dim;

---- HOUSEHOLD_DEMOGRAPHICS
CREATE TABLE IF NOT EXISTS {target_db_name}.household_demographics (
  hd_demo_sk BIGINT PRIMARY KEY,
  hd_income_band_sk BIGINT,
  hd_buy_potential STRING,
  hd_dep_count BIGINT,
  hd_vehicle_count BIGINT
)
PARTITION BY HASH (hd_demo_sk) PARTITIONS {buckets}
STORED AS KUDU
TBLPROPERTIES ('kudu.master_addresses'='{kudu_master}:7051');

INSERT INTO {target_db_name}.household_demographics
SELECT * FROM {source_db_name}.household_demographics;

---- ITEM
CREATE TABLE IF NOT EXISTS {target_db_name}.item (
  i_item_sk BIGINT PRIMARY KEY,
  i_item_id STRING,
  i_rec_start_date STRING,
  i_rec_end_date STRING,
  i_item_desc STRING,
  i_current_price DECIMAL(7,2),
  i_wholesale_cost DECIMAL(7,2),
  i_brand_id BIGINT,
  i_brand STRING,
  i_class_id BIGINT,
  i_class STRING,
  i_category_id BIGINT,
  i_category STRING,
  i_manufact_id BIGINT,
  i_manufact STRING,
  i_size STRING,
  i_formulation STRING,
  i_color STRING,
  i_units STRING,
  i_container STRING,
  i_manager_id BIGINT,
  i_product_name STRING
)
PARTITION BY HASH (i_item_sk) PARTITIONS {buckets}
STORED AS KUDU
TBLPROPERTIES ('kudu.master_addresses'='{kudu_master}:7051');

INSERT INTO {target_db_name}.item SELECT * FROM {source_db_name}.item;

---- PROMOTION
CREATE TABLE IF NOT EXISTS {target_db_name}.promotion (
  p_promo_sk BIGINT PRIMARY KEY,
  p_item_sk BIGINT,
  p_start_date_sk BIGINT,
  p_end_date_sk BIGINT,
  p_promo_id STRING,
  p_cost DECIMAL(15,2),
  p_response_target BIGINT,
  p_promo_name STRING,
  p_channel_dmail STRING,
  p_channel_email STRING,
  p_channel_catalog STRING,
  p_channel_tv STRING,
  p_channel_radio STRING,
  p_channel_press STRING,
  p_channel_event STRING,
  p_channel_demo STRING,
  p_channel_details STRING,
  p_purpose STRING,
  p_discount_active STRING
)
PARTITION BY HASH (p_promo_sk) PARTITIONS {buckets}
STORED AS KUDU
TBLPROPERTIES ('kudu.master_addresses'='{kudu_master}:7051');

INSERT INTO {target_db_name}.promotion
SELECT
  p_promo_sk,
  p_item_sk,
  p_start_date_sk,
  p_end_date_sk,
  p_promo_id,
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
FROM {source_db_name}.promotion;

---- STORE
CREATE TABLE IF NOT EXISTS {target_db_name}.store (
  s_store_sk BIGINT PRIMARY KEY,
  s_store_id STRING,
  s_rec_start_date STRING,
  s_rec_end_date STRING,
  s_closed_date_sk BIGINT,
  s_store_name STRING,
  s_number_employees BIGINT,
  s_floor_space BIGINT,
  s_hours STRING,
  s_manager STRING,
  s_market_id BIGINT,
  s_geography_class STRING,
  s_market_desc STRING,
  s_market_manager STRING,
  s_division_id BIGINT,
  s_division_name STRING,
  s_company_id BIGINT,
  s_company_name STRING,
  s_street_number STRING,
  s_street_name STRING,
  s_street_type STRING,
  s_suite_number STRING,
  s_city STRING,
  s_county STRING,
  s_state STRING,
  s_zip STRING,
  s_country STRING,
  s_gmt_offset DECIMAL(5,2),
  s_tax_precentage DECIMAL(5,2)
)
PARTITION BY HASH (s_store_sk) PARTITIONS {buckets}
STORED AS KUDU
TBLPROPERTIES ('kudu.master_addresses'='{kudu_master}:7051');

INSERT INTO {target_db_name}.store SELECT * FROM {source_db_name}.store;

---- TIME_DIM
CREATE TABLE IF NOT EXISTS {target_db_name}.time_dim (
  t_time_sk BIGINT PRIMARY KEY,
  t_time_id STRING,
  t_time BIGINT,
  t_hour BIGINT,
  t_minute BIGINT,
  t_second BIGINT,
  t_am_pm STRING,
  t_shift STRING,
  t_sub_shift STRING,
  t_meal_time STRING
)
PARTITION BY HASH (t_time_sk) PARTITIONS {buckets}
STORED AS KUDU
TBLPROPERTIES ('kudu.master_addresses'='{kudu_master}:7051');

INSERT INTO {target_db_name}.time_dim SELECT * FROM {source_db_name}.time_dim;

---- CALL_CENTER
CREATE TABLE IF NOT EXISTS {target_db_name}.call_center (
  cc_call_center_sk BIGINT PRIMARY KEY,
  cc_call_center_id STRING,
  cc_rec_start_date STRING,
  cc_rec_end_date STRING,
  cc_closed_date_sk BIGINT,
  cc_open_date_sk BIGINT,
  cc_name STRING,
  cc_class STRING,
  cc_employees BIGINT,
  cc_sq_ft BIGINT,
  cc_hours STRING,
  cc_manager STRING,
  cc_mkt_id BIGINT,
  cc_mkt_class STRING,
  cc_mkt_desc STRING,
  cc_market_manager STRING,
  cc_division BIGINT,
  cc_division_name STRING,
  cc_company BIGINT,
  cc_company_name STRING,
  cc_street_number STRING,
  cc_street_name STRING,
  cc_street_type STRING,
  cc_suite_number STRING,
  cc_city STRING,
  cc_county STRING,
  cc_state STRING,
  cc_zip STRING,
  cc_country STRING,
  cc_gmt_offset DECIMAL(5,2),
  cc_tax_percentage DECIMAL(5,2)
)
PARTITION BY HASH (cc_call_center_sk) PARTITIONS {buckets}
STORED AS KUDU
TBLPROPERTIES ('kudu.master_addresses'='{kudu_master}:7051');

INSERT INTO {target_db_name}.call_center SELECT * FROM {source_db_name}.call_center;

---- CATALOG_PAGE
CREATE TABLE IF NOT EXISTS {target_db_name}.catalog_page (
  cp_catalog_page_sk BIGINT PRIMARY KEY,
  cp_catalog_page_id STRING,
  cp_start_date_sk BIGINT,
  cp_end_date_sk BIGINT,
  cp_department STRING,
  cp_catalog_number BIGINT,
  cp_catalog_page_number BIGINT,
  cp_description STRING,
  cp_type STRING
)
PARTITION BY HASH (cp_catalog_page_sk) PARTITIONS {buckets}
STORED AS KUDU
TBLPROPERTIES ('kudu.master_addresses'='{kudu_master}:7051');

INSERT INTO {target_db_name}.catalog_page SELECT * FROM {source_db_name}.catalog_page;

---- INCOME_BANDS
CREATE TABLE IF NOT EXISTS {target_db_name}.income_band (
  ib_income_band_sk BIGINT PRIMARY KEY,
  ib_lower_bound BIGINT,
  ib_upper_bound BIGINT
)
PARTITION BY HASH (ib_income_band_sk) PARTITIONS {buckets}
STORED AS KUDU
TBLPROPERTIES ('kudu.master_addresses'='{kudu_master}:7051');

INSERT INTO {target_db_name}.income_band SELECT * FROM {source_db_name}.income_band;

---- REASON
CREATE TABLE IF NOT EXISTS {target_db_name}.reason (
  r_reason_sk BIGINT PRIMARY KEY,
  r_reason_id STRING,
  r_reason_desc STRING
)
PARTITION BY HASH (r_reason_sk) PARTITIONS {buckets}
STORED AS KUDU
TBLPROPERTIES ('kudu.master_addresses'='{kudu_master}:7051');

INSERT INTO {target_db_name}.reason SELECT * FROM {source_db_name}.reason;

---- SHIP_MODE
CREATE TABLE IF NOT EXISTS {target_db_name}.ship_mode (
  sm_ship_mode_sk BIGINT PRIMARY KEY,
  sm_ship_mode_id STRING,
  sm_type STRING,
  sm_code STRING,
  sm_carrier STRING,
  sm_contract STRING
)
PARTITION BY HASH (sm_ship_mode_sk) PARTITIONS {buckets}
STORED AS KUDU
TBLPROPERTIES ('kudu.master_addresses'='{kudu_master}:7051');

INSERT INTO {target_db_name}.ship_mode SELECT * FROM {source_db_name}.ship_mode;

---- WAREHOUSE
CREATE TABLE IF NOT EXISTS {target_db_name}.warehouse (
  w_warehouse_sk BIGINT PRIMARY KEY,
  w_warehouse_id STRING,
  w_warehouse_name STRING,
  w_warehouse_sq_ft BIGINT,
  w_street_number STRING,
  w_street_name STRING,
  w_street_type STRING,
  w_suite_number STRING,
  w_city STRING,
  w_county STRING,
  w_state STRING,
  w_zip STRING,
  w_country STRING,
  w_gmt_offset DECIMAL(5,2)
)
PARTITION BY HASH (w_warehouse_sk) PARTITIONS {buckets}
STORED AS KUDU
TBLPROPERTIES ('kudu.master_addresses'='{kudu_master}:7051');

INSERT INTO {target_db_name}.warehouse SELECT * FROM {source_db_name}.warehouse;

---- WEB_PAGE
CREATE TABLE IF NOT EXISTS {target_db_name}.web_page (
  wp_web_page_sk BIGINT PRIMARY KEY,
  wp_web_page_id STRING,
  wp_rec_start_date STRING,
  wp_rec_end_date STRING,
  wp_creation_date_sk BIGINT,
  wp_access_date_sk BIGINT,
  wp_autogen_flag STRING,
  wp_customer_sk BIGINT,
  wp_url STRING,
  wp_type STRING,
  wp_char_count BIGINT,
  wp_link_count BIGINT,
  wp_image_count BIGINT,
  wp_max_ad_count BIGINT
)
PARTITION BY HASH (wp_web_page_sk) PARTITIONS {buckets}
STORED AS KUDU
TBLPROPERTIES ('kudu.master_addresses'='{kudu_master}:7051');

INSERT INTO {target_db_name}.web_page SELECT * FROM {source_db_name}.web_page;

---- WEB_SITE
CREATE TABLE IF NOT EXISTS {target_db_name}.web_site (
  web_site_sk BIGINT PRIMARY KEY,
  web_site_id STRING,
  web_rec_start_date STRING,
  web_rec_end_date STRING,
  web_name STRING,
  web_open_date_sk BIGINT,
  web_close_date_sk BIGINT,
  web_class STRING,
  web_manager STRING,
  web_mkt_id BIGINT,
  web_mkt_class STRING,
  web_mkt_desc STRING,
  web_market_manager STRING,
  web_company_id BIGINT,
  web_company_name STRING,
  web_street_number STRING,
  web_street_name STRING,
  web_street_type STRING,
  web_suite_number STRING,
  web_city STRING,
  web_county STRING,
  web_state STRING,
  web_zip STRING,
  web_country STRING,
  web_gmt_offset DECIMAL(5,2),
  web_tax_percentage DECIMAL(5,2)
)
PARTITION BY HASH (web_site_sk) PARTITIONS {buckets}
STORED AS KUDU
TBLPROPERTIES ('kudu.master_addresses'='{kudu_master}:7051');

INSERT INTO {target_db_name}.web_site SELECT * FROM {source_db_name}.web_site;

---- COMPUTE STATS
compute stats {target_db_name}.call_center;
compute stats {target_db_name}.catalog_page;
compute stats {target_db_name}.catalog_returns;
compute stats {target_db_name}.catalog_sales;
compute stats {target_db_name}.customer;
compute stats {target_db_name}.customer_address;
compute stats {target_db_name}.customer_demographics;
compute stats {target_db_name}.date_dim;
compute stats {target_db_name}.household_demographics;
compute stats {target_db_name}.income_band;
compute stats {target_db_name}.inventory;
compute stats {target_db_name}.item;
compute stats {target_db_name}.reason;
compute stats {target_db_name}.ship_mode;
compute stats {target_db_name}.store;
compute stats {target_db_name}.store_returns;
compute stats {target_db_name}.store_sales;
compute stats {target_db_name}.time_dim;
compute stats {target_db_name}.warehouse;
compute stats {target_db_name}.web_page;
compute stats {target_db_name}.web_returns;
compute stats {target_db_name}.web_sales;
compute stats {target_db_name}.web_site;
compute stats {target_db_name}.promotion;
