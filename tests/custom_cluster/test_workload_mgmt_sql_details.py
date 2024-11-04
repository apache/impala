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

from __future__ import absolute_import, division, print_function

import os

from SystemTables.ttypes import TQueryTableColumn
from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from test_query_log import TestQueryLogTableBase
from tests.util.workload_management import assert_csv_col


class TestWorkloadManagementSQLDetails(TestQueryLogTableBase):
  """Tests that ensure the workload management data describing the details of the sql
     statement values (columns tables_queried, select_columns, where_columns,
     join_columns, aggregate_columns, and orderby_columns) in the completed
     queried table are correct. Since these fields were added to the query profile at the
     the same time they were added to the database table, a comparison between the query
     profile and the value stored in the database table does not actually assert the
     correctness of this data."""

  @classmethod
  def add_test_dimensions(cls):
    super(TestWorkloadManagementSQLDetails, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_constraint(lambda v:
        v.get_value("protocol") == "beeswax")

  def setup_method(self, method):
    super(TestWorkloadManagementSQLDetails, self).setup_method(method)
    self.wait_for_wm_init_complete()

    # Assert tables queried.
  def _assert_all(self, vector, query, expected_tables_queried, expected_select_cols,
        expected_where_cols, expected_join_cols, expected_aggregate_cols,
        expected_orderby_cols, db="tpcds"):
    """Runs the provided query ensuring it completed successfully and asserts the correct
       information is stored in the query log table.  If the 'db' parameter is not empty,
       this function first runs a 'use' query to switch to the specified database."""
    client = self.get_client(vector.get_value("protocol"))

    if db != "":
      assert client.execute("use {}".format(db)).success
    res = client.execute(query)
    assert res.success

    # Wait for the query to be written to the completed queries table.
    self.cluster.get_first_impalad().service.wait_for_metric_value(
        "impala-server.completed-queries.written", 1, 60)

    # Assert tables queried.
    assert_csv_col(client, self.QUERY_TBL, TQueryTableColumn.TABLES_QUERIED, res.query_id,
        expected_tables_queried, db)

    # Assert select columns.
    assert_csv_col(client, self.QUERY_TBL, TQueryTableColumn.SELECT_COLUMNS, res.query_id,
        expected_select_cols, db)

    # Assert where columns.
    assert_csv_col(client, self.QUERY_TBL, TQueryTableColumn.WHERE_COLUMNS, res.query_id,
        expected_where_cols, db)

    # Assert join columns.
    assert_csv_col(client, self.QUERY_TBL, TQueryTableColumn.JOIN_COLUMNS, res.query_id,
        expected_join_cols, db)

    # Aggregate Columns
    assert_csv_col(client, self.QUERY_TBL, TQueryTableColumn.AGGREGATE_COLUMNS,
        res.query_id, expected_aggregate_cols, db)

    # OrderBy Columns
    assert_csv_col(client, self.QUERY_TBL, TQueryTableColumn.ORDERBY_COLUMNS,
        res.query_id, expected_orderby_cols, db)

  def _run_test(self, vector, tpcds_query_num, expected_tables_queried, expected_select,
      expected_where, expected_join, expected_aggregate, expected_orderby):
    """Runs a single tpcds query from the testdata/worklods/tpcds/queries/raw folder and
       asserts the data written in the completed queries table about the tables queried,
       and query columns."""

    # Read the text of the tpcds query from its raw file.
    with open(os.path.join(os.environ["IMPALA_WORKLOAD_DIR"], "tpcds",
        "queries", "raw", "tpcds-query{}.sql".format(tpcds_query_num))) as file:
      query_text = file.read().lower().replace("\t", "  ")

    # Execute the query and perform assertions.
    self._assert_all(vector, query_text, expected_tables_queried, expected_select,
        expected_where, expected_join, expected_aggregate, expected_orderby)

  @CustomClusterTestSuite.with_args(
      cluster_size=1, impalad_graceful_shutdown=True, disable_log_buffering=True,
      impalad_args="--enable_workload_mgmt --query_log_write_interval_s=1",
      catalogd_args="--enable_workload_mgmt")
  def test_tpcds_1(self, vector):
    """Asserts the values inserted into the workload management table columns match
       what is expected for TPCDS query 1.

       Query 1 tests queries that leverage secondary queries using the 'with' keyword."""
    self._run_test(vector, 1,
        # Expected tables queried.
        ["store_returns", "date_dim", "store", "customer"],

        # Expected select columns.
        ["store_returns.sr_customer_sk", "store_returns.sr_store_sk",
         "store_returns.sr_fee", "customer.c_customer_id"],

        # Expected where columns.
        ["store_returns.sr_returned_date_sk", "date_dim.d_date_sk", "date_dim.d_year",
         "store.s_store_sk", "store.s_state", "customer.c_customer_sk",
         "store_returns.sr_customer_sk", "store_returns.sr_store_sk"],

        # Expected join columns.
        ["customer.c_customer_sk", "date_dim.d_date_sk", "store.s_store_sk",
         "store_returns.sr_returned_date_sk", "store_returns.sr_fee",
         "store_returns.sr_customer_sk", "store_returns.sr_store_sk"],

        # Expected aggregate columns.
        ["store_returns.sr_customer_sk", "store_returns.sr_store_sk"],

        # Expected order by columns.
        ["customer.c_customer_id"])

  @CustomClusterTestSuite.with_args(
      cluster_size=1, impalad_graceful_shutdown=True, disable_log_buffering=True,
      impalad_args="--enable_workload_mgmt --query_log_write_interval_s=1",
      catalogd_args="--enable_workload_mgmt")
  def test_tpcds_3(self, vector):
    """Asserts the values inserted into the workload management table columns match
       what is expected for TPCDS query 3.

       Query 3 tests column aliases and sql functions in the order by clause."""
    self._run_test(vector, 3,
        # Expected tables queried.
        ["date_dim", "store_sales", "item"],

        # Expected select columns.
        ["date_dim.d_year", "item.i_brand_id", "item.i_brand",
         "store_sales.ss_ext_sales_price"],

        # Expected where columns.
        ["date_dim.d_date_sk", "date_dim.d_moy", "item.i_item_sk", "item.i_manufact_id",
         "store_sales.ss_item_sk", "store_sales.ss_sold_date_sk"],

        # Expected join columns.
        ["date_dim.d_date_sk", "item.i_item_sk", "store_sales.ss_item_sk",
         "store_sales.ss_sold_date_sk"],

        # Expected aggregate columns.
        ["date_dim.d_year", "item.i_brand", "item.i_brand_id"],

        # Expected order by columns.
        ["date_dim.d_year", "store_sales.ss_ext_sales_price", "item.i_brand_id"])

  @CustomClusterTestSuite.with_args(
      cluster_size=1, impalad_graceful_shutdown=True, disable_log_buffering=True,
      impalad_args="--enable_workload_mgmt --query_log_write_interval_s=1",
      catalogd_args="--enable_workload_mgmt")
  def test_tpcds_51(self, vector):
    """Asserts the values inserted into the workload management table columns match
       what is expected for TPCDS query 51.

       Query 51 tests case statements in a query select clause."""
    self._run_test(vector, 51,
        # Expected tables queried.
        ["web_sales", "date_dim", "store_sales"],

        # Expected select columns.
        ["date_dim.d_date", "store_sales.ss_item_sk", "store_sales.ss_sales_price",
         "web_sales.ws_item_sk", "web_sales.ws_sales_price"],

        # Expected where columns.
        ["date_dim.d_date_sk", "date_dim.d_month_seq", "store_sales.ss_item_sk",
         "store_sales.ss_sold_date_sk", "web_sales.ws_item_sk",
         "web_sales.ws_sold_date_sk"],

        # Expected join columns.
        ["date_dim.d_date", "date_dim.d_date_sk",
         "store_sales.ss_item_sk", "store_sales.ss_sold_date_sk", "web_sales.ws_item_sk",
         "web_sales.ws_sold_date_sk"],

        # Expected aggregate columns.
        ["web_sales.ws_item_sk", "date_dim.d_date", "store_sales.ss_item_sk"],

        # Expected order by columns.
        ["web_sales.ws_item_sk", "store_sales.ss_item_sk", "date_dim.d_date"])

  @CustomClusterTestSuite.with_args(
      cluster_size=1, impalad_graceful_shutdown=True, disable_log_buffering=True,
      impalad_args="--enable_workload_mgmt --query_log_write_interval_s=1",
      catalogd_args="--enable_workload_mgmt")
  def test_tpcds_62(self, vector):
    """Asserts the values inserted into the workload management table columns match
       what is expected for TPCDS query 62.

       Query 62 tests sql functions in the group by clause."""
    self._run_test(vector, 62,
        # Expected tables queried.
        ["web_sales", "warehouse", "ship_mode", "web_site", "date_dim"],

        # Expected select columns.
        ["warehouse.w_warehouse_name", "ship_mode.sm_type", "web_site.web_name",
         "web_sales.ws_ship_date_sk", "web_sales.ws_sold_date_sk"],

        # Expected where columns.
        ["date_dim.d_month_seq", "web_sales.ws_ship_date_sk", "date_dim.d_date_sk",
         "web_sales.ws_warehouse_sk", "warehouse.w_warehouse_sk",
         "web_sales.ws_ship_mode_sk", "ship_mode.sm_ship_mode_sk",
         "web_sales.ws_web_site_sk", "web_site.web_site_sk"],

        # Expected join columns.
        ["web_sales.ws_ship_date_sk", "date_dim.d_date_sk", "web_sales.ws_web_site_sk",
         "web_site.web_site_sk", "web_sales.ws_ship_mode_sk", "ship_mode.sm_ship_mode_sk",
         "web_sales.ws_warehouse_sk", "warehouse.w_warehouse_sk"],

        # Expected aggregate columns.
        ["warehouse.w_warehouse_name", "ship_mode.sm_type", "web_site.web_name"],

        # Expected order by columns.
        ["warehouse.w_warehouse_name", "ship_mode.sm_type", "web_site.web_name"])

  @CustomClusterTestSuite.with_args(
      cluster_size=1, impalad_graceful_shutdown=True, disable_log_buffering=True,
      impalad_args="--enable_workload_mgmt --query_log_write_interval_s=1",
      catalogd_args="--enable_workload_mgmt")
  def test_tpcds_64(self, vector):
    """Asserts the values inserted into the workload management table columns match
       what is expected for TPCDS query 64.

       Query 64 tests additional subqueries functionality."""
    self._run_test(vector, 64,
        # Expected tables queried.
        ["catalog_sales", "catalog_returns", "store_sales", "store_returns", "date_dim",
         "store", "customer", "customer_demographics", "promotion",
         "household_demographics", "customer_address", "income_band", "item"],

        # Expected select columns.
        ["catalog_sales.cs_item_sk", "catalog_sales.cs_ext_list_price",
         "catalog_returns.cr_refunded_cash", "catalog_returns.cr_reversed_charge",
         "catalog_returns.cr_store_credit", "item.i_product_name", "item.i_item_sk",
         "store.s_store_name", "store.s_zip", "customer_address.ca_street_number",
         "customer_address.ca_street_name", "customer_address.ca_city",
         "customer_address.ca_zip", "date_dim.d_year", "store_sales.ss_wholesale_cost",
         "store_sales.ss_list_price", "store_sales.ss_coupon_amt"],

        # Expected where columns.
        ["catalog_sales.cs_item_sk", "catalog_returns.cr_item_sk",
         "catalog_sales.cs_order_number", "catalog_returns.cr_order_number",
         "store_sales.ss_store_sk", "store.s_store_sk", "store_sales.ss_sold_date_sk",
         "date_dim.d_date_sk", "store_sales.ss_customer_sk", "customer.c_customer_sk",
         "store_sales.ss_cdemo_sk", "customer_demographics.cd_demo_sk",
         "store_sales.ss_hdemo_sk", "household_demographics.hd_demo_sk",
         "store_sales.ss_addr_sk", "customer_address.ca_address_sk",
         "store_sales.ss_item_sk", "item.i_item_sk", "store_returns.sr_item_sk",
         "store_sales.ss_ticket_number", "store_returns.sr_ticket_number",
         "customer.c_current_cdemo_sk", "customer.c_current_hdemo_sk",
         "customer.c_current_addr_sk", "customer.c_first_sales_date_sk",
         "customer.c_first_shipto_date_sk", "store_sales.ss_promo_sk",
         "promotion.p_promo_sk", "household_demographics.hd_income_band_sk",
         "item.i_color", "income_band.ib_income_band_sk", "item.i_current_price",
         "customer_demographics.cd_marital_status", "date_dim.d_year",
         "store.s_store_name", "store.s_zip"],

        # Expected join columns.
        ["catalog_returns.cr_item_sk", "catalog_returns.cr_order_number",
         "catalog_sales.cs_item_sk", "catalog_sales.cs_order_number",
         "customer.c_current_addr_sk", "customer.c_current_cdemo_sk",
         "customer.c_current_hdemo_sk", "customer.c_customer_sk",
         "customer.c_first_sales_date_sk", "customer.c_first_shipto_date_sk",
         "customer_address.ca_address_sk", "customer_demographics.cd_demo_sk",
         "customer_demographics.cd_marital_status", "date_dim.d_date_sk",
         "household_demographics.hd_demo_sk", "income_band.ib_income_band_sk",
         "household_demographics.hd_income_band_sk", "item.i_item_sk",
         "promotion.p_promo_sk", "store.s_store_sk", "store.s_zip",
         "store_returns.sr_item_sk", "store_returns.sr_ticket_number",
         "store_sales.ss_addr_sk", "store_sales.ss_cdemo_sk",
         "store_sales.ss_customer_sk", "store_sales.ss_hdemo_sk",
         "store_sales.ss_item_sk", "store_sales.ss_promo_sk",
         "store_sales.ss_sold_date_sk", "store_sales.ss_store_sk",
         "store_sales.ss_ticket_number", "store.s_store_name"],

        # Expected aggregate columns.
        ["catalog_sales.cs_item_sk", "catalog_sales.cs_ext_list_price",
         "catalog_returns.cr_refunded_cash", "item.i_product_name",
         "catalog_returns.cr_reversed_charge", "item.i_item_sk",
         "catalog_returns.cr_store_credit", "store.s_store_name", "store.s_zip",
         "customer_address.ca_street_number", "customer_address.ca_street_name",
         "customer_address.ca_city", "customer_address.ca_zip", "date_dim.d_year"],

        # Expected order by columns.
        ["item.i_product_name", "store.s_store_name", "store_sales.ss_wholesale_cost"])

  @CustomClusterTestSuite.with_args(
      cluster_size=1, impalad_graceful_shutdown=True, disable_log_buffering=True,
      impalad_args="--enable_workload_mgmt --query_log_write_interval_s=1",
      catalogd_args="--enable_workload_mgmt")
  def test_tpcds_66(self, vector):
    """Asserts the values inserted into the workload management table columns match
       what is expected for TPCDS query 66.

       Query 66 tests subqueries in the from clause."""
    self._run_test(vector, 66,
        # Expected tables queried.
        ["web_sales", "ship_mode", "warehouse", "time_dim", "date_dim", "catalog_sales"],

        # Expected select columns.
        ["warehouse.w_warehouse_name", "warehouse.w_warehouse_sq_ft", "warehouse.w_city",
         "warehouse.w_country", "warehouse.w_county", "warehouse.w_state",
         "date_dim.d_year", "web_sales.ws_sales_price", "web_sales.ws_quantity",
         "web_sales.ws_net_paid_inc_tax", "date_dim.d_moy",
         "catalog_sales.cs_ext_sales_price", "catalog_sales.cs_quantity",
         "catalog_sales.cs_net_paid_inc_ship_tax"],

        # Expected where columns.
        ["catalog_sales.cs_warehouse_sk", "warehouse.w_warehouse_sk",
         "catalog_sales.cs_sold_date_sk", "date_dim.d_date_sk",
         "catalog_sales.cs_sold_time_sk", "time_dim.t_time_sk",
         "catalog_sales.cs_ship_mode_sk", "ship_mode.sm_ship_mode_sk",
         "date_dim.d_year", "time_dim.t_time", "ship_mode.sm_carrier",
         "web_sales.ws_sold_date_sk", "web_sales.ws_sold_time_sk",
         "web_sales.ws_ship_mode_sk", "web_sales.ws_warehouse_sk"],

        # Expected join columns.
        ["catalog_sales.cs_warehouse_sk", "warehouse.w_warehouse_sk",
         "catalog_sales.cs_sold_date_sk", "date_dim.d_date_sk",
         "catalog_sales.cs_sold_time_sk", "time_dim.t_time_sk",
         "catalog_sales.cs_ship_mode_sk", "ship_mode.sm_ship_mode_sk",
         "web_sales.ws_warehouse_sk", "web_sales.ws_sold_time_sk",
         "web_sales.ws_sold_date_sk", "web_sales.ws_ship_mode_sk"],

        # Expected aggregate columns.
        ["warehouse.w_warehouse_name", "warehouse.w_city", "warehouse.w_warehouse_sq_ft",
         "warehouse.w_county", "warehouse.w_state", "warehouse.w_country",
         "date_dim.d_year"],

        # Expected order by columns.
        ["warehouse.w_warehouse_name"])

  @CustomClusterTestSuite.with_args(
      cluster_size=1, impalad_graceful_shutdown=True, disable_log_buffering=True,
      impalad_args="--enable_workload_mgmt --query_log_write_interval_s=1",
      catalogd_args="--enable_workload_mgmt")
  def test_tpcds_75(self, vector):
    """Asserts the values inserted into the workload management table columns match
       what is expected for TPCDS query 75.

       Query 75 tests arithmetic expressions in a query."""
    self._run_test(vector, 75,
        # Expected tables queried.
        ["catalog_returns", "catalog_sales", "date_dim", "item", "store_returns",
         "store_sales", "web_returns", "web_sales"],

        # Expected select columns.
        ["catalog_returns.cr_return_amount", "date_dim.d_year", "item.i_brand_id",
         "catalog_returns.cr_return_quantity", "item.i_category_id", "item.i_class_id",
         "catalog_sales.cs_ext_sales_price", "catalog_sales.cs_quantity",
         "item.i_manufact_id", "store_returns.sr_return_amt", "store_sales.ss_quantity",
         "store_returns.sr_return_quantity", "store_sales.ss_ext_sales_price",
         "web_returns.wr_return_amt", "web_returns.wr_return_quantity",
         "web_sales.ws_ext_sales_price", "web_sales.ws_quantity"],

        # Expected where columns.
        ["item.i_category", "item.i_brand_id", "item.i_class_id", "item.i_category_id",
         "item.i_manufact_id", "date_dim.d_year", "catalog_returns.cr_return_quantity",
         "catalog_returns.cr_return_quantity", "store_returns.sr_return_quantity",
         "store_sales.ss_quantity", "web_returns.wr_return_quantity",
         "web_sales.ws_quantity"],

        # Expected join columns.
        ["item.i_brand_id", "item.i_category_id", "item.i_class_id", "item.i_manufact_id",
         "web_returns.wr_item_sk", "web_sales.ws_item_sk", "web_returns.wr_order_number",
         "web_sales.ws_order_number", "web_sales.ws_sold_date_sk", "date_dim.d_date_sk",
         "catalog_sales.cs_sold_date_sk", "item.i_item_sk", "store_returns.sr_item_sk",
         "store_sales.ss_item_sk", "store_returns.sr_ticket_number",
         "store_sales.ss_ticket_number", "store_sales.ss_sold_date_sk",
         "catalog_returns.cr_item_sk", "catalog_sales.cs_item_sk",
         "catalog_returns.cr_order_number", "catalog_sales.cs_order_number",
         "store_sales.ss_quantity", "web_returns.wr_return_quantity",
         "web_sales.ws_quantity", "store_returns.sr_return_quantity",
         "catalog_sales.cs_quantity", "catalog_returns.cr_return_quantity"],

        # Expected aggregate columns.
        ["date_dim.d_year", "item.i_brand_id", "item.i_class_id",
         "item.i_category_id", "item.i_manufact_id"],

        # Expected order by columns.
        ["catalog_returns.cr_return_amount", "catalog_returns.cr_return_quantity",
         "catalog_sales.cs_ext_sales_price", "catalog_sales.cs_quantity",
         "store_returns.sr_return_amt", "store_returns.sr_return_quantity",
         "store_sales.ss_ext_sales_price", "store_sales.ss_quantity",
         "web_returns.wr_return_amt", "web_returns.wr_return_quantity",
         "web_sales.ws_ext_sales_price", "web_sales.ws_quantity"])

  @CustomClusterTestSuite.with_args(
      cluster_size=1, impalad_graceful_shutdown=True, disable_log_buffering=True,
      impalad_args="--enable_workload_mgmt --query_log_write_interval_s=1",
      catalogd_args="--enable_workload_mgmt")
  def test_sql_having(self, vector):
    """Asserts the values inserted into the workload management table columns match
       what is expected when the sql contains both a group by and a having clause that
       each reference different columns."""
    self._assert_all(vector, "select ss_item_sk as Item, count(ss_item_sk) as "
        "Times_Purchased, sum(ss_quantity) as Total_Quantity_Purchased from store_sales "
        "group by ss_item_sk having count(ss_quantity) >= 100 order by sum(ss_quantity)",
        ["store_sales"],
        ["store_sales.ss_item_sk", "store_sales.ss_quantity"],
        [],
        [],
        ["store_sales.ss_item_sk", "store_sales.ss_quantity"],
        ["store_sales.ss_quantity"])

  @CustomClusterTestSuite.with_args(
      cluster_size=1, impalad_graceful_shutdown=True, disable_log_buffering=True,
      impalad_args="--enable_workload_mgmt --query_log_write_interval_s=1",
      catalogd_args="--enable_workload_mgmt")
  def test_complex_types(self, vector):
    """Asserts that queries using subtypes of complex type columns only report the column
       name and do not also report the subtype."""
    self._assert_all(vector,
        "select nested_struct_col.f2.f12.f21 from functional.allcomplextypes",
        ["allcomplextypes"],
        ["allcomplextypes.nested_struct_col"],
        [],
        [],
        [],
        [],
        "functional")

  @CustomClusterTestSuite.with_args(
      cluster_size=1, impalad_graceful_shutdown=True, disable_log_buffering=True,
      impalad_args="--enable_workload_mgmt --query_log_write_interval_s=1",
      catalogd_args="--enable_workload_mgmt")
  def test_arithmetic(self, vector):
    """Asserts that arithmetic operations record the columns used in them."""
    self._assert_all(vector, "select (tinyint_col + smallint_col), sum(float_col) from "
        "alltypes where (tinyint_col + smallint_col) < 10 "
        "group by tinyint_col, smallint_col",
        ["alltypes"],
        ["alltypes.tinyint_col", "alltypes.smallint_col", "alltypes.float_col"],
        ["alltypes.smallint_col", "alltypes.tinyint_col"],
        [],
        ["alltypes.smallint_col", "alltypes.tinyint_col"],
        [],
        "functional")

  @CustomClusterTestSuite.with_args(start_args="--use_calcite_planner=true",
      cluster_size=1, impalad_graceful_shutdown=True,
      impalad_args="--enable_workload_mgmt --query_log_write_interval_s=1",
      catalogd_args="--enable_workload_mgmt")
  def test_tpcds_8_decimal(self, vector):
    """Runs the tpcds-decimal_v2-q8 query using the calcite planner and asserts the query
       completes successfully. See IMPALA-13505 for details on why this query in
       particular is tested."""

    client = self.get_client(vector.get_value("protocol"))
    assert client.execute("use tpcds").success

    res = client.execute("SELECT s_store_name, sum(ss_net_profit) FROM store_sales,"
        "date_dim, store, (SELECT ca_zip FROM (SELECT SUBSTRING(ca_zip, 1, 5) ca_zip "
        "FROM customer_address WHERE SUBSTRING(ca_zip, 1, 5) IN ("
        "'24128', '76232', '65084', '87816', '83926', '77556', '20548',"
        "'26231', '43848', '15126', '91137', '61265', '98294', '25782', '17920',"
        "'18426', '98235', '40081', '84093', '28577', '55565', '17183', '54601') "
        "INTERSECT SELECT ca_zip FROM (SELECT SUBSTRING(ca_zip, 1, 5) ca_zip, "
        "count(*) cnt FROM customer_address, customer "
        "WHERE ca_address_sk = c_current_addr_sk AND c_preferred_cust_flag='Y' "
        "GROUP BY ca_zip HAVING count(*) > 10)A1)A2) V1 "
        "WHERE ss_store_sk = s_store_sk AND ss_sold_date_sk = d_date_sk AND d_qoy = 2 "
        "AND d_year = 1998 AND (SUBSTRING(s_zip, 1, 2) = SUBSTRING(V1.ca_zip, 1, 2)) "
        "GROUP BY s_store_name ORDER BY s_store_name LIMIT 100")
    assert res.success

    # Wait for the query to be written to the completed queries table.
    self.cluster.get_first_impalad().service.wait_for_metric_value(
        "impala-server.completed-queries.written", 1, 60)
