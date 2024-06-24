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
import string
import tempfile

from getpass import getuser
from ImpalaService import ImpalaHiveServer2Service
from random import choice, randint
from signal import SIGRTMIN
from TCLIService import TCLIService
from thrift.transport.TSocket import TSocket
from thrift.transport.TTransport import TBufferedTransport
from thrift.protocol import TBinaryProtocol
from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.impala_test_suite import IMPALAD_HS2_HOST_PORT
from tests.common.test_dimensions import create_single_exec_option_dimension
from tests.common.test_vector import ImpalaTestDimension
from tests.util.retry import retry
from tests.util.workload_management import assert_query
from time import sleep, time


class TestQueryLogTableBase(CustomClusterTestSuite):
  """Base class for all query log tests. Sets up the tests to use the Beeswax and HS2
     client protocols."""

  WM_DB = "sys"
  QUERY_TBL = "{0}.impala_query_log".format(WM_DB)
  PROTOCOL_BEESWAX = "beeswax"
  PROTOCOL_HS2 = "hs2"

  @classmethod
  def add_test_dimensions(cls):
    super(TestQueryLogTableBase, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(create_single_exec_option_dimension())
    cls.ImpalaTestMatrix.add_dimension(ImpalaTestDimension('protocol',
        cls.PROTOCOL_BEESWAX, cls.PROTOCOL_HS2))

  def setup_method(self, method):
    super(TestQueryLogTableBase, self).setup_method(method)
    # These tests run very quickly and can actually complete before Impala has finished
    # creating the completed queries table. Thus, to make these tests more robust, this
    # code checks to make sure the table create has finished before returning.
    create_match = self.assert_impalad_log_contains("INFO", r'\]\s+(\w+:\w+)\]\s+'
        r'Analyzing query: CREATE TABLE IF NOT EXISTS {}'.format(self.QUERY_TBL),
        timeout_s=60)
    self.assert_impalad_log_contains("INFO", r'Query successfully unregistered: '
        r'query_id={}'.format(create_match.group(1)),
        timeout_s=60)

  def get_client(self, protocol):
    """Retrieves the default Impala client for the specified protocol. This client is
       automatically closed after the test completes."""
    if protocol == self.PROTOCOL_BEESWAX:
      return self.client
    elif protocol == self.PROTOCOL_HS2:
      return self.hs2_client
    raise Exception("unknown protocol: {0}".format(protocol))


class TestQueryLogTableBeeswax(TestQueryLogTableBase):
  """Tests to assert the query log table is correctly populated when using the Beeswax
     client protocol."""

  @classmethod
  def add_test_dimensions(cls):
    super(TestQueryLogTableBeeswax, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_constraint(lambda v:
        v.get_value('protocol') == 'beeswax')

  @classmethod
  def get_workload(self):
    return 'functional-query'

  CACHE_DIR = tempfile.mkdtemp(prefix="cache_dir")
  MAX_SQL_PLAN_LEN = 2000
  LOG_DIR_MAX_WRITES = tempfile.mkdtemp(prefix="max_writes")
  FLUSH_MAX_RECORDS_CLUSTER_ID = "test_query_log_max_records_" + str(int(time()))
  FLUSH_MAX_RECORDS_QUERY_COUNT = 30

  @CustomClusterTestSuite.with_args(impalad_args="--enable_workload_mgmt "
                                                 "--query_log_write_interval_s=1 "
                                                 "--cluster_id=test_max_select "
                                                 "--query_log_max_sql_length={0} "
                                                 "--query_log_max_plan_length={0}"
                                                 .format(MAX_SQL_PLAN_LEN),
                                    catalogd_args="--enable_workload_mgmt",
                                    impalad_graceful_shutdown=True)
  def test_table_structure(self, vector):
    """Asserts that the log table has the expected columns."""
    self.run_test_case('QueryTest/workload-management-log', vector)

  @CustomClusterTestSuite.with_args(impalad_args="--enable_workload_mgmt "
                                                 "--query_log_write_interval_s=1 "
                                                 "--cluster_id=test_max_select "
                                                 "--query_log_max_sql_length={0} "
                                                 "--query_log_max_plan_length={0}"
                                                 .format(MAX_SQL_PLAN_LEN),
                                    catalogd_args="--enable_workload_mgmt",
                                    impalad_graceful_shutdown=True)
  def test_lower_max_sql_plan(self, vector):
    """Asserts that length limits on the sql and plan columns in the completed queries
       table are respected."""
    client = self.get_client(vector.get_value('protocol'))
    rand_long_str = "".join(choice(string.ascii_letters) for _ in
        range(self.MAX_SQL_PLAN_LEN))

    # Run the query async to avoid fetching results since fetching such a large result was
    # causing the execution to take a very long time.
    handle = client.execute_async("select '{0}'".format(rand_long_str))
    query_id = handle.get_handle().id
    client.wait_for_finished_timeout(handle, 10)
    client.close_query(handle)

    self.cluster.get_first_impalad().service.wait_for_metric_value(
        "impala-server.completed-queries.written", 1, 60)

    # Force Impala to process the inserts to the completed queries table.
    client.execute("refresh " + self.QUERY_TBL)

    res = client.execute("select length(sql),plan from {0} where query_id='{1}'"
        .format(self.QUERY_TBL, query_id))
    assert res.success
    assert len(res.data) == 1

    data = res.data[0].split("\t")
    assert len(data) == 2
    assert int(data[0]) == self.MAX_SQL_PLAN_LEN - 1, "incorrect sql statement length"
    assert len(data[1]) == self.MAX_SQL_PLAN_LEN - data[1].count("\n") - 1, \
        "incorrect plan length"

  @CustomClusterTestSuite.with_args(impalad_args="--enable_workload_mgmt "
                                                 "--query_log_write_interval_s=1 "
                                                 "--cluster_id=test_max_select",
                                    catalogd_args="--enable_workload_mgmt",
                                    impalad_graceful_shutdown=True)
  def test_sql_plan_too_long(self, vector):
    """Asserts that very long queries have their corresponding plan and sql columns
       shortened in the completed queries table."""
    client = self.get_client(vector.get_value('protocol'))
    rand_long_str = "".join(choice(string.ascii_letters) for _ in range(16778200))

    client.set_configuration_option("MAX_STATEMENT_LENGTH_BYTES", 16780000)
    handle = client.execute_async("select '{0}'".format(rand_long_str))
    query_id = handle.get_handle().id
    client.wait_for_finished_timeout(handle, 10)
    client.close_query(handle)

    self.cluster.get_first_impalad().service.wait_for_metric_value(
        "impala-server.completed-queries.written", 1, 60)

    # Force Impala to process the inserts to the completed queries table.
    client.execute("refresh " + self.QUERY_TBL)

    client.set_configuration_option("MAX_ROW_SIZE", 35000000)
    res = client.execute("select length(sql),plan from {0} where query_id='{1}'"
        .format(self.QUERY_TBL, query_id))
    assert res.success
    assert len(res.data) == 1
    data = res.data[0].split("\t")
    assert len(data) == 2
    assert data[0] == "16777215"

    # Newline characters are not counted by Impala's length function.
    assert len(data[1]) == 16777216 - data[1].count("\n") - 1

  @CustomClusterTestSuite.with_args(impalad_args="--enable_workload_mgmt "
                                                 "--query_log_write_interval_s=1 "
                                                 "--cluster_id=test_query_hist_1 "
                                                 "--query_log_size=0 "
                                                 "--query_log_size_in_bytes=0",
                                    catalogd_args="--enable_workload_mgmt",
                                    impalad_graceful_shutdown=True)
  def test_no_query_log(self, vector):
    """Asserts queries are written to the completed queries table when the in-memory
       query log queue is turned off."""
    client = self.get_client(vector.get_value('protocol'))

    # Run a select query.
    random_val = randint(1, 1000000)
    select_sql = "select {0}".format(random_val)
    res = client.execute(select_sql)
    assert res.success
    self.cluster.get_first_impalad().service.wait_for_metric_value(
        "impala-server.completed-queries.written", 1, 60)

    # Force Impala to process the inserts to the completed queries table.
    client.execute("refresh " + self.QUERY_TBL)

    actual = client.execute("select sql from {0} where query_id='{1}'".format(
        self.QUERY_TBL, res.query_id))
    assert actual.success
    assert len(actual.data) == 1
    assert actual.data[0] == select_sql

  @CustomClusterTestSuite.with_args(impalad_args="--enable_workload_mgmt "
                                                 "--query_log_write_interval_s=1 "
                                                 "--cluster_id=test_query_hist_2 "
                                                 "--always_use_data_cache "
                                                 "--data_cache={0}:5GB".format(CACHE_DIR),
                                    catalogd_args="--enable_workload_mgmt",
                                    impalad_graceful_shutdown=True,
                                    cluster_size=1)
  def test_query_data_cache(self, vector):
    """Asserts the values written to the query log table match the values from the
       query profile. Specifically focuses on the data cache metrics."""
    client = self.get_client(vector.get_value('protocol'))

    # Select all rows from the test table. Run the query multiple times to ensure data
    # is cached.
    warming_query_count = 3
    select_sql = "select * from functional.tinytable"
    for i in range(warming_query_count):
      res = client.execute(select_sql)
      assert res.success
      self.cluster.get_first_impalad().service.wait_for_metric_value(
          "impala-server.completed-queries.written", i + 1, 60)

    # Wait for the cache to be written to disk.
    self.cluster.get_first_impalad().service.wait_for_metric_value(
        "impala-server.io-mgr.remote-data-cache-num-writes", 1, 60)

    # Run the same query again so results are read from the data cache.
    res = client.execute(select_sql, fetch_profile_after_close=True)
    assert res.success
    self.cluster.get_first_impalad().service.wait_for_metric_value(
        "impala-server.completed-queries.written", warming_query_count + 1, 60)

    data = assert_query(self.QUERY_TBL, client, "test_query_hist_2",
        res.runtime_profile)

    # Since the assert_query function only asserts that the bytes read from cache
    # column is equal to the bytes read from cache in the profile, there is a potential
    # for this test to not actually assert anything different than other tests. Thus, an
    # additional assert is needed to ensure that there actually was data read from the
    # cache.
    assert data["BYTES_READ_CACHE_TOTAL"] != "0", "bytes read from cache total was " \
        "zero, test did not assert anything"

  @CustomClusterTestSuite.with_args(impalad_args="--enable_workload_mgmt "
                                                 "--query_log_write_interval_s=5",
                                    impala_log_dir=LOG_DIR_MAX_WRITES,
                                    catalogd_args="--enable_workload_mgmt",
                                    impalad_graceful_shutdown=True)
  def test_max_attempts_exceeded(self, vector):
    """Asserts that completed queries are only attempted 3 times to be inserted into the
       completed queries table. This test deletes the completed queries table thus it must
       not come last otherwise the table stays deleted. Subsequent tests will re-create
       the table."""

    print("USING LOG DIRECTORY: {0}".format(self.LOG_DIR_MAX_WRITES))

    impalad = self.cluster.get_first_impalad()
    client = self.get_client(vector.get_value('protocol'))

    res = client.execute("drop table {0} purge".format(self.QUERY_TBL))
    assert res.success
    impalad.service.wait_for_metric_value(
        "impala-server.completed-queries.scheduled-writes", 3, 60)
    impalad.service.wait_for_metric_value("impala-server.completed-queries.failure", 3,
        60)

    query_count = 0

    # Allow time for logs to be written to disk.
    sleep(5)

    with open(os.path.join(self.LOG_DIR_MAX_WRITES, "impalad.ERROR")) as file:
      for line in file:
        if line.find('could not write completed query table="{0}" query_id="{1}"'
                          .format(self.QUERY_TBL, res.query_id)) >= 0:
          query_count += 1

    assert query_count == 1

    assert impalad.service.get_metric_value(
      "impala-server.completed-queries.max-records-writes") == 0
    assert impalad.service.get_metric_value(
      "impala-server.completed-queries.queued") == 0
    assert impalad.service.get_metric_value(
      "impala-server.completed-queries.failure") == 3
    assert impalad.service.get_metric_value(
      "impala-server.completed-queries.scheduled-writes") == 4
    assert impalad.service.get_metric_value(
      "impala-server.completed-queries.written") == 0

  @CustomClusterTestSuite.with_args(impalad_args="--enable_workload_mgmt "
                                                 "--query_log_max_queued={0} "
                                                 "--query_log_write_interval_s=9999 "
                                                 "--cluster_id={1}"
                                                 .format(FLUSH_MAX_RECORDS_QUERY_COUNT,
                                                 FLUSH_MAX_RECORDS_CLUSTER_ID),
                                    catalogd_args="--enable_workload_mgmt",
                                    default_query_options=[
                                      ('statement_expression_limit', 1024)],
                                    impalad_graceful_shutdown=True)
  def test_flush_on_queued_count_exceeded(self, vector):
    """Asserts that queries that have completed are written to the query log table when
       the maximum number of queued records it reached. Also verifies that writing
       completed queries is not limited by default statement_expression_limit."""

    impalad = self.cluster.get_first_impalad()
    client = self.get_client(vector.get_value('protocol'))

    rand_str = "{0}-{1}".format(vector.get_value('protocol'), time())

    test_sql = "select '{0}','{1}'".format(rand_str,
        self.FLUSH_MAX_RECORDS_CLUSTER_ID)
    test_sql_assert = "select '{0}', count(*) from {1} where sql='{2}'".format(
        rand_str, self.QUERY_TBL, test_sql.replace("'", r"\'"))

    for _ in range(0, self.FLUSH_MAX_RECORDS_QUERY_COUNT):
      res = client.execute(test_sql)
      assert res.success

    # Running this query results in the number of queued completed queries to exceed
    # the max and thus all completed queries will be written to the query log table.
    res = client.execute(test_sql_assert)
    assert res.success
    assert 1 == len(res.data)
    assert "0" == res.data[0].split("\t")[1]

    # Wait until the completed queries have all been written out because the max queued
    # count was exceeded.
    impalad.service.wait_for_metric_value(
        "impala-server.completed-queries.max-records-writes", 1, 60)
    self.cluster.get_first_impalad().service.wait_for_metric_value(
        "impala-server.completed-queries.written",
        self.FLUSH_MAX_RECORDS_QUERY_COUNT + 1, 60)

    # Force Impala to process the inserts to the completed queries table.
    client.execute("refresh " + self.QUERY_TBL)

    # This query will remain queued due to the long write interval and max queued
    # records limit not being reached.
    res = client.execute(r"select count(*) from {0} where sql like 'select \'{1}\'%'"
        .format(self.QUERY_TBL, rand_str))
    assert res.success
    assert 1 == len(res.data)
    assert str(self.FLUSH_MAX_RECORDS_QUERY_COUNT + 1) == res.data[0]
    impalad.service.wait_for_metric_value(
        "impala-server.completed-queries.queued", 2, 60)

    assert impalad.service.get_metric_value(
        "impala-server.completed-queries.max-records-writes") == 1
    assert impalad.service.get_metric_value(
        "impala-server.completed-queries.scheduled-writes") == 0
    assert impalad.service.get_metric_value("impala-server.completed-queries.written") \
        == self.FLUSH_MAX_RECORDS_QUERY_COUNT + 1
    assert impalad.service.get_metric_value(
        "impala-server.completed-queries.queued") == 2

  @CustomClusterTestSuite.with_args(impalad_args="--enable_workload_mgmt "
                                                 "--query_log_write_interval_s=1",
                                    cluster_size=3,
                                    num_exclusive_coordinators=2,
                                    catalogd_args="--enable_workload_mgmt",
                                    impalad_graceful_shutdown=True)
  def test_dedicated_coordinator_no_mt_dop(self, vector):
    """Asserts the values written to the query log table match the values from the
       query profile when dedicated coordinators are used."""
    client = self.get_client(vector.get_value('protocol'))
    test_sql = "select * from functional.tinytable"

    # Select all rows from the test table.
    res = client.execute(test_sql, fetch_profile_after_close=True)
    assert res.success

    self.cluster.get_first_impalad().service.wait_for_metric_value(
        "impala-server.completed-queries.written", 1, 60)

    client2 = self.create_client_for_nth_impalad(1, vector.get_value('protocol'))
    try:
      assert client2 is not None
      assert_query(self.QUERY_TBL, client2, "",
          res.runtime_profile)
    finally:
      client2.close()

  @CustomClusterTestSuite.with_args(impalad_args="--enable_workload_mgmt "
                                                 "--query_log_write_interval_s=1",
                                    cluster_size=3,
                                    num_exclusive_coordinators=2,
                                    catalogd_args="--enable_workload_mgmt",
                                    impalad_graceful_shutdown=True)
  def test_dedicated_coordinator_with_mt_dop(self, vector):
    """Asserts the values written to the query log table match the values from the
       query profile when dedicated coordinators are used along with an MT_DOP setting
       greater than 0."""
    client = self.get_client(vector.get_value('protocol'))
    test_sql = "select * from functional.tinytable"

    # Select all rows from the test table.
    client.set_configuration_option("MT_DOP", "4")
    res = client.execute(test_sql, fetch_profile_after_close=True)
    assert res.success

    self.cluster.get_first_impalad().service.wait_for_metric_value(
        "impala-server.completed-queries.written", 1, 60)

    client2 = self.create_client_for_nth_impalad(1, vector.get_value('protocol'))
    try:
      assert client2 is not None
      assert_query(self.QUERY_TBL, client2, "",
          res.runtime_profile)
    finally:
      client2.close()


class TestQueryLogOtherTable(TestQueryLogTableBase):
  """Tests to assert that query_log_table_name works with non-default value."""

  OTHER_TBL = "completed_queries_table_{0}".format(int(time()))
  # Used in TestQueryLogTableBase.setup_method
  QUERY_TBL = "{0}.{1}".format(TestQueryLogTableBase.WM_DB, OTHER_TBL)

  @classmethod
  def add_test_dimensions(cls):
    super(TestQueryLogOtherTable, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_constraint(lambda v:
        v.get_value('protocol') == 'beeswax')

  @CustomClusterTestSuite.with_args(impalad_args="--enable_workload_mgmt "
                                                 "--query_log_write_interval_s=1 "
                                                 "--blacklisted_dbs=information_schema "
                                                 "--query_log_table_name={0}"
                                                 .format(OTHER_TBL),
                                    catalogd_args="--enable_workload_mgmt "
                                                  "--blacklisted_dbs=information_schema",
                                    impalad_graceful_shutdown=True)
  def test_renamed_log_table(self, vector):
    """Asserts that the completed queries table can be renamed."""

    client = self.get_client(vector.get_value('protocol'))

    try:
      res = client.execute("show tables in {0}".format(self.WM_DB))
      assert res.success
      assert len(res.data) > 0, "could not find any tables in database {0}" \
          .format(self.WM_DB)

      tbl_found = False
      for tbl in res.data:
        if tbl.startswith(self.OTHER_TBL):
          tbl_found = True
          break
      assert tbl_found, "could not find table '{0}' in database '{1}'" \
          .format(self.OTHER_TBL, self.WM_DB)
    finally:
      client.execute("drop table {0}.{1} purge".format(self.WM_DB, self.OTHER_TBL))


class TestQueryLogTableHS2(TestQueryLogTableBase):
  """Tests to assert the query log table is correctly populated when using the HS2
     client protocol."""

  HS2_OPERATIONS_CLUSTER_ID = "hs2-operations-" + str(int(time()))

  @classmethod
  def add_test_dimensions(cls):
    super(TestQueryLogTableHS2, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_constraint(lambda v:
        v.get_value('protocol') == 'hs2')

  @CustomClusterTestSuite.with_args(impalad_args="--enable_workload_mgmt "
                                                 "--query_log_write_interval_s=1 "
                                                 "--cluster_id={}"
                                                 .format(HS2_OPERATIONS_CLUSTER_ID),
                                    catalogd_args="--enable_workload_mgmt",
                                    cluster_size=2,
                                    impalad_graceful_shutdown=True)
  def test_hs2_metadata_operations(self, vector):
    """Certain HS2 operations appear to Impala as a special kind of query. Specifically,
       these operations have a type of unknown and a normally invalid sql syntax. This
       test asserts those queries are not written to the completed queries table since
       they are trivial."""
    client = self.get_client(vector.get_value('protocol'))

    host, port = IMPALAD_HS2_HOST_PORT.split(":")
    socket = TSocket(host, port)
    transport = TBufferedTransport(socket)
    transport.open()
    protocol = TBinaryProtocol.TBinaryProtocol(transport)
    hs2_client = ImpalaHiveServer2Service.Client(protocol)

    # Asserts the response from an HS2 operation indicates success.
    def assert_resp(resp):
      assert resp.status.statusCode == TCLIService.TStatusCode.SUCCESS_STATUS

    # Closes an HS2 operation.
    def close_op(client, resp):
      close_operation_req = TCLIService.TCloseOperationReq()
      close_operation_req.operationHandle = resp.operationHandle
      assert_resp(hs2_client.CloseOperation(close_operation_req))

    try:
      # Open a new HS2 session.
      open_session_req = TCLIService.TOpenSessionReq()
      open_session_req.username = getuser()
      open_session_req.configuration = dict()
      open_sess_resp = hs2_client.OpenSession(open_session_req)
      assert_resp(open_sess_resp)

      # Test the get_type_info query.
      get_typeinfo_req = TCLIService.TGetTypeInfoReq()
      get_typeinfo_req.sessionHandle = open_sess_resp.sessionHandle
      get_typeinfo_resp = hs2_client.GetTypeInfo(get_typeinfo_req)
      assert_resp(get_typeinfo_resp)
      close_op(hs2_client, get_typeinfo_resp)

      # Test the get_catalogs query.
      get_cats_req = TCLIService.TGetCatalogsReq()
      get_cats_req.sessionHandle = open_sess_resp.sessionHandle
      get_cats_resp = hs2_client.GetCatalogs(get_cats_req)
      assert_resp(get_cats_resp)
      close_op(hs2_client, get_cats_resp)

      # Test the get_schemas query.
      get_schemas_req = TCLIService.TGetSchemasReq()
      get_schemas_req.sessionHandle = open_sess_resp.sessionHandle
      get_schemas_resp = hs2_client.GetSchemas(get_schemas_req)
      assert_resp(get_schemas_resp)
      close_op(hs2_client, get_schemas_resp)

      # Test the get_tables query.
      get_tables_req = TCLIService.TGetTablesReq()
      get_tables_req.sessionHandle = open_sess_resp.sessionHandle
      get_tables_req.schemaName = self.WM_DB
      get_tables_resp = hs2_client.GetTables(get_tables_req)
      assert_resp(get_tables_resp)
      close_op(hs2_client, get_tables_resp)

      # Test the get_table_types query.
      get_tbl_typ_req = TCLIService.TGetTableTypesReq()
      get_tbl_typ_req.sessionHandle = open_sess_resp.sessionHandle
      get_tbl_typ_req.schemaName = self.WM_DB
      get_tbl_typ_resp = hs2_client.GetTableTypes(get_tbl_typ_req)
      assert_resp(get_tbl_typ_resp)
      close_op(hs2_client, get_tbl_typ_resp)

      # Test the get_columns query.
      get_cols_req = TCLIService.TGetColumnsReq()
      get_cols_req.sessionHandle = open_sess_resp.sessionHandle
      get_cols_req.schemaName = 'functional'
      get_cols_req.tableName = 'parent_table'
      get_cols_resp = hs2_client.GetColumns(get_cols_req)
      assert_resp(get_cols_resp)
      close_op(hs2_client, get_cols_resp)

      # Test the get_primary_keys query.
      get_pk_req = TCLIService.TGetPrimaryKeysReq()
      get_pk_req.sessionHandle = open_sess_resp.sessionHandle
      get_pk_req.schemaName = 'functional'
      get_pk_req.tableName = 'parent_table'
      get_pk_resp = hs2_client.GetPrimaryKeys(get_pk_req)
      assert_resp(get_pk_resp)
      close_op(hs2_client, get_pk_resp)

      # Test the get_cross_reference query.
      get_cr_req = TCLIService.TGetCrossReferenceReq()
      get_cr_req.sessionHandle = open_sess_resp.sessionHandle
      get_cr_req.parentSchemaName = "functional"
      get_cr_req.foreignSchemaName = "functional"
      get_cr_req.parentTableName = "parent_table"
      get_cr_req.foreignTableName = "child_table"
      get_cr_resp = hs2_client.GetCrossReference(get_cr_req)
      assert_resp(get_cr_resp)
      close_op(hs2_client, get_cr_resp)

      close_session_req = TCLIService.TCloseSessionReq()
      close_session_req.sessionHandle = open_sess_resp.sessionHandle
      resp = hs2_client.CloseSession(close_session_req)
      assert resp.status.statusCode == TCLIService.TStatusCode.SUCCESS_STATUS
    finally:
      socket.close()

    # Execute a general query and wait for it to appear in the completed queries table to
    # ensure there are no false positives caused by the assertion query executing before
    # Impala has a chance to write queued queries to the completed queries table.
    assert client.execute("select 1").success
    self.cluster.get_first_impalad().service.wait_for_metric_value(
          "impala-server.completed-queries.written", 1, 30)

    # Force Impala to process the inserts to the completed queries table.
    client.execute("refresh {}".format(self.QUERY_TBL))

    # Assert only the one expected query was written to the completed queries table.
    assert_results = client.execute("select count(*) from {} where cluster_id='{}'"
        .format(self.QUERY_TBL, self.HS2_OPERATIONS_CLUSTER_ID))
    assert assert_results.success
    assert assert_results.data[0] == "1"

  @CustomClusterTestSuite.with_args(impalad_args="--enable_workload_mgmt "
                                                 "--query_log_write_interval_s=1 "
                                                 "--cluster_id=test_query_hist_mult",
                                    catalogd_args="--enable_workload_mgmt",
                                    cluster_size=2,
                                    impalad_graceful_shutdown=True)
  def test_query_multiple_tables(self, vector):
    """Asserts the values written to the query log table match the values from the
       query profile for a query that reads from multiple tables."""
    client = self.get_client(vector.get_value('protocol'))

    # Select all rows from the test table.
    client.set_configuration_option("MAX_MEM_ESTIMATE_FOR_ADMISSION", "10MB")
    res = client.execute("select a.zip,a.income,b.timezone,c.timezone from "
        "functional.zipcode_incomes a inner join functional.zipcode_timezones b on "
        "a.zip = b.zip inner join functional.alltimezones c on b.timezone = c.timezone",
        fetch_profile_after_close=True)
    assert res.success
    self.cluster.get_first_impalad().service.wait_for_metric_value(
        "impala-server.completed-queries.written", 1, 60)

    client2 = self.create_client_for_nth_impalad(1, vector.get_value('protocol'))
    try:
      assert client2 is not None
      assert_query(self.QUERY_TBL, client2, "test_query_hist_mult", res.runtime_profile,
          max_mem_for_admission=10485760)
    finally:
      client2.close()

  @CustomClusterTestSuite.with_args(impalad_args="--enable_workload_mgmt "
                                                 "--query_log_write_interval_s=1 "
                                                 "--cluster_id=test_query_hist_3",
                                    catalogd_args="--enable_workload_mgmt",
                                    impalad_graceful_shutdown=True)
  def test_insert_select(self, vector, unique_database,
      unique_name):
    """Asserts the values written to the query log table match the values from the
       query profile for a query that insert selects."""
    tbl_name = "{0}.{1}".format(unique_database, unique_name)
    client = self.get_client(vector.get_value('protocol'))

    # Create the destination test table.
    assert client.execute("create table {0} (identifier INT, product_name STRING) "
        .format(tbl_name)).success, "could not create source table"

    # Insert select into the destination table.
    res = client.execute("insert into {0} (identifier, product_name) select id, "
        "string_col from functional.alltypes limit 50".format(tbl_name),
        fetch_profile_after_close=True)
    assert res.success, "could not insert select"

    # Include the two queries run by the unique_database fixture setup.
    self.cluster.get_first_impalad().service.wait_for_metric_value(
        "impala-server.completed-queries.written", 4, 60)

    client2 = self.create_client_for_nth_impalad(2, vector.get_value('protocol'))
    try:
      assert client2 is not None
      assert_query(self.QUERY_TBL, client2, "test_query_hist_3", res.runtime_profile)
    finally:
      client2.close()

  @CustomClusterTestSuite.with_args(impalad_args="--enable_workload_mgmt "
                                                 "--query_log_write_interval_s=15",
                                    catalogd_args="--enable_workload_mgmt",
                                    impalad_graceful_shutdown=True)
  def test_flush_on_interval(self, vector):
    """Asserts that queries that have completed are written to the query log table
       after the specified write interval elapses."""

    client = self.get_client(vector.get_value('protocol'))

    query_count = 10

    for i in range(query_count):
      res = client.execute("select sleep(1000)")
      assert res.success

    # At least 10 seconds have already elapsed, wait up to 10 more seconds for the
    # queries to be written to the completed queries table.
    self.cluster.get_first_impalad().service.wait_for_metric_value(
      "impala-server.completed-queries.written", query_count, 10)

  @CustomClusterTestSuite.with_args(impalad_args="--enable_workload_mgmt "
                                                 "--query_log_write_interval_s=9999 "
                                                 "--shutdown_grace_period_s=0 "
                                                 "--shutdown_deadline_s=15",
                                    catalogd_args="--enable_workload_mgmt")
  def test_flush_on_shutdown(self, vector):
    """Asserts that queries that have completed but are not yet written to the query
       log table are flushed to the table before the coordinator exits. Graceful shutdown
       for 2nd coordinator not needed because query_log_write_interval_s is very long."""

    impalad = self.cluster.get_first_impalad()
    client = self.get_client(vector.get_value('protocol'))

    # Execute sql statements to ensure all get written to the query log table.
    sql1 = client.execute("select 1")
    assert sql1.success

    sql2 = client.execute("select 2")
    assert sql2.success

    sql3 = client.execute("select 3")
    assert sql3.success

    impalad.service.wait_for_metric_value("impala-server.completed-queries.queued", 3,
        60)

    impalad.kill_and_wait_for_exit(SIGRTMIN)

    client2 = self.create_client_for_nth_impalad(1, vector.get_value('protocol'))

    try:
      def assert_func(last_iteration):
        results = client2.execute("select query_id,sql from {0} where query_id in "
                                  "('{1}','{2}','{3}')".format(self.QUERY_TBL,
                                  sql1.query_id, sql2.query_id, sql3.query_id))

        success = len(results.data) == 3
        if last_iteration:
          assert len(results.data) == 3

        return success

      assert retry(func=assert_func, max_attempts=5, sleep_time_s=3)
    finally:
      client2.close()


class TestQueryLogTableAll(TestQueryLogTableBase):
  """Tests to assert the query log table is correctly populated when using all the
     client protocols."""

  @CustomClusterTestSuite.with_args(impalad_args="--enable_workload_mgmt "
                                                 "--query_log_write_interval_s=1 "
                                                 "--cluster_id=test_query_hist_2",
                                    catalogd_args="--enable_workload_mgmt",
                                    impalad_graceful_shutdown=True)
  def test_ddl(self, vector, unique_database, unique_name):
    """Asserts the values written to the query log table match the values from the
       query profile for a DDL query."""
    create_tbl_sql = "create table {0}.{1} (id INT, product_name STRING) " \
        "partitioned by (category INT)".format(unique_database, unique_name)
    client = self.get_client(vector.get_value('protocol'))

    res = client.execute(create_tbl_sql, fetch_profile_after_close=True)
    assert res.success

    # Include the two queries run by the unique_database fixture setup.
    self.cluster.get_first_impalad().service.wait_for_metric_value(
        "impala-server.completed-queries.written", 3, 60)

    client2 = self.create_client_for_nth_impalad(2, vector.get_value('protocol'))
    try:
      assert client2 is not None
      assert_query(self.QUERY_TBL, client2, "test_query_hist_2", res.runtime_profile)
    finally:
      client2.close()

  @CustomClusterTestSuite.with_args(impalad_args="--enable_workload_mgmt "
                                                 "--query_log_write_interval_s=1 "
                                                 "--cluster_id=test_query_hist_3",
                                    catalogd_args="--enable_workload_mgmt",
                                    impalad_graceful_shutdown=True)
  def test_dml(self, vector, unique_database, unique_name):
    """Asserts the values written to the query log table match the values from the
       query profile for a DML query."""
    tbl_name = "{0}.{1}".format(unique_database, unique_name)
    client = self.get_client(vector.get_value('protocol'))

    # Create the test table.
    create_tbl_sql = "create table {0} (id INT, product_name STRING) " \
      "partitioned by (category INT)".format(tbl_name)
    create_tbl_results = client.execute(create_tbl_sql)
    assert create_tbl_results.success

    insert_sql = "insert into {0} (id,category,product_name) values " \
                  "(0,1,'the product')".format(tbl_name)
    res = client.execute(insert_sql, fetch_profile_after_close=True)
    assert res.success

    # Include the two queries run by the unique_database fixture setup.
    self.cluster.get_first_impalad().service.wait_for_metric_value(
        "impala-server.completed-queries.written", 4, 60)

    client2 = self.create_client_for_nth_impalad(2, vector.get_value('protocol'))
    try:
      assert client2 is not None
      assert_query(self.QUERY_TBL, client2, "test_query_hist_3", res.runtime_profile)
    finally:
      client2.close()

  @CustomClusterTestSuite.with_args(impalad_args="--enable_workload_mgmt "
                                                 "--query_log_write_interval_s=1 "
                                                 "--cluster_id=test_query_hist_2",
                                    catalogd_args="--enable_workload_mgmt",
                                    impalad_graceful_shutdown=True)
  def test_invalid_query(self, vector):
    """Asserts correct values are written to the completed queries table for a failed
       query. The query profile is used as the source of expected values."""
    client = self.get_client(vector.get_value('protocol'))

    # Assert an invalid query
    unix_now = time()
    try:
      client.execute("{0}".format(unix_now))
    except Exception as _:
      pass

    # Get the query id from the completed queries table since the call to execute errors
    # instead of return the results object which contains the query id.
    impalad = self.cluster.get_first_impalad()
    impalad.service.wait_for_metric_value("impala-server.completed-queries.written", 1,
        60)

    result = client.execute("select query_id from {0} where sql='{1}'"
                            .format(self.QUERY_TBL, unix_now),
                            fetch_profile_after_close=True)
    assert result.success
    assert len(result.data) == 1

    assert_query(query_tbl=self.QUERY_TBL, client=client,
        expected_cluster_id="test_query_hist_2", impalad=impalad, query_id=result.data[0])

  @CustomClusterTestSuite.with_args(impalad_args="--enable_workload_mgmt "
                                                 "--query_log_write_interval_s=1",
                                    catalogd_args="--enable_workload_mgmt",
                                    impalad_graceful_shutdown=True)
  def test_ignored_sqls_not_written(self, vector):
    """Asserts that expected queries are not written to the query log table."""
    client = self.get_client(vector.get_value('protocol'))

    sqls = {}
    sqls["use default"] = False
    sqls["USE default"] = False
    sqls["uSe default"] = False
    sqls["--mycomment\nuse default"] = False
    sqls["/*mycomment*/ use default"] = False

    sqls["set all"] = False
    sqls["SET all"] = False
    sqls["SeT all"] = False
    sqls["--mycomment\nset all"] = False
    sqls["/*mycomment*/ set all"] = False

    sqls["show tables"] = False
    sqls["SHOW tables"] = False
    sqls["ShoW tables"] = False
    sqls["ShoW create table {0}".format(self.QUERY_TBL)] = False
    sqls["show databases"] = False
    sqls["SHOW databases"] = False
    sqls["ShoW databases"] = False
    sqls["show schemas"] = False
    sqls["SHOW schemas"] = False
    sqls["ShoW schemas"] = False
    sqls["--mycomment\nshow tables"] = False
    sqls["/*mycomment*/ show tables"] = False
    sqls["/*mycomment*/ show tables"] = False
    sqls["/*mycomment*/ show create table {0}".format(self.QUERY_TBL)] = False
    sqls["/*mycomment*/ show files in {0}".format(self.QUERY_TBL)] = False
    sqls["/*mycomment*/ show functions"] = False
    sqls["/*mycomment*/ show data sources"] = False
    sqls["/*mycomment*/ show views"] = False
    sqls["show metadata tables in {0}".format(self.QUERY_TBL)] = False

    sqls["describe database default"] = False
    sqls["/*mycomment*/ describe database default"] = False
    sqls["describe {0}".format(self.QUERY_TBL)] = False
    sqls["/*mycomment*/ describe {0}".format(self.QUERY_TBL)] = False
    sqls["describe history {0}".format(self.QUERY_TBL)] = False
    sqls["/*mycomment*/ describe history {0}".format(self.QUERY_TBL)] = False
    sqls["select 1"] = True

    control_queries_count = 0
    for sql, experiment_control in sqls.items():
      results = client.execute(sql)
      assert results.success, "could not execute query '{0}'".format(sql)
      sqls[sql] = results.query_id

      # Ensure at least one sql statement was written to the completed queries table
      # to avoid false negatives where the sql statements that are ignored are not
      # written to the completed queries table because of another issue. Does not check
      # the completed-queries.written metric because, if another query that should not
      # have been written to the completed queries was actually written, the metric will
      # be wrong.
      if experiment_control:
        control_queries_count += 1
        sql_results = None
        for _ in range(6):
          sql_results = client.execute("select * from {0} where query_id='{1}'".format(
            self.QUERY_TBL, results.query_id))
          control_queries_count += 1
          if sql_results.success and len(sql_results.data) == 1:
            break
          else:
            # The query is not yet available in the completed queries table, wait before
            # checking again.
            sleep(5)
        assert sql_results.success
        assert len(sql_results.data) == 1, "query not found in completed queries table"
        sqls.pop(sql)

    for sql, query_id in sqls.items():
      log_results = client.execute("select * from {0} where query_id='{1}'"
                                    .format(self.QUERY_TBL, query_id))
      assert log_results.success
      assert len(log_results.data) == 0, "found query in query log table: {0}".format(sql)

    # Assert there was one query per sql item written to the query log table. The queries
    # inserted into the completed queries table are the queries used to assert the ignored
    # queries were not written to the table.
    self.cluster.get_first_impalad().service.wait_for_metric_value(
        "impala-server.completed-queries.written", len(sqls) + control_queries_count, 60)
    assert self.cluster.get_first_impalad().service.get_metric_value(
        "impala-server.completed-queries.failure") == 0

  @CustomClusterTestSuite.with_args(impalad_args="--enable_workload_mgmt "
                                                 "--query_log_write_interval_s=1",
                                    catalogd_args="--enable_workload_mgmt",
                                    impalad_graceful_shutdown=True)
  def test_sql_injection_attempts(self, vector):
    client = self.get_client(vector.get_value('protocol'))
    impalad = self.cluster.get_first_impalad()

    # Try a sql injection attack with closing double quotes.
    sql1_str = "select * from functional.alltypes where string_col='product-2-3\"'"
    self.__run_sql_inject(impalad, client, sql1_str, "closing quotes", 1)

    # Try a sql injection attack with closing single quotes.
    sql1_str = "select * from functional.alltypes where string_col=\"product-2-3'\""
    self.__run_sql_inject(impalad, client, sql1_str, "closing quotes", 4)

    # Try a sql inject attack with terminating quote and semicolon.
    sql2_str = "select 1'); drop table {0}; select('".format(self.QUERY_TBL)
    self.__run_sql_inject(impalad, client, sql2_str, "terminating semicolon", 7)

    # Attempt to cause an error using multiline comments.
    sql3_str = "select 1' /* foo"
    self.__run_sql_inject(impalad, client, sql3_str, "multiline comments", 11, False)

    # Attempt to cause an error using single line comments.
    sql4_str = "select 1' -- foo"
    self.__run_sql_inject(impalad, client, sql4_str, "single line comments", 15, False)

  def __run_sql_inject(self, impalad, client, sql, test_case, expected_writes,
                       expect_success=True):
    # Capture coordinators "now" so we match only queries in this test case.
    start_time = None
    if not expect_success:
      utc_timestamp = self.execute_query('select utc_timestamp()')
      assert len(utc_timestamp.data) == 1
      start_time = utc_timestamp.data[0]

    sql_result = None
    try:
      sql_result = client.execute(sql)
    except Exception as e:
      if expect_success:
        raise e

    if expect_success:
      assert sql_result.success

    impalad.service.wait_for_metric_value(
        "impala-server.completed-queries.written", expected_writes, 60)

    # Force Impala to process the inserts to the completed queries table.
    client.execute("refresh " + self.QUERY_TBL)

    if expect_success:
      sql_verify = client.execute(
          "select sql from {0} where query_id='{1}'"
          .format(self.QUERY_TBL, sql_result.query_id))

      assert sql_verify.success, test_case
      assert len(sql_verify.data) == 1, "did not find query '{0}' in query log " \
                                        "table for test case '{1}" \
                                        .format(sql_result.query_id, test_case)
      assert sql_verify.data[0] == sql, test_case
    else:
      assert start_time is not None
      esc_sql = sql.replace("'", "\\'")
      sql_verify = client.execute("select sql from {0} where sql='{1}' "
                                  "and start_time_utc > '{2}'"
                                  .format(self.QUERY_TBL, esc_sql, start_time))
      assert sql_verify.success, test_case
      assert len(sql_verify.data) == 1, "did not find query '{0}' in query log " \
                                        "table for test case '{1}" \
                                        .format(esc_sql, test_case)


class TestQueryLogTableBufferPool(TestQueryLogTableBase):
  """Base class for all query log tests that set the buffer pool query option."""

  SCRATCH_DIR = tempfile.mkdtemp(prefix="scratch_dir")

  @classmethod
  def add_test_dimensions(cls):
    super(TestQueryLogTableBufferPool, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(ImpalaTestDimension('buffer_pool_limit',
        None, "14.97MB"))

  @CustomClusterTestSuite.with_args(impalad_args="--enable_workload_mgmt "
                                                 "--query_log_write_interval_s=1 "
                                                 "--cluster_id=test_query_hist_1 "
                                                 "--scratch_dirs={0}:5G"
                                                 .format(SCRATCH_DIR),
                                    catalogd_args="--enable_workload_mgmt",
                                    impalad_graceful_shutdown=True)
  def test_select(self, vector):
    """Asserts the values written to the query log table match the values from the
       query profile. If the buffer_pool_limit parameter is not None, then this test
       requires that the query spills to disk to assert that the spill metrics are correct
       in the completed queries table."""
    buffer_pool_limit = vector.get_value('buffer_pool_limit')
    client = self.get_client(vector.get_value('protocol'))
    test_sql = "select * from functional.tinytable"

    # When buffer pool limit is not None, the test is forcing the query to spill. Thus,
    # a large number of records is needed to force the spilling.
    if buffer_pool_limit is not None:
      test_sql = "select a.*,b.*,c.* from " \
        "functional.zipcode_incomes a inner join functional.zipcode_timezones b on " \
        "a.zip = b.zip inner join functional.alltimezones c on b.timezone = c.timezone"

    # Set up query configuration
    client.set_configuration_option("MAX_MEM_ESTIMATE_FOR_ADMISSION", "10MB")
    if buffer_pool_limit is not None:
      client.set_configuration_option("BUFFER_POOL_LIMIT", buffer_pool_limit)

    # Select all rows from the test table.
    res = client.execute(test_sql, fetch_profile_after_close=True)
    assert res.success

    self.cluster.get_first_impalad().service.wait_for_metric_value(
        "impala-server.completed-queries.written", 1, 60)

    client2 = self.create_client_for_nth_impalad(2, vector.get_value('protocol'))
    try:
      assert client2 is not None
      data = assert_query(self.QUERY_TBL, client2, "test_query_hist_1",
          res.runtime_profile, max_mem_for_admission=10485760)
    finally:
      client2.close()

    if buffer_pool_limit is not None:
      # Since the assert_query function only asserts that the compressed bytes spilled
      # column is equal to the compressed bytes spilled in the profile, there is a
      # potential for this test to not actually assert anything different than other
      # tests. Thus, an additional assert is needed to ensure that there actually was
      # data that was spilled.
      assert data["COMPRESSED_BYTES_SPILLED"] != "0", "compressed bytes spilled total " \
          "was zero, test did not assert anything"
