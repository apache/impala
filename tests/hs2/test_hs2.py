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
# Client tests for Impala's HiveServer2 interface

from __future__ import absolute_import, division, print_function
from builtins import range
from getpass import getuser
from contextlib import contextmanager
import json
import logging
import pytest
import random
import threading
import time
import uuid

try:
  from urllib.request import urlopen
except ImportError:
  from urllib2 import urlopen

from ImpalaService import ImpalaHiveServer2Service
from tests.common.environ import ImpalaTestClusterProperties
from tests.common.skip import SkipIfDockerizedCluster
from tests.hs2.hs2_test_suite import (HS2TestSuite, needs_session,
    operation_id_to_query_id, create_session_handle_without_secret,
    create_op_handle_without_secret, needs_session_cluster_properties)
from TCLIService import TCLIService

LOG = logging.getLogger('test_hs2')

SQLSTATE_GENERAL_ERROR = "HY000"


# Context manager that wraps a session. This is used over 'needs_session' to allow more
# direct access to the TOpenSessionReq parameters.
@contextmanager
def ScopedSession(hs2_client, *args, **kwargs):
  try:
    open_session_req = TCLIService.TOpenSessionReq(*args, **kwargs)
    session = hs2_client.OpenSession(open_session_req)
    yield session
  finally:
    if session.status.statusCode != TCLIService.TStatusCode.SUCCESS_STATUS:
      return
    close_session_req = TCLIService.TCloseSessionReq()
    close_session_req.sessionHandle = session.sessionHandle
    HS2TestSuite.check_response(hs2_client.CloseSession(close_session_req))


class TestHS2(HS2TestSuite):
  def test_open_session(self):
    """Check that a session can be opened"""
    open_session_req = TCLIService.TOpenSessionReq()
    with ScopedSession(self.hs2_client) as session:
      TestHS2.check_response(session)

  def test_open_session_query_options(self):
    """Check that OpenSession sets query options"""
    configuration = {'MAX_ERRORS': '45678', 'NUM_NODES': '1',
                     'MAX_NUM_RUNTIME_FILTERS': '333'}
    with ScopedSession(self.hs2_client, configuration=configuration) as session:
      TestHS2.check_response(session)
      for k, v in configuration.items():
        assert session.configuration[k] == v
    # simulate hive jdbc's action, see IMPALA-11992
    hiveconfs = dict([("set:hiveconf:" + k, v) for k, v in configuration.items()])
    with ScopedSession(self.hs2_client, configuration=hiveconfs) as sessions:
      TestHS2.check_response(sessions)
      for k, v in configuration.items():
        assert sessions.configuration[k] == v

  def get_session_options(self, setCmd):
    """Returns dictionary of query options."""
    execute_statement_resp = self.execute_statement(setCmd)

    fetch_results_req = TCLIService.TFetchResultsReq()
    fetch_results_req.operationHandle = execute_statement_resp.operationHandle
    fetch_results_req.maxRows = 1000
    fetch_results_resp = self.hs2_client.FetchResults(fetch_results_req)
    TestHS2.check_response(fetch_results_resp)

    # Close the query
    close_operation_req = TCLIService.TCloseOperationReq()
    close_operation_req.operationHandle = execute_statement_resp.operationHandle
    TestHS2.check_response(self.hs2_client.CloseOperation(close_operation_req))

    # Results are returned in a columnar way:
    cols = fetch_results_resp.results.columns
    assert len(cols) == 3
    vals = dict(zip(cols[0].stringVal.values, cols[1].stringVal.values))
    levels = dict(zip(cols[0].stringVal.values, cols[2].stringVal.values))
    return vals, levels

  @needs_session()
  def test_session_options_via_set(self):
    """
    Tests that session options are returned by a SET
    query and can be updated by a "SET k=v" query.
    """
    vals, levels = self.get_session_options("SET")

    # No default; should be empty string.
    assert vals["COMPRESSION_CODEC"] == ""
    # Has default of 0
    assert vals["SYNC_DDL"] == "0"

    # Set an option using SET
    self.execute_statement("SET COMPRESSION_CODEC=gzip")

    vals2, levels = self.get_session_options("SET")
    assert vals2["COMPRESSION_CODEC"] == "GZIP"
    assert levels["COMPRESSION_CODEC"] == "REGULAR"
    # Should be unchanged
    assert vals2["SYNC_DDL"] == "0"

    assert "MAX_ERRORS" in vals2
    assert levels["MAX_ERRORS"] == "ADVANCED"
    # Verify that 'DEVELOPMENT' options are not returned.
    assert "DEBUG_ACTION" not in vals2

    # Removed options are not returned.
    assert "MAX_IO_BUFFERS" not in vals2

  @needs_session()
  def test_session_option_levels_via_set_all(self):
    """
    Tests the level of session options returned by a SET ALL query except DEPRECATED as we
    currently do not have any of those left.
    """
    vals, levels = self.get_session_options("SET ALL")

    assert "COMPRESSION_CODEC" in vals
    assert "SYNC_DDL" in vals
    assert "MAX_ERRORS" in vals
    assert "DEBUG_ACTION" in vals
    assert levels["COMPRESSION_CODEC"] == "REGULAR"
    assert levels["SYNC_DDL"] == "REGULAR"
    assert levels["MAX_ERRORS"] == "ADVANCED"
    assert levels["DEBUG_ACTION"] == "DEVELOPMENT"
    # Removed options are returned by "SET ALL" for the benefit of impala-shell.
    assert levels["MAX_IO_BUFFERS"] == "REMOVED"

  @needs_session()
  def test_session_option_levels_via_unset_all(self):
    self.execute_statement("SET COMPRESSION_CODEC=gzip")
    self.execute_statement("SET SYNC_DDL=1")
    vals, levels = self.get_session_options("SET")
    assert vals["COMPRESSION_CODEC"] == "GZIP"
    assert vals["SYNC_DDL"] == "1"

    # Unset all query options
    self.execute_statement("UNSET ALL")
    vals2, levels = self.get_session_options("SET")

    # Reset to default value
    assert vals2["COMPRESSION_CODEC"] == ""
    assert vals2["SYNC_DDL"] == "0"

  @SkipIfDockerizedCluster.internal_hostname
  def test_open_session_http_addr(self):
    """Check that OpenSession returns the coordinator's http address."""
    with ScopedSession(self.hs2_client) as session:
      TestHS2.check_response(session)
      http_addr = session.configuration['http_addr']
      resp = urlopen("http://%s/queries?json" % http_addr)
      assert resp.msg == 'OK'
      queries_json = json.loads(resp.read())
      assert 'completed_queries' in queries_json
      assert 'in_flight_queries' in queries_json

  def test_open_session_unsupported_protocol(self):
    """Test that we get the right protocol version back if we ask for one larger than the
    server supports. This test will fail as we support newer version of HS2, and should be
    updated."""
    client_protocol = TCLIService.TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V7
    with ScopedSession(self.hs2_client, client_protocol=client_protocol) as session:
      TestHS2.check_response(session)
      assert session.serverProtocolVersion == \
          TCLIService.TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V6

  def test_open_session_empty_user(self):
    """Test that we get the expected errors back if either impala.doas.user is set but
    username is empty, or username is set but impala.doas.user is empty."""
    configuration = {"impala.doas.user": "do_as_user"}
    with ScopedSession(self.hs2_client, configuration=configuration) as session:
      TestHS2.check_response(session, TCLIService.TStatusCode.ERROR_STATUS,
          "Unable to delegate using empty proxy username.")

    username = "user"
    configuration = {"impala.doas.user": ""}
    with ScopedSession(
        self.hs2_client, username=username, configuration=configuration) as session:
      TestHS2.check_response(session, TCLIService.TStatusCode.ERROR_STATUS,
          "Unable to delegate using empty doAs username.")

  def test_close_session(self):
    """Test that an open session can be closed"""
    open_session_req = TCLIService.TOpenSessionReq()
    resp = self.hs2_client.OpenSession(open_session_req)
    TestHS2.check_response(resp)

    # Check that CloseSession validates session secret and acts as if the session didn't
    # exist.
    invalid_close_session_req = TCLIService.TCloseSessionReq()
    invalid_close_session_req.sessionHandle = create_session_handle_without_secret(
        resp.sessionHandle)
    TestHS2.check_invalid_session(self.hs2_client.CloseSession(invalid_close_session_req))

    close_session_req = TCLIService.TCloseSessionReq()
    close_session_req.sessionHandle = resp.sessionHandle
    TestHS2.check_response(self.hs2_client.CloseSession(close_session_req))

  def test_double_close_session(self):
    """Test that an already closed session cannot be closed a second time"""
    open_session_req = TCLIService.TOpenSessionReq()
    resp = self.hs2_client.OpenSession(open_session_req)
    TestHS2.check_response(resp)

    close_session_req = TCLIService.TCloseSessionReq()
    close_session_req.sessionHandle = resp.sessionHandle
    TestHS2.check_response(self.hs2_client.CloseSession(close_session_req))

    # Double close should be an error
    TestHS2.check_invalid_session(self.hs2_client.CloseSession(close_session_req))

  # This test verifies the number of open and expired sessions so avoid running
  # concurrently with other sessions.
  @pytest.mark.execute_serially
  def test_concurrent_session_mixed_idle_timeout(self):
    """Test for concurrent idle sessions' expiration with mixed timeout durations."""
    timeout_periods = [0, 5, 10]
    session_handles = []
    last_time_session_active = []

    for timeout in timeout_periods:
      open_session_req = TCLIService.TOpenSessionReq()
      open_session_req.configuration = {}
      open_session_req.configuration['idle_session_timeout'] = str(timeout)
      resp = self.hs2_client.OpenSession(open_session_req)
      TestHS2.check_response(resp)
      session_handles.append(resp.sessionHandle)

    num_open_sessions = self.impalad_test_service.get_metric_value(
        "impala-server.num-open-hiveserver2-sessions")
    num_expired_sessions = self.impalad_test_service.get_metric_value(
        "impala-server.num-sessions-expired")

    execute_statement_req = TCLIService.TExecuteStatementReq()
    execute_statement_req.statement = "SELECT 1+2"
    for session_handle in session_handles:
      execute_statement_req.sessionHandle = session_handle
      resp = self.hs2_client.ExecuteStatement(execute_statement_req)
      TestHS2.check_response(resp)
      last_time_session_active.append(time.time())

    assert num_open_sessions == self.impalad_test_service.get_metric_value(
        "impala-server.num-open-hiveserver2-sessions")
    assert num_expired_sessions == self.impalad_test_service.get_metric_value(
        "impala-server.num-sessions-expired")

    for timeout in timeout_periods:
      sleep_period = timeout + 1.5
      time.sleep(sleep_period)
      for i, session_handle in enumerate(session_handles):
        if session_handle is not None:
          execute_statement_req.sessionHandle = session_handle
          resp = self.hs2_client.ExecuteStatement(execute_statement_req)
          last_exec_statement_time = time.time()
          if timeout_periods[i] == 0 or \
             timeout_periods[i] > last_exec_statement_time - last_time_session_active[i]:
            TestHS2.check_response(resp)
            last_time_session_active[i] = last_exec_statement_time
          else:
            TestHS2.check_response(resp, TCLIService.TStatusCode.ERROR_STATUS)
            close_session_req = TCLIService.TCloseSessionReq()
            close_session_req.sessionHandle = session_handles[i]
            TestHS2.check_response(self.hs2_client.CloseSession(close_session_req))
            session_handles[i] = None
            num_open_sessions -= 1
            num_expired_sessions += 1
      assert num_open_sessions == self.impalad_test_service.get_metric_value(
          "impala-server.num-open-hiveserver2-sessions")
      assert num_expired_sessions == self.impalad_test_service.get_metric_value(
          "impala-server.num-sessions-expired")

    # Close the remaining sessions.
    for session_handle in session_handles:
      if session_handle is not None:
        close_session_req = TCLIService.TCloseSessionReq(session_handle)
        TestHS2.check_response(self.hs2_client.CloseSession(close_session_req))

  @needs_session()
  def test_get_operation_status(self):
    """Tests that GetOperationStatus returns a valid result for a running query"""
    statement = "SELECT COUNT(*) FROM functional.alltypes"
    execute_statement_resp = self.execute_statement(statement)

    get_operation_status_resp = \
        self.get_operation_status(execute_statement_resp.operationHandle)
    TestHS2.check_response(get_operation_status_resp)
    # If ExecuteStatement() has completed but the results haven't been fetched yet, the
    # query must have reached either PENDING or RUNNING or FINISHED.
    assert get_operation_status_resp.operationState in \
        [TCLIService.TOperationState.PENDING_STATE,
         TCLIService.TOperationState.RUNNING_STATE,
         TCLIService.TOperationState.FINISHED_STATE]

    fetch_results_req = TCLIService.TFetchResultsReq()
    fetch_results_req.operationHandle = execute_statement_resp.operationHandle
    fetch_results_req.maxRows = 100
    fetch_results_resp = self.hs2_client.FetchResults(fetch_results_req)

    get_operation_status_resp = \
        self.get_operation_status(execute_statement_resp.operationHandle)
    TestHS2.check_response(get_operation_status_resp)
    # After fetching the results, the query must be in state FINISHED.
    assert get_operation_status_resp.operationState == \
        TCLIService.TOperationState.FINISHED_STATE

    # Validate that the operation secret is checked.
    TestHS2.check_invalid_query(self.get_operation_status(
        create_op_handle_without_secret(execute_statement_resp.operationHandle)),
        expect_legacy_err=True)

    close_operation_req = TCLIService.TCloseOperationReq()
    close_operation_req.operationHandle = execute_statement_resp.operationHandle
    TestHS2.check_response(self.hs2_client.CloseOperation(close_operation_req))

    get_operation_status_resp = \
        self.get_operation_status(execute_statement_resp.operationHandle)
    # GetOperationState should return 'Invalid or unknown query handle' if the query has
    # been closed.
    TestHS2.check_response(get_operation_status_resp, \
        TCLIService.TStatusCode.ERROR_STATUS)

  @needs_session(conf_overlay={"abort_on_error": "1"})
  def test_get_operation_status_error(self):
    """Tests that GetOperationStatus returns a valid result for a query that encountered
    an error"""
    statement = "SELECT * FROM functional.alltypeserror"
    execute_statement_resp = self.execute_statement(statement)

    get_operation_status_resp = self.wait_for_operation_state( \
        execute_statement_resp.operationHandle, TCLIService.TOperationState.ERROR_STATE)

    # Check that an error message and sql state have been set.
    assert get_operation_status_resp.errorMessage is not None and \
        get_operation_status_resp.errorMessage is not ""
    assert get_operation_status_resp.sqlState == SQLSTATE_GENERAL_ERROR

  @needs_session()
  def test_malformed_get_operation_status(self):
    """Tests that a short guid / secret returns an error (regression would be to crash
    impalad)"""
    operation_handle = TCLIService.TOperationHandle()
    operation_handle.operationId = TCLIService.THandleIdentifier()
    operation_handle.operationId.guid = "short"
    operation_handle.operationId.secret = "short_secret"
    assert len(operation_handle.operationId.guid) != 16
    assert len(operation_handle.operationId.secret) != 16
    operation_handle.operationType = TCLIService.TOperationType.EXECUTE_STATEMENT
    operation_handle.hasResultSet = False

    get_operation_status_resp = self.get_operation_status(operation_handle)
    TestHS2.check_response(get_operation_status_resp, \
        TCLIService.TStatusCode.ERROR_STATUS)

    err_msg = "(guid size: %d, expected 16, secret size: %d, expected 16)" \
        % (len(operation_handle.operationId.guid),
           len(operation_handle.operationId.secret))
    assert err_msg in get_operation_status_resp.status.errorMessage

  @needs_session()
  def test_invalid_query_handle(self):
    operation_handle = TCLIService.TOperationHandle()
    operation_handle.operationId = TCLIService.THandleIdentifier()
    operation_handle.operationId.guid = "\x01\x23\x45\x67\x89\xab\xcd\xef76543210"
    operation_handle.operationId.secret = "PasswordIsPencil"
    operation_handle.operationType = TCLIService.TOperationType.EXECUTE_STATEMENT
    operation_handle.hasResultSet = False

    get_operation_status_resp = self.get_operation_status(operation_handle)
    TestHS2.check_response(get_operation_status_resp, \
        TCLIService.TStatusCode.ERROR_STATUS)

    err_msg = "Invalid or unknown query handle: efcdab8967452301:3031323334353637"
    assert err_msg in get_operation_status_resp.status.errorMessage

    get_result_set_metadata_req = TCLIService.TGetResultSetMetadataReq()
    get_result_set_metadata_req.operationHandle = operation_handle
    get_result_set_metadata_resp = \
        self.hs2_client.GetResultSetMetadata(get_result_set_metadata_req)
    TestHS2.check_response(get_result_set_metadata_resp, \
        TCLIService.TStatusCode.ERROR_STATUS)

    err_msg = "Invalid or unknown query handle: efcdab8967452301:3031323334353637"
    assert err_msg in get_result_set_metadata_resp.status.errorMessage

  @pytest.mark.execute_serially
  def test_multiple_sessions(self):
    """Test that multiple sessions on the same socket connection are allowed"""
    num_sessions = self.impalad_test_service.get_metric_value(
        "impala-server.num-open-hiveserver2-sessions")
    session_ids = []
    for _ in range(5):
      open_session_req = TCLIService.TOpenSessionReq()
      resp = self.hs2_client.OpenSession(open_session_req)
      TestHS2.check_response(resp)
      # Check that all sessions get different IDs
      assert resp.sessionHandle not in session_ids
      session_ids.append(resp.sessionHandle)

    self.impalad_test_service.wait_for_metric_value(
        "impala-server.num-open-hiveserver2-sessions", num_sessions + 5)

    for session_id in session_ids:
      close_session_req = TCLIService.TCloseSessionReq(session_id)
      resp = self.hs2_client.CloseSession(close_session_req)
      TestHS2.check_response(resp)

    self.impalad_test_service.wait_for_metric_value(
        "impala-server.num-open-hiveserver2-sessions", num_sessions)

  @needs_session()
  def test_get_info(self):
    # Negative test for invalid session secret.
    invalid_req = TCLIService.TGetInfoReq(create_session_handle_without_secret(
        self.session_handle), TCLIService.TGetInfoType.CLI_DBMS_NAME)
    TestHS2.check_invalid_session(self.hs2_client.GetInfo(invalid_req))

    # TODO: it would be useful to add positive tests for GetInfo().

  @needs_session()
  def test_get_schemas(self):
    get_schemas_req = TCLIService.TGetSchemasReq()
    get_schemas_req.sessionHandle = self.session_handle
    get_schemas_resp = self.hs2_client.GetSchemas(get_schemas_req)
    TestHS2.check_response(get_schemas_resp)
    fetch_results_req = TCLIService.TFetchResultsReq()
    fetch_results_req.operationHandle = get_schemas_resp.operationHandle
    fetch_results_req.maxRows = 100
    fetch_results_resp = self.hs2_client.FetchResults(fetch_results_req)
    TestHS2.check_response(fetch_results_resp)
    query_id = operation_id_to_query_id(get_schemas_resp.operationHandle.operationId)
    profile_page = self.impalad_test_service.read_query_profile_page(query_id)

    # Test fix for IMPALA-619
    assert "Sql Statement: GET_SCHEMAS" in profile_page
    assert "Query Type: DDL" in profile_page

    # Test that session secret is validated by this API.
    get_schemas_req.sessionHandle = create_session_handle_without_secret(
        self.session_handle)
    TestHS2.check_invalid_session(self.hs2_client.GetSchemas(get_schemas_req))

  @needs_session_cluster_properties()
  def test_get_schemas_on_transient_db(self, cluster_properties, unique_database):
    # Use a new db name
    unique_database += "_tmp"
    stop = False

    def create_drop_db():
      while not stop:
        self.execute_query("create database if not exists " + unique_database)
        time.sleep(0.1)
        if stop:
          break
        self.execute_query("drop database " + unique_database)
    t = threading.Thread(target=create_drop_db)
    t.start()

    try:
      get_schemas_req = TCLIService.TGetSchemasReq(self.session_handle)
      for i in range(100):
        get_schemas_resp = self.hs2_client.GetSchemas(get_schemas_req)
        TestHS2.check_response(get_schemas_resp)
        time.sleep(0.2)
    finally:
      stop = True
      t.join()

  @needs_session_cluster_properties()
  def test_get_tables_on_transient_db(self, cluster_properties, unique_database):
    # Use a new db name
    unique_database += "_tmp"
    stop = False

    def create_drop_db():
      while not stop:
        self.execute_query("create database if not exists " + unique_database)
        time.sleep(0.1)
        if stop:
          break
        self.execute_query("drop database " + unique_database)
    t = threading.Thread(target=create_drop_db)
    t.start()

    try:
      # Use empty 'schemaName' to get tables in all dbs.
      get_tables_req = TCLIService.TGetTablesReq(self.session_handle)
      for i in range(100):
        get_tables_resp = self.hs2_client.GetTables(get_tables_req)
        TestHS2.check_response(get_tables_resp)
        time.sleep(0.2)
    finally:
      stop = True
      t.join()

  @needs_session_cluster_properties()
  def test_get_tables(self, cluster_properties, unique_database):
    """Basic test for the GetTables() HS2 method. Needs to execute serially because
    the test depends on controlling whether a table is loaded or not and other
    concurrent tests loading or invalidating tables could interfere with it."""
    table = "__hs2_table_comment_test"
    view = "__hs2_view_comment_test"
    self.execute_query("use {0}".format(unique_database))
    self.execute_query("drop table if exists {0}".format(table))
    self.execute_query("drop view if exists {0}".format(view))
    self.execute_query("""
        create table {0} (a int comment 'column comment')
        comment 'table comment'""".format(table))
    self.execute_query("""
        create view {0} comment 'view comment' as select * from {1}
        """.format(view, table))
    try:
      req = TCLIService.TGetTablesReq()
      req.sessionHandle = self.session_handle
      req.schemaName = unique_database

      # Execute the request twice, the first time with the table/view unloaded and the
      # second with them loaded.
      self.execute_query("invalidate metadata {0}".format(table))
      self.execute_query("invalidate metadata {0}".format(view))
      for i in range(2):
        get_tables_resp = self.hs2_client.GetTables(req)
        TestHS2.check_response(get_tables_resp)

        fetch_results_resp = self._fetch_results(get_tables_resp.operationHandle, 100)
        results = fetch_results_resp.results
        # The returned results should only contain metadata for the table and the view.
        # Note that results are in columnar format so we get #rows by the length of the
        # first column.
        assert len(results.columns[0].stringVal.values) == 2
        for row_idx in range(2):
          table_cat = results.columns[0].stringVal.values[row_idx]
          table_schema = results.columns[1].stringVal.values[row_idx]
          table_name = results.columns[2].stringVal.values[row_idx]
          table_type = results.columns[3].stringVal.values[row_idx]
          table_remarks = results.columns[4].stringVal.values[row_idx]
          assert table_cat == ''
          assert table_schema == unique_database
          if i == 0:
            # In the first iteration the table and view are unloaded, so their types are
            # in the default value (TABLE) and their comments are empty.
            assert table_name in (table, view)
            assert table_type == "TABLE"
            assert table_remarks == ""
          else:
            # In the second iteration the table and view are loaded. They should be shown
            # with correct types and comments. Verify types and comments based on table
            # names so we don't care the order in which the table/view are returned.
            if table_name == table:
              assert table_type == "TABLE"
              assert table_remarks == "table comment"
            else:
              assert table_name == view
              assert table_type == "VIEW"
              assert table_remarks == "view comment"
        # Ensure the table and view are loaded for the second iteration.
        self.execute_query("describe {0}".format(table))
        self.execute_query("describe {0}".format(view))

      # Test that session secret is validated by this API.
      invalid_req = TCLIService.TGetTablesReq()
      invalid_req.sessionHandle = create_session_handle_without_secret(
          self.session_handle)
      invalid_req.schemaName = unique_database
      invalid_req.tableName = table
      TestHS2.check_invalid_session(self.hs2_client.GetTables(invalid_req))
    finally:
      self.execute_query("drop table {0}".format(table))

  @needs_session()
  def test_get_primary_keys(self):
    req = TCLIService.TGetPrimaryKeysReq()
    req.sessionHandle = self.session_handle
    req.schemaName = 'functional'
    req.tableName = 'parent_table'

    get_primary_keys_resp = self.hs2_client.GetPrimaryKeys(req)
    TestHS2.check_response(get_primary_keys_resp)

    fetch_results_resp = self._fetch_results(
        get_primary_keys_resp.operationHandle, 100)

    results = fetch_results_resp.results
    for i in range(2):
      table_cat = results.columns[0].stringVal.values[i]
      table_schema = results.columns[1].stringVal.values[i]
      table_name = results.columns[2].stringVal.values[i]
      pk_name = results.columns[5].stringVal.values[i]
      assert table_cat == ''
      assert table_schema == 'functional'
      assert table_name == 'parent_table'
      assert len(pk_name) > 0

    # Assert PK column names.
    assert results.columns[3].stringVal.values[0] == 'id'
    assert results.columns[3].stringVal.values[1] == 'year'

  @needs_session()
  def test_get_cross_reference(self):
    req = TCLIService.TGetCrossReferenceReq()
    req.sessionHandle = self.session_handle
    req.parentSchemaName = "functional"
    req.foreignSchemaName = "functional"
    req.parentTableName = "parent_table"
    req.foreignTableName = "child_table"

    get_foreign_keys_resp = self.hs2_client.GetCrossReference(req)
    TestHS2.check_response(get_foreign_keys_resp)

    fetch_results_resp = self._fetch_results(
        get_foreign_keys_resp.operationHandle, 100)

    results = fetch_results_resp.results

    for i in range(2):
      parent_table_cat = results.columns[0].stringVal.values[i]
      parent_table_schema = results.columns[1].stringVal.values[i]
      parent_table_name = results.columns[2].stringVal.values[i]
      foreign_table_cat = results.columns[4].stringVal.values[i]
      foreign_table_schema = results.columns[5].stringVal.values[i]
      foreign_table_name = results.columns[6].stringVal.values[i]
      assert parent_table_cat == ''
      assert parent_table_schema == 'functional'
      assert parent_table_name == 'parent_table'
      assert foreign_table_cat == ''
      assert foreign_table_schema == 'functional'
      assert foreign_table_name == 'child_table'

    # Assert PK column names.
    assert results.columns[3].stringVal.values[0] == 'id'
    assert results.columns[3].stringVal.values[1] == 'year'

    # Assert FK column names.
    assert results.columns[7].stringVal.values[0] == 'id'
    assert results.columns[7].stringVal.values[1] == 'year'

    # Get all foreign keys from the FK side by not setting pkTableSchema
    # and pkTable name in the request.
    req = TCLIService.TGetCrossReferenceReq()
    req.sessionHandle = self.session_handle
    req.foreignSchemaName = 'functional'
    req.foreignTableName = 'child_table'

    get_foreign_keys_resp = self.hs2_client.GetCrossReference(req)
    TestHS2.check_response(get_foreign_keys_resp)

    fetch_results_resp = self._fetch_results(
        get_foreign_keys_resp.operationHandle, 100)

    results = fetch_results_resp.results

    for i in range(3):
      parent_table_cat = results.columns[0].stringVal.values[i]
      parent_table_schema = results.columns[1].stringVal.values[i]
      foreign_table_cat = results.columns[4].stringVal.values[i]
      foreign_table_schema = results.columns[5].stringVal.values[i]
      foreign_table_name = results.columns[6].stringVal.values[i]
      assert parent_table_cat == ''
      assert parent_table_schema == 'functional'
      assert foreign_table_cat == ''
      assert foreign_table_schema == 'functional'
      assert foreign_table_name == 'child_table'

    # First two FKs have 'parent_table' as PK table.
    pk_table_names = ['parent_table', 'parent_table', 'parent_table_2']
    for i in range(len(pk_table_names)):
      parent_table_name = results.columns[2].stringVal.values[i]
      parent_table_name == pk_table_names[i]

      # Assert PK column names.
    assert results.columns[3].stringVal.values[0] == 'id'
    assert results.columns[3].stringVal.values[1] == 'year'
    assert results.columns[3].stringVal.values[2] == 'a'

    # Assert FK column names.
    assert results.columns[7].stringVal.values[0] == 'id'
    assert results.columns[7].stringVal.values[1] == 'year'
    assert results.columns[7].stringVal.values[2] == 'a'

  @needs_session(conf_overlay={"idle_session_timeout": "5"})
  def test_get_operation_status_session_timeout(self):
    """Regression test for IMPALA-4488: GetOperationStatus() would not keep a session
    alive"""
    # Choose a long-running query so that it can't finish before the session timeout.
    statement = """select * from functional.alltypes a
    join functional.alltypes b join functional.alltypes c"""
    execute_statement_resp = self.execute_statement(statement)

    now = time.time()
    # Loop until the session would be timed-out if IMPALA-4488 had not been fixed.
    while time.time() - now < 10:
      get_operation_status_resp = \
        self.get_operation_status(execute_statement_resp.operationHandle)
      # Will fail if session has timed out.
      TestHS2.check_response(get_operation_status_resp)
      time.sleep(0.1)

  def get_log(self, query_stmt):
    execute_statement_resp = self.execute_statement(query_stmt)

    # Fetch results to make sure errors are generated. Errors are only guaranteed to be
    # seen by the coordinator after FetchResults() returns eos.
    has_more_results = True
    while has_more_results:
      fetch_results_req = TCLIService.TFetchResultsReq()
      fetch_results_req.operationHandle = execute_statement_resp.operationHandle
      fetch_results_req.maxRows = 100
      fetch_results_resp = self.hs2_client.FetchResults(fetch_results_req)
      TestHS2.check_response(fetch_results_resp)
      has_more_results = fetch_results_resp.hasMoreRows

    # Test that secret is validated.
    invalid_get_log_req = TCLIService.TGetLogReq()
    invalid_get_log_req.operationHandle = create_op_handle_without_secret(
        execute_statement_resp.operationHandle)
    TestHS2.check_invalid_query(self.hs2_client.GetLog(invalid_get_log_req),
        expect_legacy_err=True)

    get_log_req = TCLIService.TGetLogReq()
    get_log_req.operationHandle = execute_statement_resp.operationHandle
    get_log_resp = self.hs2_client.GetLog(get_log_req)
    TestHS2.check_response(get_log_resp)
    return get_log_resp.log

  @needs_session()
  def test_get_log(self):
    # Test query that generates BE warnings
    log = self.get_log("select * from functional.alltypeserror")
    assert "Error converting column" in log

    log = self.get_log("select base64decode('foo')")
    assert "Invalid base64 string; input length is 3, which is not a multiple of 4" in log

  @needs_session()
  def test_get_exec_summary(self):
    statement = "SELECT COUNT(1) FROM functional.alltypes"
    execute_statement_resp = self.execute_statement(statement)

    exec_summary_req = ImpalaHiveServer2Service.TGetExecSummaryReq()
    exec_summary_req.operationHandle = execute_statement_resp.operationHandle
    exec_summary_req.sessionHandle = self.session_handle
    exec_summary_resp = self.hs2_client.GetExecSummary(exec_summary_req)

    # Test getting the summary while query is running. We can't verify anything
    # about the summary (depends how much progress query has made) but the call
    # should work.
    TestHS2.check_response(exec_summary_resp)

    # Wait for query to start running so we can get a non-empty ExecSummary.
    self.wait_for_admission_control(execute_statement_resp.operationHandle)
    exec_summary_resp = self.hs2_client.GetExecSummary(exec_summary_req)
    TestHS2.check_response(exec_summary_resp)
    assert len(exec_summary_resp.summary.nodes) > 0

    # Test that session secret is validated. Note that operation secret does not need to
    # be validated in addition if the session secret is valid and the operation belongs
    # to the session, because the user has full access to the session.
    TestHS2.check_invalid_session(self.hs2_client.GetExecSummary(
      ImpalaHiveServer2Service.TGetExecSummaryReq(execute_statement_resp.operationHandle,
        create_session_handle_without_secret(self.session_handle))))

    # Attempt to access query with different user should fail.
    evil_user = getuser() + "_evil_twin"
    with ScopedSession(self.hs2_client, username=evil_user) as session:
      session_handle2 = session.sessionHandle
      TestHS2.check_profile_access_denied(self.hs2_client.GetExecSummary(
        ImpalaHiveServer2Service.TGetExecSummaryReq(
          execute_statement_resp.operationHandle, session_handle2)), user=evil_user)

      # Now close the query and verify the exec summary is available.
      close_operation_req = TCLIService.TCloseOperationReq()
      close_operation_req.operationHandle = execute_statement_resp.operationHandle
      TestHS2.check_response(self.hs2_client.CloseOperation(close_operation_req))

      # Attempt to access query with different user from log should fail.
      TestHS2.check_profile_access_denied(self.hs2_client.GetRuntimeProfile(
        ImpalaHiveServer2Service.TGetRuntimeProfileReq(
          execute_statement_resp.operationHandle, session_handle2)),
        user=evil_user)

    exec_summary_resp = self.hs2_client.GetExecSummary(exec_summary_req)
    TestHS2.check_response(exec_summary_resp)
    assert len(exec_summary_resp.summary.nodes) > 0

  @needs_session()
  def test_get_backend_config(self):
    get_backend_config_req = ImpalaHiveServer2Service.TGetBackendConfigReq()
    get_backend_config_req.sessionHandle = self.session_handle
    get_backend_config_resp = self.hs2_client.GetBackendConfig(get_backend_config_req)
    TestHS2.check_response(get_backend_config_resp,
        TCLIService.TStatusCode.ERROR_STATUS, "Unsupported operation")

  @needs_session()
  def test_get_executor_membership(self):
    get_executor_membership_req = ImpalaHiveServer2Service.TGetExecutorMembershipReq()
    get_executor_membership_req.sessionHandle = self.session_handle
    get_executor_membership_resp = self.hs2_client.GetExecutorMembership(
      get_executor_membership_req)
    TestHS2.check_response(get_executor_membership_resp,
        TCLIService.TStatusCode.ERROR_STATUS, "Unsupported operation")

  @needs_session()
  def test_init_query_context(self):
    init_query_context_resp = self.hs2_client.InitQueryContext()
    TestHS2.check_response(init_query_context_resp,
        TCLIService.TStatusCode.ERROR_STATUS, "Unsupported operation")

  @needs_session()
  def test_get_profile(self):
    statement = "SELECT COUNT(2) FROM functional.alltypes"
    execute_statement_resp = self.execute_statement(statement)

    get_profile_req = ImpalaHiveServer2Service.TGetRuntimeProfileReq()
    get_profile_req.operationHandle = execute_statement_resp.operationHandle
    get_profile_req.sessionHandle = self.session_handle
    get_profile_resp = self.hs2_client.GetRuntimeProfile(get_profile_req)
    TestHS2.check_response(get_profile_resp)
    assert statement in get_profile_resp.profile
    # If ExecuteStatement() has completed but the results haven't been fetched yet, the
    # query must have reached either COMPILED or RUNNING or FINISHED.
    assert "Query State: COMPILED" in get_profile_resp.profile or \
        "Query State: RUNNING" in get_profile_resp.profile or \
        "Query State: FINISHED" in get_profile_resp.profile, get_profile_resp.profile

    fetch_results_req = TCLIService.TFetchResultsReq()
    fetch_results_req.operationHandle = execute_statement_resp.operationHandle
    fetch_results_req.maxRows = 100
    fetch_results_resp = self.hs2_client.FetchResults(fetch_results_req)

    get_profile_resp = self.hs2_client.GetRuntimeProfile(get_profile_req)
    TestHS2.check_response(get_profile_resp)
    assert statement in get_profile_resp.profile
    # After fetching the results, we must be in state FINISHED.
    assert "Query State: FINISHED" in get_profile_resp.profile, get_profile_resp.profile

    # Test that session secret is validated. Note that operation secret does not need to
    # be validated in addition if the session secret is valid and the operation belongs
    # to the session, because the user has full access to the session.
    TestHS2.check_invalid_session(self.hs2_client.GetRuntimeProfile(
      ImpalaHiveServer2Service.TGetRuntimeProfileReq(
        execute_statement_resp.operationHandle,
        create_session_handle_without_secret(self.session_handle))))

    # Attempt to access query with different user should fail.
    evil_user = getuser() + "_evil_twin"
    with ScopedSession(self.hs2_client, username=evil_user) as session:
      session_handle2 = session.sessionHandle
      TestHS2.check_profile_access_denied(self.hs2_client.GetRuntimeProfile(
        ImpalaHiveServer2Service.TGetRuntimeProfileReq(
          execute_statement_resp.operationHandle, session_handle2)),
          user=evil_user)

      close_operation_req = TCLIService.TCloseOperationReq()
      close_operation_req.operationHandle = execute_statement_resp.operationHandle
      TestHS2.check_response(self.hs2_client.CloseOperation(close_operation_req))

      # Attempt to access query with different user from log should fail.
      TestHS2.check_profile_access_denied(self.hs2_client.GetRuntimeProfile(
        ImpalaHiveServer2Service.TGetRuntimeProfileReq(
          execute_statement_resp.operationHandle, session_handle2)),
          user=evil_user)

    get_profile_resp = self.hs2_client.GetRuntimeProfile(get_profile_req)
    TestHS2.check_response(get_profile_resp)
    assert statement in get_profile_resp.profile
    assert "Query State: FINISHED" in get_profile_resp.profile, get_profile_resp.profile

    # Check get JSON format profile
    get_profile_req.format = 3  # json format
    get_profile_resp = self.hs2_client.GetRuntimeProfile(get_profile_req)
    TestHS2.check_response(get_profile_resp)

    try:
      json_res = json.loads(get_profile_resp.profile)
    except ValueError:
      assert False, "Download JSON format query profile cannot be parsed." \
          "Response text:{0}".format(get_profile_resp.profile)

    # The query statement should exist in json info_strings
    if ("child_profiles" not in json_res["contents"]) or \
        ("info_strings" not in json_res["contents"]["child_profiles"][0]):
      assert False, "JSON content is invalid. Content: {0}"\
        .format(get_profile_resp.profile)

    for info_string in json_res["contents"]["child_profiles"][0]["info_strings"]:
      if info_string["key"] == "Sql Statement":
        assert statement in info_string["value"], \
          "JSON content is invalid. Content: {0}".format(get_profile_resp.profile)
        break

  @needs_session()
  def test_concurrent_unregister(self):
    """Test that concurrently unregistering a query from multiple clients is safe and
    that the profile can be fetched during the process."""
    # Attach a UUID to the query text to make it easy to identify in web UI.
    query_uuid = str(uuid.uuid4())
    statement = "/*{0}*/ SELECT COUNT(2) FROM functional.alltypes".format(query_uuid)
    execute_statement_resp = self.execute_statement(statement)
    op_handle = execute_statement_resp.operationHandle

    fetch_results_req = TCLIService.TFetchResultsReq()
    fetch_results_req.operationHandle = op_handle
    fetch_results_req.maxRows = 100
    fetch_results_resp = self.hs2_client.FetchResults(fetch_results_req)
    TestHS2.check_response(fetch_results_resp)

    # Create a profile fetch thread and multiple unregister threads.
    NUM_UNREGISTER_THREADS = 10
    threads = []
    profile_fetch_exception = [None]
    unreg_exceptions = [None] * NUM_UNREGISTER_THREADS
    sockets = []
    try:
      # Start a profile fetch thread first that will fetch the profile
      # as the query is unregistered.
      threads.append(threading.Thread(target=self._fetch_profile_loop,
        args=(profile_fetch_exception, query_uuid, op_handle)))

      # Start threads that will race to unregister the query.
      for i in range(NUM_UNREGISTER_THREADS):
        socket, client = self._open_hs2_connection()
        sockets.append(socket)
        threads.append(threading.Thread(target=self._unregister_query,
            args=(i, unreg_exceptions, client, op_handle)))
      for thread in threads:
        thread.start()
      for thread in threads:
        thread.join()
    finally:
      for socket in sockets:
        socket.close()

    if profile_fetch_exception[0] is not None:
      raise profile_fetch_exception[0]

    # Validate the exceptions and ensure only one thread successfully unregistered
    # the query.
    num_successful = 0
    for exception in unreg_exceptions:
      if exception is None:
        num_successful += 1
      elif "Invalid or unknown query handle" not in str(exception):
        raise exception
    assert num_successful == 1, "Only one client should have been able to unregister"

  def _fetch_profile_loop(self, exception_array, query_uuid, op_handle):
    try:
      # This thread will keep fetching the profile to make sure that the
      # ClientRequestState object can be continually accessed during unregistration.
      get_profile_req = ImpalaHiveServer2Service.TGetRuntimeProfileReq()
      get_profile_req.operationHandle = op_handle
      get_profile_req.sessionHandle = self.session_handle

      def find_query(array):
        """Find the query for this test in a JSON array returned from web UI."""
        return [q for q in array if query_uuid in q['stmt']]
      # Loop until the query has been unregistered and moved out of in-flight
      # queries.
      registered = True
      while registered:
        get_profile_resp = self.hs2_client.GetRuntimeProfile(get_profile_req)
        TestHS2.check_response(get_profile_resp)
        if "Unregister query" in get_profile_resp.profile:
          json = self.impalad_test_service.get_queries_json()
          inflight_query = find_query(json['in_flight_queries'])
          completed_query = find_query(json['completed_queries'])
          # Query should only be in one list.
          assert len(inflight_query) + len(completed_query) == 1
          if completed_query:
            registered = False
    except BaseException as e:
      exception_array[0] = e

  def _unregister_query(self, thread_num, exceptions, client, op_handle):
    # Add some delay/jitter so that unregisters come in at different times.
    time.sleep(0.01 + 0.005 * random.random())
    try:
      close_operation_req = TCLIService.TCloseOperationReq()
      close_operation_req.operationHandle = op_handle
      TestHS2.check_response(client.CloseOperation(close_operation_req))
    except BaseException as e:
      exceptions[thread_num] = e

  @needs_session(conf_overlay={"use:database": "functional"})
  def test_change_default_database(self):
    statement = "SELECT 1 FROM alltypes LIMIT 1"
    # Will fail if there's no table called 'alltypes' in the database
    self.execute_statement(statement)

  @needs_session(conf_overlay={"use:database": "FUNCTIONAL"})
  def test_change_default_database_case_insensitive(self):
    statement = "SELECT 1 FROM alltypes LIMIT 1"
    # Will fail if there's no table called 'alltypes' in the database
    self.execute_statement(statement)

  @needs_session(conf_overlay={"use:database": "doesnt-exist"})
  def test_bad_default_database(self):
    execute_statement_req = TCLIService.TExecuteStatementReq()
    execute_statement_req.sessionHandle = self.session_handle
    execute_statement_req.statement = "SELECT 1 FROM alltypes LIMIT 1"
    execute_statement_resp = self.hs2_client.ExecuteStatement(execute_statement_req)
    TestHS2.check_response(execute_statement_resp, TCLIService.TStatusCode.ERROR_STATUS)

  @needs_session()
  def test_get_type_info(self):
    get_type_info_req = TCLIService.TGetTypeInfoReq()
    get_type_info_req.sessionHandle = self.session_handle
    get_type_info_resp = self.hs2_client.GetTypeInfo(get_type_info_req)
    TestHS2.check_response(get_type_info_resp)
    fetch_results_req = TCLIService.TFetchResultsReq()
    fetch_results_req.operationHandle = get_type_info_resp.operationHandle
    fetch_results_req.maxRows = 100
    fetch_results_resp = self.hs2_client.FetchResults(fetch_results_req)
    TestHS2.check_response(fetch_results_resp)
    results = fetch_results_resp.results
    types = ['BOOLEAN', 'TINYINT', 'SMALLINT', 'INT', 'BIGINT', 'FLOAT', 'DOUBLE', 'DATE',
             'TIMESTAMP', 'STRING', 'VARCHAR', 'DECIMAL', 'CHAR', 'ARRAY', 'MAP',
             'STRUCT', 'BINARY']
    assert self.get_num_rows(results) == len(types)
    # Validate that each type description (result row) has the required 18 fields as
    # described in the DatabaseMetaData.getTypeInfo() documentation.
    assert len(results.columns) == 18
    typed_col = getattr(results.columns[0], 'stringVal')
    for colType in types:
      assert typed_col.values.count(colType) == 1

    # Test that session secret is validated by this API.
    invalid_req = TCLIService.TGetTypeInfoReq(
        create_session_handle_without_secret(self.session_handle))
    TestHS2.check_invalid_session(self.hs2_client.GetTypeInfo(invalid_req))

  def _fetch_results(self, operation_handle, max_rows):
    """Fetch results from 'operation_handle' with up to 'max_rows' rows using
    self.hs2_client, returning the TFetchResultsResp object."""
    fetch_results_req = TCLIService.TFetchResultsReq()
    fetch_results_req.operationHandle = operation_handle
    fetch_results_req.maxRows = max_rows
    fetch_results_resp = self.hs2_client.FetchResults(fetch_results_req)
    TestHS2.check_response(fetch_results_resp)
    return fetch_results_resp

  def test_close_connection(self):
    """Tests that an hs2 session remains valid even after the connection is dropped."""
    open_session_req = TCLIService.TOpenSessionReq()
    open_session_resp = self.hs2_client.OpenSession(open_session_req)
    TestHS2.check_response(open_session_resp)
    self.session_handle = open_session_resp.sessionHandle
    # Run a query, which should succeed.
    self.execute_statement("select 1")

    # Reset the connection.
    self.teardown()
    self.setup()

    # Run another query with the same session handle. It should succeed even though it's
    # on a new connection, since disconnected_session_timeout (default of 1 hour) will not
    # have been hit.
    self.execute_statement("select 2")

    # Close the session.
    close_session_req = TCLIService.TCloseSessionReq()
    close_session_req.sessionHandle = self.session_handle
    TestHS2.check_response(self.hs2_client.CloseSession(close_session_req))

    # Run another query, which should fail since the session is closed.
    self.execute_statement("select 3", expected_error_prefix="Invalid session id",
        expected_status_code=TCLIService.TStatusCode.ERROR_STATUS)
