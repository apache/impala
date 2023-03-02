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

from __future__ import absolute_import, division, print_function
import pytest
import re

from time import sleep
from time import time
from tests.common.errors import Timeout
from tests.hs2.hs2_test_suite import (HS2TestSuite, needs_session,
    create_op_handle_without_secret)
from TCLIService import TCLIService, constants
from TCLIService.ttypes import TTypeId


# Simple test to make sure all the HS2 types are supported for both the row and
# column-oriented versions of the HS2 protocol.
class TestFetch(HS2TestSuite):
  def __verify_primitive_type(self, expected_type, hs2_type):
    assert hs2_type.typeDesc.types[0].primitiveEntry.type == expected_type

  def __verify_char_max_len(self, t, max_len):
    l = t.typeDesc.types[0].primitiveEntry.typeQualifiers.qualifiers\
      [constants.CHARACTER_MAXIMUM_LENGTH]
    assert l.i32Value == max_len

  def __verify_decimal_precision_scale(self, hs2_type, precision, scale):
    p = hs2_type.typeDesc.types[0].primitiveEntry.typeQualifiers.qualifiers\
      [constants.PRECISION]
    s = hs2_type.typeDesc.types[0].primitiveEntry.typeQualifiers.qualifiers\
      [constants.SCALE]
    assert p.i32Value == precision
    assert s.i32Value == scale

  def __fetch_result_column_types(self, query, expected_row_count, execute_statement_req):
    """ Fetches the results for 'query' and return the response and the
    array of column types."""
    execute_statement_req.statement = query
    execute_statement_resp = self.hs2_client.ExecuteStatement(execute_statement_req)
    HS2TestSuite.check_response(execute_statement_resp)
    results = self.fetch_at_most(execute_statement_resp.operationHandle,
                               TCLIService.TFetchOrientation.FETCH_NEXT, 1, 1)
    assert len(results.results.rows) == expected_row_count
    metadata_resp = self.result_metadata(execute_statement_resp.operationHandle)
    return execute_statement_resp, metadata_resp.schema.columns

  @needs_session(TCLIService.TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V1)
  def test_result_metadata_v1(self):
    execute_statement_req = TCLIService.TExecuteStatementReq()
    execute_statement_req.sessionHandle = self.session_handle

    # Verify all primitive types in the alltypes table.
    execute_statement_resp, column_types = self.__fetch_result_column_types(
        "SELECT * FROM functional.alltypessmall ORDER BY id LIMIT 1", 1,
        execute_statement_req)
    assert len(column_types) == 13
    self.__verify_primitive_type(TTypeId.INT_TYPE, column_types[0])
    self.__verify_primitive_type(TTypeId.BOOLEAN_TYPE, column_types[1])
    self.__verify_primitive_type(TTypeId.TINYINT_TYPE, column_types[2])
    self.__verify_primitive_type(TTypeId.SMALLINT_TYPE, column_types[3])
    self.__verify_primitive_type(TTypeId.INT_TYPE, column_types[4])
    self.__verify_primitive_type(TTypeId.BIGINT_TYPE, column_types[5])
    self.__verify_primitive_type(TTypeId.FLOAT_TYPE, column_types[6])
    self.__verify_primitive_type(TTypeId.DOUBLE_TYPE, column_types[7])
    self.__verify_primitive_type(TTypeId.STRING_TYPE, column_types[8])
    self.__verify_primitive_type(TTypeId.STRING_TYPE, column_types[9])
    self.__verify_primitive_type(TTypeId.TIMESTAMP_TYPE, column_types[10])
    self.__verify_primitive_type(TTypeId.INT_TYPE, column_types[11])
    self.__verify_primitive_type(TTypeId.INT_TYPE, column_types[12])
    self.close(execute_statement_resp.operationHandle)

    # Verify the result metadata for the DECIMAL type.
    execute_statement_resp, column_types = self.__fetch_result_column_types(
        "SELECT d1,d5 FROM functional.decimal_tbl ORDER BY d1 LIMIT 1", 1,
        execute_statement_req)
    assert len(column_types) == 2
    self.__verify_primitive_type(TTypeId.DECIMAL_TYPE, column_types[0])
    self.__verify_decimal_precision_scale(column_types[0], 9, 0)
    self.__verify_primitive_type(TTypeId.DECIMAL_TYPE, column_types[1])
    self.__verify_decimal_precision_scale(column_types[1], 10, 5)
    self.close(execute_statement_resp.operationHandle)

    # Verify the result metadata for the CHAR/VARCHAR types.
    execute_statement_resp, column_types = self.__fetch_result_column_types(
        "SELECT * FROM functional.chars_tiny ORDER BY cs LIMIT 1", 1,
        execute_statement_req)
    assert len(column_types) == 3
    self.__verify_primitive_type(TTypeId.CHAR_TYPE, column_types[0])
    self.__verify_char_max_len(column_types[0], 5)
    self.__verify_primitive_type(TTypeId.CHAR_TYPE, column_types[1])
    self.__verify_char_max_len(column_types[1], 140)
    self.__verify_primitive_type(TTypeId.VARCHAR_TYPE, column_types[2])
    self.__verify_char_max_len(column_types[2], 32)
    self.close(execute_statement_resp.operationHandle)

    # Verify the result metadata for the DATE type.
    execute_statement_resp, column_types = self.__fetch_result_column_types(
        "SELECT * FROM functional.date_tbl ORDER BY date_col LIMIT 1", 1,
        execute_statement_req)
    assert len(column_types) == 3
    self.__verify_primitive_type(TTypeId.INT_TYPE, column_types[0])
    self.__verify_primitive_type(TTypeId.DATE_TYPE, column_types[1])
    self.__verify_primitive_type(TTypeId.DATE_TYPE, column_types[2])
    self.close(execute_statement_resp.operationHandle)

    # Verify the result metadata for the BINARY type.
    execute_statement_resp, column_types = self.__fetch_result_column_types(
        "SELECT * from functional.binary_tbl ORDER BY binary_col LIMIT 1", 1,
        execute_statement_req)
    assert len(column_types) == 3
    self.__verify_primitive_type(TTypeId.INT_TYPE, column_types[0])
    self.__verify_primitive_type(TTypeId.STRING_TYPE, column_types[1])
    self.__verify_primitive_type(TTypeId.BINARY_TYPE, column_types[2])
    self.close(execute_statement_resp.operationHandle)

  def __query_and_fetch(self, query):
    execute_statement_req = TCLIService.TExecuteStatementReq()
    execute_statement_req.sessionHandle = self.session_handle
    execute_statement_req.statement = query
    execute_statement_resp = self.hs2_client.ExecuteStatement(execute_statement_req)
    HS2TestSuite.check_response(execute_statement_resp)

    # Do the actual fetch with a valid request.
    fetch_results_req = TCLIService.TFetchResultsReq()
    fetch_results_req.operationHandle = execute_statement_resp.operationHandle
    fetch_results_req.maxRows = 1024
    fetch_results_resp = self.fetch(fetch_results_req)

    return fetch_results_resp

  @needs_session()
  def test_alltypes_v6(self):
    """Test that a simple select statement works for all types"""
    fetch_results_resp = self.__query_and_fetch(
      "SELECT *, NULL from functional.alltypes ORDER BY id LIMIT 1")

    num_rows, result = self.column_results_to_string(fetch_results_resp.results.columns)
    assert num_rows == 1
    assert result == \
        "0, True, 0, 0, 0, 0, 0.0, 0.0, 01/01/09, 0, 2009-01-01 00:00:00, 2009, 1, NULL\n"

    # Decimals
    fetch_results_resp = self.__query_and_fetch(
      "SELECT * from functional.decimal_tbl LIMIT 1")
    num_rows, result = self.column_results_to_string(fetch_results_resp.results.columns)
    assert result == ("1234, 2222, 1.2345678900, "
                      "0.12345678900000000000000000000000000000, 12345.78900, 1\n")

    # VARCHAR
    fetch_results_resp = self.__query_and_fetch("SELECT CAST('str' AS VARCHAR(3))")
    num_rows, result = self.column_results_to_string(fetch_results_resp.results.columns)
    assert result == "str\n"

    # CHAR not inlined
    fetch_results_resp = self.__query_and_fetch("SELECT CAST('car' AS CHAR(140))")
    num_rows, result = self.column_results_to_string(fetch_results_resp.results.columns)
    assert result == "car" + (" " * 137) + "\n"

    # CHAR inlined
    fetch_results_resp = self.__query_and_fetch("SELECT CAST('car' AS CHAR(5))")
    num_rows, result = self.column_results_to_string(fetch_results_resp.results.columns)
    assert result == "car  \n"

    # Date
    fetch_results_resp = self.__query_and_fetch(
      "SELECT * from functional.date_tbl ORDER BY date_col LIMIT 1")
    num_rows, result = self.column_results_to_string(fetch_results_resp.results.columns)
    assert result == ("0, 0001-01-01, 0001-01-01\n")

    # Binary
    fetch_results_resp = self.__query_and_fetch(
      "SELECT * from functional.binary_tbl ORDER BY id LIMIT 1")
    num_rows, result = self.column_results_to_string(fetch_results_resp.results.columns)
    assert result == ("1, ascii, binary1\n")

  @needs_session()
  def test_show_partitions(self):
    """Regression test for IMPALA-1330"""
    for query in ["SHOW PARTITIONS functional.alltypes",
                  "SHOW TABLE STATS functional.alltypes"]:
      fetch_results_resp = self.__query_and_fetch(query)
      num_rows, result = \
          self.column_results_to_string(fetch_results_resp.results.columns)
      assert num_rows == 25
      # Match whether stats are computed or not
      assert re.match(
        r"2009, 1, -?\d+, -?\d+, \d*\.?\d+KB, NOT CACHED, NOT CACHED, TEXT", result) is not None

  @needs_session()
  def test_show_column_stats(self):
    fetch_results_resp = self.__query_and_fetch("SHOW COLUMN STATS functional.alltypes")
    num_rows, result = self.column_results_to_string(fetch_results_resp.results.columns)
    assert num_rows == 13
    assert re.match(r"id, INT, -?\d+, -?\d+, -?\d+, 4.0", result) is not None

  @needs_session(TCLIService.TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V1)
  def test_execute_select_v1(self):
    """Test that a simple select statement works in the row-oriented protocol"""
    execute_statement_req = TCLIService.TExecuteStatementReq()
    execute_statement_req.sessionHandle = self.session_handle
    execute_statement_req.statement = "SELECT COUNT(*) FROM functional.alltypes"
    execute_statement_resp = self.hs2_client.ExecuteStatement(execute_statement_req)
    HS2TestSuite.check_response(execute_statement_resp)

    fetch_results_req = TCLIService.TFetchResultsReq()
    fetch_results_req.operationHandle = execute_statement_resp.operationHandle
    fetch_results_req.maxRows = 100
    fetch_results_resp = self.fetch(fetch_results_req)

    assert len(fetch_results_resp.results.rows) == 1
    assert fetch_results_resp.results.startRowOffset == 0

    try:
      assert not fetch_results_resp.hasMoreRows
    except AssertionError:
      pytest.xfail("IMPALA-558")

  @needs_session()
  def test_select_null(self):
    """Regression test for IMPALA-1370, where NULL literals would appear as strings where
    they should be booleans"""
    execute_statement_req = TCLIService.TExecuteStatementReq()
    execute_statement_req.sessionHandle = self.session_handle
    execute_statement_req.statement = "select null"
    execute_statement_resp = self.hs2_client.ExecuteStatement(execute_statement_req)
    HS2TestSuite.check_response(execute_statement_resp)

    # Check that the expected type is boolean (for compatibility with Hive, see also
    # IMPALA-914)
    get_result_metadata_req = TCLIService.TGetResultSetMetadataReq()
    get_result_metadata_req.operationHandle = execute_statement_resp.operationHandle
    get_result_metadata_resp = \
        self.hs2_client.GetResultSetMetadata(get_result_metadata_req)
    col = get_result_metadata_resp.schema.columns[0]
    assert col.typeDesc.types[0].primitiveEntry.type == TTypeId.BOOLEAN_TYPE

    # Check that the actual type is boolean
    fetch_results_req = TCLIService.TFetchResultsReq()
    fetch_results_req.operationHandle = execute_statement_resp.operationHandle
    fetch_results_req.maxRows = 1
    fetch_results_resp = self.fetch(fetch_results_req)
    assert fetch_results_resp.results.columns[0].boolVal is not None

    assert self.column_results_to_string(
      fetch_results_resp.results.columns) == (1, "NULL\n")

  @needs_session()
  def test_compute_stats(self):
    """Exercise the child query path"""
    self.__query_and_fetch("compute stats functional.alltypes")

  @needs_session()
  def test_invalid_secret(self):
    """Test that the FetchResults, GetResultSetMetadata and CloseOperation APIs validate
    the session secret."""
    execute_req = TCLIService.TExecuteStatementReq(
        self.session_handle, "select 'something something'")
    execute_resp = self.hs2_client.ExecuteStatement(execute_req)
    HS2TestSuite.check_response(execute_resp)

    good_handle = execute_resp.operationHandle
    bad_handle = create_op_handle_without_secret(good_handle)

    # Fetching and closing operations with an invalid handle should be a no-op, i.e.
    # the later operations with the good handle should succeed.
    HS2TestSuite.check_invalid_query(self.hs2_client.FetchResults(
        TCLIService.TFetchResultsReq(operationHandle=bad_handle, maxRows=1024)),
        expect_legacy_err=True)
    HS2TestSuite.check_invalid_query(self.hs2_client.GetResultSetMetadata(
        TCLIService.TGetResultSetMetadataReq(operationHandle=bad_handle)),
        expect_legacy_err=True)
    HS2TestSuite.check_invalid_query(self.hs2_client.CloseOperation(
        TCLIService.TCloseOperationReq(operationHandle=bad_handle)),
        expect_legacy_err=True)

    # Ensure that the good handle remained valid.
    HS2TestSuite.check_response(self.hs2_client.FetchResults(
        TCLIService.TFetchResultsReq(operationHandle=good_handle, maxRows=1024)))
    HS2TestSuite.check_response(self.hs2_client.GetResultSetMetadata(
        TCLIService.TGetResultSetMetadataReq(operationHandle=good_handle)))
    HS2TestSuite.check_response(self.hs2_client.CloseOperation(
        TCLIService.TCloseOperationReq(operationHandle=good_handle)))
