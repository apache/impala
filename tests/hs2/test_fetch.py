#!/usr/bin/env python
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import pytest
import re
from tests.hs2.hs2_test_suite import HS2TestSuite, needs_session
from TCLIService import TCLIService

# Simple test to make sure all the HS2 types are supported for both the row and
# column-oriented versions of the HS2 protocol.
class TestFetch(HS2TestSuite):
  def __verify_result_precision_scale(self, t, precision, scale):
    # This should be DECIMAL_TYPE but how do I get that in python
    assert t.typeDesc.types[0].primitiveEntry.type == 15
    p = t.typeDesc.types[0].primitiveEntry.typeQualifiers.qualifiers['precision']
    s = t.typeDesc.types[0].primitiveEntry.typeQualifiers.qualifiers['scale']
    assert p.i32Value == precision
    assert s.i32Value == scale

  @needs_session(TCLIService.TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V1)
  def test_alltypes_v1(self):
    execute_statement_req = TCLIService.TExecuteStatementReq()
    execute_statement_req.sessionHandle = self.session_handle

    execute_statement_req.statement =\
        "SELECT * FROM functional.alltypessmall ORDER BY id LIMIT 1"
    execute_statement_resp = self.hs2_client.ExecuteStatement(execute_statement_req)
    HS2TestSuite.check_response(execute_statement_resp)
    results = self.fetch(execute_statement_resp.operationHandle,
                TCLIService.TFetchOrientation.FETCH_NEXT, 1, 1)
    assert len(results.results.rows) == 1
    self.close(execute_statement_resp.operationHandle)

    execute_statement_req.statement =\
        "SELECT d1,d5 FROM functional.decimal_tbl ORDER BY d1 LIMIT 1"
    execute_statement_resp = self.hs2_client.ExecuteStatement(execute_statement_req)
    HS2TestSuite.check_response(execute_statement_resp)
    results = self.fetch(execute_statement_resp.operationHandle,
                TCLIService.TFetchOrientation.FETCH_NEXT, 1, 1)
    assert len(results.results.rows) == 1

    # Verify the result schema is what we expect. The result has 2 columns, the
    # first is decimal(9,0) and the second is decimal(10,5)
    metadata_resp = self.result_metadata(execute_statement_resp.operationHandle)
    column_types = metadata_resp.schema.columns
    assert len(column_types) == 2
    self.__verify_result_precision_scale(column_types[0], 9, 0)
    self.__verify_result_precision_scale(column_types[1], 10, 5)

    self.close(execute_statement_resp.operationHandle)

  def __query_and_fetch(self, query):
    execute_statement_req = TCLIService.TExecuteStatementReq()
    execute_statement_req.sessionHandle = self.session_handle
    execute_statement_req.statement = query
    execute_statement_resp = self.hs2_client.ExecuteStatement(execute_statement_req)
    HS2TestSuite.check_response(execute_statement_resp)

    fetch_results_req = TCLIService.TFetchResultsReq()
    fetch_results_req.operationHandle = execute_statement_resp.operationHandle
    fetch_results_req.maxRows = 1024
    fetch_results_resp = self.hs2_client.FetchResults(fetch_results_req)
    HS2TestSuite.check_response(fetch_results_resp)

    return fetch_results_resp

  def __column_results_to_string(self, columns):
    """Quick-and-dirty way to get a readable string to compare the output of a
    columnar-oriented query to its expected output"""
    formatted = ""
    num_rows = 0
    # Determine the number of rows by finding the type of the first column
    for col_type in HS2TestSuite.HS2_V6_COLUMN_TYPES:
      typed_col = getattr(columns[0], col_type)
      if typed_col != None:
        num_rows = len(typed_col.values)
        break

    for i in xrange(num_rows):
      row = []
      for c in columns:
        for col_type in HS2TestSuite.HS2_V6_COLUMN_TYPES:
          typed_col = getattr(c, col_type)
          if typed_col != None:
            indicator = ord(typed_col.nulls[i / 8])
            if indicator & (1 << (i % 8)):
              row.append("NULL")
            else:
              row.append(str(typed_col.values[i]))
            break
      formatted += (", ".join(row) + "\n")
    return (num_rows, formatted)

  @needs_session()
  def test_alltypes_v6(self):
    """Test that a simple select statement works for all types"""
    fetch_results_resp = self.__query_and_fetch(
      "SELECT *, NULL from functional.alltypes ORDER BY id LIMIT 1")

    num_rows, result = self.__column_results_to_string(fetch_results_resp.results.columns)
    assert num_rows == 1
    assert result == \
        "0, True, 0, 0, 0, 0, 0.0, 0.0, 01/01/09, 0, 2009-01-01 00:00:00, 2009, 1, NULL\n"

    # Decimals
    fetch_results_resp = self.__query_and_fetch(
      "SELECT * from functional.decimal_tbl LIMIT 1")
    num_rows, result = self.__column_results_to_string(fetch_results_resp.results.columns)
    assert result == ("1234, 2222, 1.2345678900, "
                      "0.12345678900000000000000000000000000000, 12345.78900, 1\n")

    # VARCHAR
    fetch_results_resp = self.__query_and_fetch("SELECT CAST('str' AS VARCHAR(3))")
    num_rows, result = self.__column_results_to_string(fetch_results_resp.results.columns)
    assert result == "str\n"

    # CHAR not inlined
    fetch_results_resp = self.__query_and_fetch("SELECT CAST('car' AS CHAR(140))")
    num_rows, result = self.__column_results_to_string(fetch_results_resp.results.columns)
    assert result == "car" + (" " * 137) + "\n"

    # CHAR inlined
    fetch_results_resp = self.__query_and_fetch("SELECT CAST('car' AS CHAR(5))")
    num_rows, result = self.__column_results_to_string(fetch_results_resp.results.columns)
    assert result == "car  \n"

  @needs_session()
  def test_show_partitions(self):
    """Regression test for IMPALA-1330"""
    for query in ["SHOW PARTITIONS functional.alltypes",
                  "SHOW TABLE STATS functional.alltypes"]:
      fetch_results_resp = self.__query_and_fetch(query)
      num_rows, result = \
          self.__column_results_to_string(fetch_results_resp.results.columns)
      assert num_rows == 25
      # Match whether stats are computed or not
      assert re.match(
        r"2009, 1, -?\d+, -?\d+, \d*\.?\d+KB, NOT CACHED, NOT CACHED, TEXT", result) is not None

  @needs_session()
  def test_show_column_stats(self):
    fetch_results_resp = self.__query_and_fetch("SHOW COLUMN STATS functional.alltypes")
    num_rows, result = self.__column_results_to_string(fetch_results_resp.results.columns)
    assert num_rows == 13
    assert re.match(r"id, INT, -?\d+, -?\d+, (NULL|\d+), 4.0", result) is not None

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
    fetch_results_resp = self.hs2_client.FetchResults(fetch_results_req)
    HS2TestSuite.check_response(fetch_results_resp)

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
    assert col.typeDesc.types[0].primitiveEntry.type == TCLIService.TTypeId.BOOLEAN_TYPE

    # Check that the actual type is boolean
    fetch_results_req = TCLIService.TFetchResultsReq()
    fetch_results_req.operationHandle = execute_statement_resp.operationHandle
    fetch_results_req.maxRows = 1
    fetch_results_resp = self.hs2_client.FetchResults(fetch_results_req)
    HS2TestSuite.check_response(fetch_results_resp)
    assert fetch_results_resp.results.columns[0].boolVal is not None

    assert self.__column_results_to_string(
      fetch_results_resp.results.columns) == (1, "NULL\n")

  @needs_session()
  def test_compute_stats(self):
    """Exercise the child query path"""
    self.__query_and_fetch("compute stats functional.alltypes")
