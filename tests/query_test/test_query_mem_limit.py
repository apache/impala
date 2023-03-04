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
# Targeted tests to validate per-query memory limit.

from __future__ import absolute_import, division, print_function
import pytest
import re
import sys
from copy import copy

from tests.beeswax.impala_beeswax import ImpalaBeeswaxException
from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.skip import SkipIfEC
from tests.common.test_dimensions import (
    ImpalaTestDimension,
    create_single_exec_option_dimension,
    create_uncompressed_text_dimension)

class TestQueryMemLimit(ImpalaTestSuite):
  """Test class to do functional validation of per query memory limits.

  A specific query is run on text files, with the memory limit being added as
  an extra dimension. The query is expected to fail/pass depending on the limit
  value.
  """
  # There are a lot of 'unique' comments in lineitem.
  # Almost 80% of the table size.
  QUERIES = ["select count(distinct l_comment) from lineitem",
             "select group_concat(l_linestatus) from lineitem"]
  # TODO: It will be nice if we can get how much memory a query uses
  # dynamically, even if it is a rough approximation.
  # A mem_limit is expressed in bytes, with values <= 0 signifying no cap.
  # These values are either really small, unlimited, or have a really large cap.
  MAXINT_BYTES = str(sys.maxsize)
  MAXINT_MB = str(sys.maxsize // (1024 * 1024))
  MAXINT_GB = str(sys.maxsize // (1024 * 1024 * 1024))
  # We expect the tests with MAXINT_* using valid units [bmg] to succeed.
  PASS_REGEX = re.compile("(%s|%s|%s)[bmg]?$" % (MAXINT_BYTES, MAXINT_MB, MAXINT_GB),
                          re.I)
  MEM_LIMITS = ["-1", "0", "1", "10", "100", "1000", "10000", MAXINT_BYTES,
                MAXINT_BYTES + "b", MAXINT_BYTES + "B",
                MAXINT_MB + "m", MAXINT_MB + "M",
                MAXINT_GB + "g", MAXINT_GB + "G",
                # invalid per-query memory limits
                "-1234", "-3.14", "xyz", "100%", MAXINT_BYTES + "k", "k" + MAXINT_BYTES]

  MEM_LIMITS_CORE = ["-1", "0", "10000", MAXINT_BYTES,
                MAXINT_BYTES + "b", MAXINT_MB + "M", MAXINT_GB + "g"]

  @classmethod
  def get_workload(self):
    return 'tpch'

  @classmethod
  def add_test_dimensions(cls):
    super(TestQueryMemLimit, cls).add_test_dimensions()
    # Only run the query for text
    cls.ImpalaTestMatrix.add_dimension(
        create_uncompressed_text_dimension(cls.get_workload()))

    # add mem_limit as a test dimension.
    if cls.exploration_strategy() == 'core':
      cls.ImpalaTestMatrix.add_dimension(\
          ImpalaTestDimension('mem_limit', *TestQueryMemLimit.MEM_LIMITS_CORE))
    else:
      cls.ImpalaTestMatrix.add_dimension(\
          ImpalaTestDimension('mem_limit', *TestQueryMemLimit.MEM_LIMITS))

    # Make query a test dimension so we can support more queries.
    cls.ImpalaTestMatrix.add_dimension(
        ImpalaTestDimension('query', *TestQueryMemLimit.QUERIES))
    # This query takes a very long time to finish with a bound on the batch_size.
    # Remove the bound on the batch size.
    cls.ImpalaTestMatrix.add_constraint(
        lambda v: v.get_value('exec_option')['batch_size'] == 0)

  @pytest.mark.execute_serially
  def test_mem_limit(self, vector):
    mem_limit = copy(vector.get_value('mem_limit'))
    exec_options = copy(vector.get_value('exec_option'))
    exec_options['mem_limit'] = mem_limit
    # Send to the no-limits pool so that no memory limits apply.
    exec_options['request_pool'] = "root.no-limits"
    # IMPALA-9856: For the group_concat query, this test expect a resulting row up to
    # 17.17 MB in size.Therefore, we explicitly set 18 MB MAX_ROW_SIZE here so that it
    # can fit in BufferedPlanRootSink.
    exec_options['max_row_size'] = '18M'
    query = vector.get_value('query')
    table_format = vector.get_value('table_format')
    if mem_limit in["0", "-1"] or self.PASS_REGEX.match(mem_limit):
      # should succeed
      self.__exec_query(query, exec_options, True, table_format)
    else:
      # should fail
      self.__exec_query(query, exec_options, False, table_format)

  def __exec_query(self, query, exec_options, should_succeed, table_format):
    try:
      self.execute_query(query, exec_options, table_format=table_format)
      assert should_succeed, "Query was expected to fail"
    except ImpalaBeeswaxException as e:
      assert not should_succeed, "Query should not have failed: %s" % e


class TestCodegenMemLimit(ImpalaTestSuite):
  """Tests that memory limit applies to codegen """
  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestCodegenMemLimit, cls).add_test_dimensions()
    # Run with num_nodes=1 to avoid races between fragments allocating memory.
    cls.ImpalaTestMatrix.add_dimension(create_single_exec_option_dimension(
        num_nodes=1, disable_codegen_rows_threshold=0))
    # Only run the query for parquet
    cls.ImpalaTestMatrix.add_constraint(
      lambda v: v.get_value('table_format').file_format == 'parquet')

  def test_codegen_mem_limit(self, vector):
    self.run_test_case('QueryTest/codegen-mem-limit', vector)
