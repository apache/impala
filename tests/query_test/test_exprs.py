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
from builtins import range
import pytest
import re
from random import randint

from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.test_dimensions import create_exec_option_dimension
from tests.common.test_dimensions import create_uncompressed_text_dimension
from tests.common.test_vector import ImpalaTestDimension
from tests.util.test_file_parser import QueryTestSectionReader

class TestExprs(ImpalaTestSuite):
  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestExprs, cls).add_test_dimensions()
    # Test with and without expr rewrites to cover regular expr evaluations
    # as well as constant folding, in particular, timestamp literals.
    cls.ImpalaTestMatrix.add_dimension(
        ImpalaTestDimension('enable_expr_rewrites', *[0,1]))
    if cls.exploration_strategy() == 'core':
      # Test with file format that supports codegen
      cls.ImpalaTestMatrix.add_constraint(lambda v:
          v.get_value('table_format').file_format == 'parquet' and
          v.get_value('table_format').compression_codec == 'none')

  def test_exprs(self, vector):
    vector.get_value('exec_option')['enable_expr_rewrites'] = \
        vector.get_value('enable_expr_rewrites')
    # TODO: Enable some of these tests for Avro if possible
    # Don't attempt to evaluate timestamp expressions with Avro tables (which don't
    # support a timestamp type)"
    table_format = vector.get_value('table_format')
    if table_format.file_format == 'avro':
      pytest.skip()
    if table_format.file_format == 'hbase':
      pytest.xfail("A lot of queries check for NULLs, which hbase does not recognize")
    if table_format.file_format == 'kudu':
      # Can't load LikeTbl without KUDU-1570.
      pytest.xfail("Need support for Kudu tables with nullable PKs (KUDU-1570)")
    self.run_test_case('QueryTest/exprs', vector)

    # This will change the current database to matching table format and then execute
    # select current_database(). An error will be thrown if multiple values are returned.
    current_db = self.execute_scalar('select current_database()', vector=vector)
    assert current_db == QueryTestSectionReader.get_db_name(table_format)

  def test_special_strings(self, vector):
    """Test handling of expressions with "special" strings."""
    vector.get_value('exec_option')['enable_expr_rewrites'] = \
        vector.get_value('enable_expr_rewrites')
    self.run_test_case('QueryTest/special-strings', vector)

# Tests very deep expression trees and expressions with many children. Impala defines
# a 'safe' upper bound on the expr depth and the number of expr children in the
# FE Expr.java and any changes to those limits should be reflected in this test.
# The expr limits primarily guard against stack overflows or similar problems
# causing crashes. Therefore, this tests succeeds if no Impalads crash.
class TestExprLimits(ImpalaTestSuite):
  # Keep these in sync with Expr.java
  EXPR_CHILDREN_LIMIT = 10000
  EXPR_DEPTH_LIMIT = 1000

  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestExprLimits, cls).add_test_dimensions()
    if cls.exploration_strategy() != 'exhaustive':
      # Ensure the test runs with codegen enabled and disabled, even when the
      # exploration strategy is not exhaustive.
      cls.ImpalaTestMatrix.clear_dimension('exec_option')
      cls.ImpalaTestMatrix.add_dimension(create_exec_option_dimension(
          cluster_sizes=[0], disable_codegen_options=[False, True], batch_sizes=[0]))

    # There is no reason to run these tests using all dimensions.
    cls.ImpalaTestMatrix.add_dimension(
        create_uncompressed_text_dimension(cls.get_workload()))

  def test_expr_child_limit(self, vector):
    # IN predicate
    in_query = "select 1 IN("
    for i in range(0, self.EXPR_CHILDREN_LIMIT - 1):
      in_query += str(i)
      if (i + 1 != self.EXPR_CHILDREN_LIMIT - 1):
        in_query += ","
    in_query += ")"
    self.__exec_query(in_query)

    # CASE expr
    case_query = "select case "
    for i in range(0, self.EXPR_CHILDREN_LIMIT // 2):
      case_query += " when true then 1"
    case_query += " end"
    self.__exec_query(case_query)

  def test_expr_depth_limit(self, vector):
    # Compound predicates
    and_query = "select " + self.__gen_deep_infix_expr("true", " and false")
    self.__exec_query(and_query)
    or_query = "select " + self.__gen_deep_infix_expr("true", " or false")
    self.__exec_query(or_query)

    # Arithmetic expr
    arith_query = "select " + self.__gen_deep_infix_expr("1", " + 1")
    self.__exec_query(arith_query)

    func_query = "select " + self.__gen_deep_func_expr("lower(", "'abc'", ")")
    self.__exec_query(func_query)

    # Casts.
    cast_query = "select " + self.__gen_deep_func_expr("cast(", "1", " as int)")
    self.__exec_query(cast_query)

  def test_under_statement_expression_limit(self):
    """Generate a huge case statement that barely fits within the statement expression
       limit and verify that it runs."""
    # This takes 20+ minutes, so only run it on exhaustive.
    # TODO: Determine whether this needs to run serially. It use >5 GB of memory.
    if self.exploration_strategy() != 'exhaustive':
      pytest.skip("Only test limit of codegen on exhaustive")
    case = self.__gen_huge_case("int_col", 32, 2, "  ")
    query = "select {0} as huge_case from functional_parquet.alltypes".format(case)
    self.__exec_query(query)

  def test_max_statement_size(self):
    """Generate a huge case statement that exceeds the default 16MB limit and verify
       that it gets rejected."""

    expected_err_tmpl = ("Statement length of {0} bytes exceeds the maximum "
        "statement length \({1} bytes\)")
    size_16mb = 16 * 1024 * 1024

    # Case 1: a valid SQL that would parse correctly
    case = self.__gen_huge_case("int_col", 75, 2, "  ")
    query = "select {0} as huge_case from functional.alltypes".format(case)
    err = self.execute_query_expect_failure(self.client, query)
    assert re.search(expected_err_tmpl.format(len(query), size_16mb), str(err))

    # Case 2: a string of 'a' characters that does not parse. This will still fail
    # with the same message, because the check is before parsing.
    invalid_sql = 'a' * (size_16mb + 1)
    err = self.execute_query_expect_failure(self.client, invalid_sql)
    assert re.search(expected_err_tmpl.format(len(invalid_sql), size_16mb), str(err))

  # This test can take ~2GB memory while it takes only ~10 seconds. It caused OOM
  # in the past, so it is safer to run it serially.
  @pytest.mark.execute_serially
  def test_statement_expression_limit(self):
    """Generate a huge case statement that barely fits within the 16MB limit but exceeds
       the statement expression limit. Verify that it fails."""
    case = self.__gen_huge_case("int_col", 66, 2, "  ")
    query = "select {0} as huge_case from functional.alltypes".format(case)
    assert len(query) < 16 * 1024 * 1024
    expected_err_re = ("Exceeded the statement expression limit \({0}\)\n"
        "Statement has .* expressions.").format(250000)
    err = self.execute_query_expect_failure(self.client, query)
    assert re.search(expected_err_re, str(err))

  def __gen_huge_case(self, col_name, fanout, depth, indent):
    toks = ["case\n"]
    for i in range(fanout):
      add = randint(1, 1000000)
      divisor = randint(1, 10000000)
      mod = randint(0, divisor)
      # Generate a mathematical expr that can't be easily optimised out.
      when_expr = "{0} + {1} % {2} = {3}".format(col_name, add, divisor, mod)
      if depth == 0:
        then_expr = "{0}".format(i)
      else:
        then_expr = "({0})".format(
            self.__gen_huge_case(col_name, fanout, depth - 1, indent + "  "))
      toks.append(indent)
      toks.append("when {0} then {1}\n".format(when_expr, then_expr))
    toks.append(indent)
    toks.append("end")
    return ''.join(toks)

  def __gen_deep_infix_expr(self, prefix, repeat_suffix):
    expr = prefix
    for i in range(self.EXPR_DEPTH_LIMIT - 1):
      expr += repeat_suffix
    return expr

  def __gen_deep_func_expr(self, open_func, base_arg, close_func):
    expr = ""
    for i in range(self.EXPR_DEPTH_LIMIT - 1):
      expr += open_func
    expr += base_arg
    for i in range(self.EXPR_DEPTH_LIMIT - 1):
      expr += close_func
    return expr

  def __exec_query(self, sql_str):
    try:
      impala_ret = self.execute_query(sql_str)
      assert impala_ret.success, "Failed to execute query %s" % (sql_str)
    except Exception as e:  # consider any exception a failure
      assert False, "Failed to execute query %s: %s" % (sql_str, e)

class TestUtcTimestampFunctions(ImpalaTestSuite):
  """Tests for UTC timestamp functions, i.e. functions that do not depend on the behavior
     of the flag --use_local_tz_for_unix_timestamp_conversions. Tests added here should
     also be run in the custom cluster test test_local_tz_conversion.py to ensure they
     have the same behavior when the conversion flag is set to true."""

  @classmethod
  def add_test_dimensions(cls):
    super(TestUtcTimestampFunctions, cls).add_test_dimensions()
    # Test with and without expr rewrites to cover regular expr evaluations
    # as well as constant folding, in particular, timestamp literals.
    cls.ImpalaTestMatrix.add_dimension(
        ImpalaTestDimension('enable_expr_rewrites', *[0,1]))
    if cls.exploration_strategy() == 'core':
      # Test with file format that supports codegen
      cls.ImpalaTestMatrix.add_constraint(lambda v:\
          v.get_value('table_format').file_format == 'text' and\
          v.get_value('table_format').compression_codec == 'none')

  @classmethod
  def get_workload(cls):
    return 'functional-query'

  def test_utc_functions(self, vector):
    vector.get_value('exec_option')['enable_expr_rewrites'] = \
        vector.get_value('enable_expr_rewrites')
    self.run_test_case('QueryTest/utc-timestamp-functions', vector)


class TestConstantFoldingNoTypeLoss(ImpalaTestSuite):
  """"Regression tests for IMPALA-11462."""

  @classmethod
  def get_workload(self):
    return "functional-query"

  @classmethod
  def add_test_dimensions(cls):
    super(TestConstantFoldingNoTypeLoss, cls).add_test_dimensions()
    # Test with and without expr rewrites to verify that constant folding does not change
    # the behaviour.
    cls.ImpalaTestMatrix.add_dimension(
        ImpalaTestDimension('enable_expr_rewrites', *[0,1]))
    # We don't actually use a table so one file format is enough.
    cls.ImpalaTestMatrix.add_constraint(lambda v:
        v.get_value('table_format').file_format in ['parquet'])

  def test_shiftleft(self, vector):
    """ Tests that the return values of the 'shiftleft' functions are correct for the
    input types (the return type should be the same as the first argument)."""
    types_and_widths = [
      ("TINYINT", 8),
      ("SMALLINT", 16),
      ("INT", 32),
      ("BIGINT", 64)
    ]
    query_template = ("select shiftleft(cast(1 as {typename}), z) c "
        "from (select {shift_val} z ) x")
    for (typename, width) in types_and_widths:
      shift_val = width - 2  # Valid and positive for signed types.
      expected_value = 1 << shift_val
      result = self.execute_query_expect_success(self.client,
          query_template.format(typename=typename, shift_val=shift_val))
      assert result.data == [str(expected_value)]

  def test_addition(self, vector):
    query = "select typeof(cast(1 as bigint) + cast(rand() as tinyint))"
    result = self.execute_query_expect_success(self.client, query)
    assert result.data == ["BIGINT"]


class TestNonConstPatternILike(ImpalaTestSuite):
  """Tests for ILIKE and IREGEXP with non-constant patterns for IMPALA-12581.
     These tests verify that ILIKE and IREGEXP work correctly when the pattern
     is not a constant string."""
  @classmethod
  def add_test_dimensions(cls):
    super(TestNonConstPatternILike, cls).add_test_dimensions()

  @classmethod
  def get_workload(cls):
    return 'functional-query'

  def test_non_const_pattern_ilike(self, vector, unique_database):
    tbl_name = '`{0}`.`ilike_test`'.format(unique_database)

    self.execute_query_expect_success(self.client,
        "CREATE TABLE {0} (pattern_str string)".format(tbl_name))
    self.execute_query_expect_success(self.client,
        "INSERT INTO TABLE {0} VALUES('%b%'), ('.*b.*')".format(tbl_name))

    ilike_result = self.execute_query_expect_success(self.client,
        "SELECT count(*) FROM {0} WHERE 'ABC' ILIKE pattern_str".format(tbl_name))
    assert int(ilike_result.get_data()) == 1
    iregexp_result = self.execute_query_expect_success(self.client,
        "SELECT count(*) FROM {0} WHERE 'ABC' IREGEXP pattern_str".format(tbl_name))
    assert int(iregexp_result.get_data()) == 1
