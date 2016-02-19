# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
# Validates limit on scan nodes
#
import logging
import pytest
from copy import copy
from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.test_vector import *
from tests.common.test_dimensions import create_exec_option_dimension
from tests.common.test_dimensions import create_uncompressed_text_dimension
from tests.util.test_file_parser import QueryTestSectionReader

class TestExprs(ImpalaTestSuite):
  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestExprs, cls).add_test_dimensions()
    if cls.exploration_strategy() == 'core':
      # Test with file format that supports codegen
      cls.TestMatrix.add_constraint(lambda v:\
          v.get_value('table_format').file_format == 'text' and\
          v.get_value('table_format').compression_codec == 'none')

  def test_exprs(self, vector):
    # TODO: Enable some of these tests for Avro if possible
    # Don't attempt to evaluate timestamp expressions with Avro tables (which don't
    # support a timestamp type)"
    table_format = vector.get_value('table_format')
    if table_format.file_format == 'avro':
      pytest.skip()
    if table_format.file_format == 'hbase':
      pytest.xfail("A lot of queries check for NULLs, which hbase does not recognize")
    self.run_test_case('QueryTest/exprs', vector)

    # This will change the current database to matching table format and then execute
    # select current_database(). An error will be thrown if multiple values are returned.
    current_db = self.execute_scalar('select current_database()', vector=vector)
    assert current_db == QueryTestSectionReader.get_db_name(table_format)

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
      cls.TestMatrix.clear_dimension('exec_option')
      cls.TestMatrix.add_dimension(create_exec_option_dimension(
          cluster_sizes=[0], disable_codegen_options=[False, True], batch_sizes=[0]))

    # There is no reason to run these tests using all dimensions.
    cls.TestMatrix.add_dimension(create_uncompressed_text_dimension(cls.get_workload()))

  def test_expr_child_limit(self, vector):
    # IN predicate
    in_query = "select 1 IN("
    for i in xrange(0, self.EXPR_CHILDREN_LIMIT - 1):
      in_query += str(i)
      if (i + 1 != self.EXPR_CHILDREN_LIMIT - 1):
        in_query += ","
    in_query += ")"
    self.__exec_query(in_query)

    # CASE expr
    case_query = "select case "
    for i in xrange(0, self.EXPR_CHILDREN_LIMIT/2):
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

  def __gen_deep_infix_expr(self, prefix, repeat_suffix):
    expr = prefix
    for i in xrange(self.EXPR_DEPTH_LIMIT - 1):
      expr += repeat_suffix
    return expr

  def __gen_deep_func_expr(self, open_func, base_arg, close_func):
    expr = ""
    for i in xrange(self.EXPR_DEPTH_LIMIT - 1):
      expr += open_func
    expr += base_arg
    for i in xrange(self.EXPR_DEPTH_LIMIT - 1):
      expr += close_func
    return expr

  def __exec_query(self, sql_str):
    try:
      impala_ret = self.execute_query(sql_str)
      assert impala_ret.success, "Failed to execute query %s" % (sql_str)
    except: # consider any exception a failure
      assert False, "Failed to execute query %s" % (sql_str)
