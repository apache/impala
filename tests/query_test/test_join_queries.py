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

# Targeted tests for Impala joins
#
from __future__ import absolute_import, division, print_function
import pytest
from copy import deepcopy

from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.skip import SkipIf, SkipIfFS
from tests.common.test_dimensions import (
    add_exec_option_dimension,
    add_mandatory_exec_option,
    create_exec_option_dimension,
    create_single_exec_option_dimension,
    create_table_format_dimension)


ENABLE_OUTER_JOIN_TO_INNER_TRANSFORMATION = ['false', 'true']


def batch_size_dim(cls):
  if cls.exploration_strategy() == 'exhaustive':
    return [0, 1]
  else:
    return [0]


def mt_dop_dim(cls):
  if cls.exploration_strategy() == 'exhaustive':
    return [0, 1, 4]
  else:
    return [0, 4]


class TestJoinBase(ImpalaTestSuite):
  """The base class for test join classes.
  Intended to provide subclasses with default test dimensions declaration
  and constraints through add_test_dimensions() both in core and
  exhaustive exploration."""

  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestJoinBase, cls).add_test_dimensions()

    # Set exec options
    cls.ImpalaTestMatrix.add_dimension(
        create_exec_option_dimension(batch_sizes=batch_size_dim(cls)))

    cls.ImpalaTestMatrix.add_constraint(lambda v:
        v.get_value('table_format').file_format in ['parquet'])


class TestJoinQueries(TestJoinBase):

  @classmethod
  def add_test_dimensions(cls):
    super(TestJoinQueries, cls).add_test_dimensions()
    add_exec_option_dimension(cls, 'mt_dop', mt_dop_dim(cls))

  def test_basic_joins(self, vector):
    self.run_test_case('QueryTest/joins', vector)

  @SkipIfFS.hbase
  @SkipIf.skip_hbase
  def test_joins_against_hbase(self, vector):
    # TODO: Look into splitting up join tests to accomodate hbase.
    # Joins with hbase tables produce drastically different results.
    self.run_test_case('QueryTest/joins-against-hbase', vector)

  def test_outer_joins(self, vector):
    self.run_test_case('QueryTest/outer-joins', vector)

  def test_empty_build_joins(self, vector):
    self.run_test_case('QueryTest/empty-build-joins', vector)


class TestSingleNodeJoins(TestJoinBase):

  @classmethod
  def add_test_dimensions(cls):
    super(TestSingleNodeJoins, cls).add_test_dimensions()
    # Redeclare exec options with num_nodes=1, batch_size=0.
    cls.ImpalaTestMatrix.add_dimension(
        create_exec_option_dimension(cluster_sizes=[1], batch_sizes=[0]))

  def test_single_node_nested_loop_joins(self, vector):
    # Test the execution of nested-loops joins for join types that can only be
    # executed in a single node (right [outer|semi|anti] and full outer joins).
    self.run_test_case('QueryTest/single-node-nlj', vector)


class TestSingleNodeJoinsExhaustive(TestJoinBase):

  @classmethod
  def add_test_dimensions(cls):
    super(TestSingleNodeJoinsExhaustive, cls).add_test_dimensions()
    if cls.exploration_strategy() != 'exhaustive':
      # skip this test if not in exhaustive exploration.
      pytest.skip("Only run in exhaustive exploration.")

    # Redeclare exec options with num_nodes=1, batch_size=0.
    cls.ImpalaTestMatrix.add_dimension(
        create_exec_option_dimension(cluster_sizes=[1], batch_sizes=[0]))
    add_exec_option_dimension(cls, 'mt_dop', mt_dop_dim(cls))

  def test_single_node_joins_with_limits_exhaustive(self, vector):
    new_vector = deepcopy(vector)
    del new_vector.get_value('exec_option')['batch_size']  # .test file sets batch_size
    self.run_test_case('QueryTest/single-node-joins-with-limits-exhaustive', new_vector)

  def test_single_node_nested_loop_joins_exhaustive(self, vector):
    # Test the execution of nested-loops joins for join types that can only be
    # executed in a single node (right [outer|semi|anti] and full outer joins).
    self.run_test_case('QueryTest/single-node-nlj-exhaustive', vector)


class TestOuterJoinToInnerTransformation(TestJoinBase):

  @classmethod
  def add_test_dimensions(cls):
    super(TestOuterJoinToInnerTransformation, cls).add_test_dimensions()
    add_exec_option_dimension(cls, 'enable_outer_join_to_inner_transformation',
                              ENABLE_OUTER_JOIN_TO_INNER_TRANSFORMATION)

  def test_outer_to_inner_joins(self, vector):
    self.run_test_case('QueryTest/outer-to-inner-joins', vector)


class TestMissTupleJoins(TestJoinBase):

  @classmethod
  def add_test_dimensions(cls):
    super(TestMissTupleJoins, cls).add_test_dimensions()
    # Only need to run with single exec option dimension.
    cls.ImpalaTestMatrix.add_dimension(create_single_exec_option_dimension())

  def test_miss_tuple_joins(self, vector, unique_database):
    self.run_test_case('QueryTest/miss-tuple-joins', vector, unique_database)


class TestTPCHJoinQueries(TestJoinBase):
  # Uses the TPC-H dataset in order to have larger joins. Needed for example to test
  # the repartitioning codepaths.

  @classmethod
  def get_workload(cls):
    return 'tpch'

  @classmethod
  def add_test_dimensions(cls):
    super(TestTPCHJoinQueries, cls).add_test_dimensions()

  @classmethod
  def teardown_class(cls):
    cls.client.execute('set mem_limit = 0')
    super(TestTPCHJoinQueries, cls).teardown_class()

  def test_outer_joins(self, vector, unique_database):
    self.run_test_case('tpch-outer-joins', vector,
        test_file_vars={'$UNIQUE_DB': unique_database})


class TestSemiJoinQueries(TestJoinBase):

  @classmethod
  def add_test_dimensions(cls):
    super(TestSemiJoinQueries, cls).add_test_dimensions()

  def __load_semi_join_tables(self, db_name):
    # Create and load fresh test tables for semi/anti-join tests
    fq_tbl_name_a = '%s.SemiJoinTblA' % db_name
    self.client.execute('create table %s (a int, b int, c int)' % fq_tbl_name_a)
    self.client.execute('insert into %s values(1,1,1)' % fq_tbl_name_a)
    self.client.execute('insert into %s values(1,1,10)' % fq_tbl_name_a)
    self.client.execute('insert into %s values(1,2,10)' % fq_tbl_name_a)
    self.client.execute('insert into %s values(1,3,10)' % fq_tbl_name_a)
    self.client.execute('insert into %s values(NULL,NULL,30)' % fq_tbl_name_a)
    self.client.execute('insert into %s values(2,4,30)' % fq_tbl_name_a)
    self.client.execute('insert into %s values(2,NULL,20)' % fq_tbl_name_a)

    fq_tbl_name_b = '%s.SemiJoinTblB' % db_name
    self.client.execute('create table %s (a int, b int, c int)' % fq_tbl_name_b)
    self.client.execute('insert into %s values(1,1,1)' % fq_tbl_name_b)
    self.client.execute('insert into %s values(1,1,10)' % fq_tbl_name_b)
    self.client.execute('insert into %s values(1,2,5)' % fq_tbl_name_b)
    self.client.execute('insert into %s values(1,NULL,10)' % fq_tbl_name_b)
    self.client.execute('insert into %s values(2,10,NULL)' % fq_tbl_name_b)
    self.client.execute('insert into %s values(3,NULL,NULL)' % fq_tbl_name_b)
    self.client.execute('insert into %s values(3,NULL,50)' % fq_tbl_name_b)

  def test_semi_joins(self, vector, unique_database):
    self.__load_semi_join_tables(unique_database)
    self.run_test_case('QueryTest/semi-joins', vector, unique_database)


class TestSemiJoinQueriesExhaustive(TestJoinBase):

  @classmethod
  def add_test_dimensions(cls):
    super(TestSemiJoinQueriesExhaustive, cls).add_test_dimensions()
    if cls.exploration_strategy() != 'exhaustive':
      # skip this test if not in exhaustive exploration.
      pytest.skip("Only run in exhaustive exploration.")

  @pytest.mark.execute_serially
  def test_semi_joins_exhaustive(self, vector):
    """Expensive and memory-intensive semi-join tests."""
    self.run_test_case('QueryTest/semi-joins-exhaustive', vector)


class TestSpillingHashJoin(ImpalaTestSuite):

  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestSpillingHashJoin, cls).add_test_dimensions()
    # To cut down on test execution time, only run in exhaustive.
    if cls.exploration_strategy() != 'exhaustive':
      pytest.skip("Only run in exhaustive exploration.")
    cls.ImpalaTestMatrix.add_constraint(
        lambda v: v.get_value('table_format').file_format == 'parquet')
    cls.ImpalaTestMatrix.add_constraint(lambda v:
        v.get_value('exec_option')['disable_codegen'] is False)

  @pytest.mark.execute_serially
  def test_spilling_hash_join(self, vector, unique_database):
    """Regression test for IMPALA-13138. It loads a few large tables and runs a complex
    query that spills during JOIN build that crashed Impala before IMPALA-13138."""
    self.run_test_case('QueryTest/create-tables-impala-13138', vector, unique_database)
    for i in range(0, 5):
      self.run_test_case('QueryTest/query-impala-13138', vector, unique_database)


class TestExprValueCache(ImpalaTestSuite):
  # Test that HashTableCtx::ExprValueCache memory usage stays under 256KB.
  # Run TPC-DS Q97 with bare minimum memory limit, MT_DOP=1, and max BATCH_SIZE.
  # Before IMPALA-13075, the test query will pass Planner and Admission Control,
  # but later failed during backend execution due to memory limit exceeded.

  @classmethod
  def get_workload(cls):
    return 'tpcds_partitioned'

  @classmethod
  def add_test_dimensions(cls):
    super(TestExprValueCache, cls).add_test_dimensions()
    # create_single_exec_option_dimension + batch_size=65536
    cls.ImpalaTestMatrix.add_dimension(create_exec_option_dimension(
      cluster_sizes=[0], disable_codegen_options=[False], batch_sizes=[65536],
      disable_codegen_rows_threshold_options=[5000]))
    cls.ImpalaTestMatrix.add_dimension(
        create_table_format_dimension(cls.get_workload(), 'parquet/snap/block'))
    add_mandatory_exec_option(cls, 'runtime_filter_mode', 'OFF')
    add_mandatory_exec_option(cls, 'mem_limit', '149mb')
    add_mandatory_exec_option(cls, 'mt_dop', 1)

  def test_expr_value_cache_fits(self, vector):
    self.run_test_case('tpcds-q97', vector)
