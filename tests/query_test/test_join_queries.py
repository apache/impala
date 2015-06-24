# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
# Targeted tests for Impala joins
#
import logging
import os
import pytest
from copy import copy
from tests.common.test_vector import *
from tests.common.impala_test_suite import *
from tests.common.skip import SkipIfS3, SkipIfIsilon

class TestJoinQueries(ImpalaTestSuite):
  BATCH_SIZES = [0, 1]

  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestJoinQueries, cls).add_test_dimensions()
    cls.TestMatrix.add_dimension(
        TestDimension('batch_size', *TestJoinQueries.BATCH_SIZES))
    # TODO: Look into splitting up join tests to accomodate hbase.
    # Joins with hbase tables produce drastically different results.
    cls.TestMatrix.add_constraint(lambda v:\
        v.get_value('table_format').file_format in ['parquet'])

    if cls.exploration_strategy() != 'exhaustive':
      # Cut down on execution time when not running in exhaustive mode.
      cls.TestMatrix.add_constraint(lambda v: v.get_value('batch_size') != 1)

  def test_joins(self, vector):
    new_vector = copy(vector)
    new_vector.get_value('exec_option')['batch_size'] = vector.get_value('batch_size')
    self.run_test_case('QueryTest/joins', new_vector)

  @SkipIfS3.hbase
  @SkipIfIsilon.hbase
  def test_joins_against_hbase(self, vector):
    new_vector = copy(vector)
    new_vector.get_value('exec_option')['batch_size'] = vector.get_value('batch_size')
    self.run_test_case('QueryTest/joins-against-hbase', new_vector)

  def test_outer_joins(self, vector):
    new_vector = copy(vector)
    new_vector.get_value('exec_option')['batch_size'] = vector.get_value('batch_size')
    self.run_test_case('QueryTest/outer-joins', new_vector)

  def test_single_node_nested_loop_joins(self, vector):
    # Test the execution of nested-loops joins for join types that can only be
    # executed in a single node (right [outer|semi|anti] and full outer joins).
    new_vector = copy(vector)
    new_vector.get_value('exec_option')['num_nodes'] = 1
    self.run_test_case('QueryTest/single-node-nlj', new_vector)

class TestTPCHJoinQueries(ImpalaTestSuite):
  # Uses the tpch dataset in order to have larger joins. Needed for example to test
  # the repartitioning codepaths.
  BATCH_SIZES = [0, 1]

  @classmethod
  def get_workload(cls):
    return 'tpch'

  @classmethod
  def add_test_dimensions(cls):
    super(TestTPCHJoinQueries, cls).add_test_dimensions()
    cls.TestMatrix.add_dimension(
        TestDimension('batch_size', *TestJoinQueries.BATCH_SIZES))
    cls.TestMatrix.add_constraint(lambda v:\
        v.get_value('table_format').file_format in ['parquet'])

    if cls.exploration_strategy() != 'exhaustive':
      # Cut down on execution time when not running in exhaustive mode.
      cls.TestMatrix.add_constraint(lambda v: v.get_value('batch_size') != 1)

  @classmethod
  def teardown_class(cls):
    cls.client.execute('set mem_limit = 0');
    super(TestTPCHJoinQueries, cls).teardown_class()

  def test_outer_joins(self, vector):
    new_vector = copy(vector)
    new_vector.get_value('exec_option')['batch_size'] = vector.get_value('batch_size')
    self.run_test_case('tpch-outer-joins', new_vector)

@SkipIfS3.insert
class TestSemiJoinQueries(ImpalaTestSuite):
  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestSemiJoinQueries, cls).add_test_dimensions()
    cls.TestMatrix.add_dimension(
        TestDimension('batch_size', *TestJoinQueries.BATCH_SIZES))
    # Joins with hbase tables produce drastically different results.
    cls.TestMatrix.add_constraint(lambda v:\
        v.get_value('table_format').file_format in ['parquet'])

    if cls.exploration_strategy() != 'exhaustive':
      # Cut down on execution time when not running in exhaustive mode.
      cls.TestMatrix.add_constraint(lambda v: v.get_value('batch_size') != 1)

  @classmethod
  def setup_class(cls):
    super(TestSemiJoinQueries, cls).setup_class()
    cls.__cleanup_semi_join_tables()
    cls.__load_semi_join_tables()

  @classmethod
  def teardown_class(cls):
    cls.__cleanup_semi_join_tables()
    super(TestSemiJoinQueries, cls).teardown_class()

  @classmethod
  def __load_semi_join_tables(cls):
    SEMIJOIN_TABLES = ['functional.SemiJoinTblA', 'functional.SemiJoinTblB']
    # Cleanup, create and load fresh test tables for semi/anti-join tests
    cls.client.execute('create table if not exists '\
                          'functional.SemiJoinTblA(a int, b int, c int)')
    cls.client.execute('create table if not exists '\
                          'functional.SemiJoinTblB(a int, b int, c int)')
    # loads some values with NULLs in the first table
    cls.client.execute('insert into %s values(1,1,1)' % SEMIJOIN_TABLES[0]);
    cls.client.execute('insert into %s values(1,1,10)' % SEMIJOIN_TABLES[0]);
    cls.client.execute('insert into %s values(1,2,10)' % SEMIJOIN_TABLES[0]);
    cls.client.execute('insert into %s values(1,3,10)' % SEMIJOIN_TABLES[0]);
    cls.client.execute('insert into %s values(NULL,NULL,30)'  % SEMIJOIN_TABLES[0]);
    cls.client.execute('insert into %s values(2,4,30)' % SEMIJOIN_TABLES[0]);
    cls.client.execute('insert into %s values(2,NULL,20)' % SEMIJOIN_TABLES[0]);
    # loads some values with NULLs in the second table
    cls.client.execute('insert into %s values(1,1,1)' % SEMIJOIN_TABLES[1]);
    cls.client.execute('insert into %s values(1,1,10)' % SEMIJOIN_TABLES[1]);
    cls.client.execute('insert into %s values(1,2,5)' % SEMIJOIN_TABLES[1]);
    cls.client.execute('insert into %s values(1,NULL,10)' % SEMIJOIN_TABLES[1]);
    cls.client.execute('insert into %s values(2,10,NULL)' % SEMIJOIN_TABLES[1]);
    cls.client.execute('insert into %s values(3,NULL,NULL)' % SEMIJOIN_TABLES[1]);
    cls.client.execute('insert into %s values(3,NULL,50)' % SEMIJOIN_TABLES[1]);

  @classmethod
  def __cleanup_semi_join_tables(cls):
    cls.client.execute('drop table if exists functional.SemiJoinTblA')
    cls.client.execute('drop table if exists functional.SemiJoinTblB')

  @pytest.mark.execute_serially
  def test_semi_joins(self, vector):
    new_vector = copy(vector)
    new_vector.get_value('exec_option')['batch_size'] = vector.get_value('batch_size')
    self.run_test_case('QueryTest/semi-joins', new_vector)
