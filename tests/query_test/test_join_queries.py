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
import pytest
from copy import deepcopy

from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.skip import (
    SkipIf,
    SkipIfIsilon,
    SkipIfLocal,
    SkipIfS3,
    SkipIfABFS,
    SkipIfADLS)
from tests.common.test_vector import ImpalaTestDimension

class TestJoinQueries(ImpalaTestSuite):
  BATCH_SIZES = [0, 1]

  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestJoinQueries, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(
        ImpalaTestDimension('batch_size', *TestJoinQueries.BATCH_SIZES))
    # TODO: Look into splitting up join tests to accomodate hbase.
    # Joins with hbase tables produce drastically different results.
    cls.ImpalaTestMatrix.add_constraint(lambda v:\
        v.get_value('table_format').file_format in ['parquet'])

    if cls.exploration_strategy() != 'exhaustive':
      # Cut down on execution time when not running in exhaustive mode.
      cls.ImpalaTestMatrix.add_constraint(lambda v: v.get_value('batch_size') != 1)

  def test_basic_joins(self, vector):
    new_vector = deepcopy(vector)
    new_vector.get_value('exec_option')['batch_size'] = vector.get_value('batch_size')
    self.run_test_case('QueryTest/joins', new_vector)

  def test_single_node_joins_with_limits_exhaustive(self, vector):
    if self.exploration_strategy() != 'exhaustive': pytest.skip()
    new_vector = deepcopy(vector)
    new_vector.get_value('exec_option')['num_nodes'] = 1
    del new_vector.get_value('exec_option')['batch_size']  # .test file sets batch_size
    self.run_test_case('QueryTest/single-node-joins-with-limits-exhaustive', new_vector)

  @SkipIfS3.hbase
  @SkipIfABFS.hbase
  @SkipIfADLS.hbase
  @SkipIfIsilon.hbase
  @SkipIf.skip_hbase
  @SkipIfLocal.hbase
  def test_joins_against_hbase(self, vector):
    new_vector = deepcopy(vector)
    new_vector.get_value('exec_option')['batch_size'] = vector.get_value('batch_size')
    self.run_test_case('QueryTest/joins-against-hbase', new_vector)

  def test_outer_joins(self, vector):
    new_vector = deepcopy(vector)
    new_vector.get_value('exec_option')['batch_size'] = vector.get_value('batch_size')
    self.run_test_case('QueryTest/outer-joins', new_vector)

  def test_single_node_nested_loop_joins(self, vector):
    # Test the execution of nested-loops joins for join types that can only be
    # executed in a single node (right [outer|semi|anti] and full outer joins).
    new_vector = deepcopy(vector)
    new_vector.get_value('exec_option')['num_nodes'] = 1
    self.run_test_case('QueryTest/single-node-nlj', new_vector)

  def test_single_node_nested_loop_joins_exhaustive(self, vector):
    if self.exploration_strategy() != 'exhaustive': pytest.skip()
    new_vector = deepcopy(vector)
    new_vector.get_value('exec_option')['num_nodes'] = 1
    self.run_test_case('QueryTest/single-node-nlj-exhaustive', new_vector)

  def test_empty_build_joins(self, vector):
    new_vector = deepcopy(vector)
    new_vector.get_value('exec_option')['batch_size'] = vector.get_value('batch_size')
    self.run_test_case('QueryTest/empty-build-joins', new_vector)

class TestTPCHJoinQueries(ImpalaTestSuite):
  # Uses the TPC-H dataset in order to have larger joins. Needed for example to test
  # the repartitioning codepaths.

  @classmethod
  def get_workload(cls):
    return 'tpch'

  @classmethod
  def add_test_dimensions(cls):
    super(TestTPCHJoinQueries, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(
        ImpalaTestDimension('batch_size', *TestJoinQueries.BATCH_SIZES))
    cls.ImpalaTestMatrix.add_constraint(lambda v:\
        v.get_value('table_format').file_format in ['parquet'])

    if cls.exploration_strategy() != 'exhaustive':
      # Cut down on execution time when not running in exhaustive mode.
      cls.ImpalaTestMatrix.add_constraint(lambda v: v.get_value('batch_size') != 1)

  @classmethod
  def teardown_class(cls):
    cls.client.execute('set mem_limit = 0');
    super(TestTPCHJoinQueries, cls).teardown_class()

  def test_outer_joins(self, vector):
    new_vector = deepcopy(vector)
    new_vector.get_value('exec_option')['batch_size'] = vector.get_value('batch_size')
    self.run_test_case('tpch-outer-joins', new_vector)

class TestSemiJoinQueries(ImpalaTestSuite):
  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestSemiJoinQueries, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(
        ImpalaTestDimension('batch_size', *TestJoinQueries.BATCH_SIZES))
    # Joins with hbase tables produce drastically different results.
    cls.ImpalaTestMatrix.add_constraint(lambda v:\
        v.get_value('table_format').file_format in ['parquet'])

    if cls.exploration_strategy() != 'exhaustive':
      # Cut down on execution time when not running in exhaustive mode.
      cls.ImpalaTestMatrix.add_constraint(lambda v: v.get_value('batch_size') != 1)

  def __load_semi_join_tables(self, db_name):
    # Create and load fresh test tables for semi/anti-join tests
    fq_tbl_name_a = '%s.SemiJoinTblA' % db_name
    self.client.execute('create table %s (a int, b int, c int)' % fq_tbl_name_a)
    self.client.execute('insert into %s values(1,1,1)' % fq_tbl_name_a);
    self.client.execute('insert into %s values(1,1,10)' % fq_tbl_name_a);
    self.client.execute('insert into %s values(1,2,10)' % fq_tbl_name_a);
    self.client.execute('insert into %s values(1,3,10)' % fq_tbl_name_a);
    self.client.execute('insert into %s values(NULL,NULL,30)'  % fq_tbl_name_a);
    self.client.execute('insert into %s values(2,4,30)' % fq_tbl_name_a);
    self.client.execute('insert into %s values(2,NULL,20)' % fq_tbl_name_a);

    fq_tbl_name_b = '%s.SemiJoinTblB' % db_name
    self.client.execute('create table %s (a int, b int, c int)' % fq_tbl_name_b)
    self.client.execute('insert into %s values(1,1,1)' % fq_tbl_name_b);
    self.client.execute('insert into %s values(1,1,10)' % fq_tbl_name_b);
    self.client.execute('insert into %s values(1,2,5)' % fq_tbl_name_b);
    self.client.execute('insert into %s values(1,NULL,10)' % fq_tbl_name_b);
    self.client.execute('insert into %s values(2,10,NULL)' % fq_tbl_name_b);
    self.client.execute('insert into %s values(3,NULL,NULL)' % fq_tbl_name_b);
    self.client.execute('insert into %s values(3,NULL,50)' % fq_tbl_name_b);

  def test_semi_joins(self, vector, unique_database):
    new_vector = deepcopy(vector)
    new_vector.get_value('exec_option')['batch_size'] = vector.get_value('batch_size')
    self.__load_semi_join_tables(unique_database)
    self.run_test_case('QueryTest/semi-joins', new_vector, unique_database)

  @pytest.mark.execute_serially
  def test_semi_joins_exhaustive(self, vector):
    """Expensive and memory-intensive semi-join tests."""
    if self.exploration_strategy() != 'exhaustive': pytest.skip()
    self.run_test_case('QueryTest/semi-joins-exhaustive', vector)
