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

# Tests queries with the MT_DOP query option.

from __future__ import absolute_import, division, print_function
import pytest
import logging

from copy import deepcopy
from tests.common.environ import ImpalaTestClusterProperties, build_flavor_timeout
from tests.common.environ import HIVE_MAJOR_VERSION
from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.kudu_test_suite import KuduTestSuite
from tests.common.skip import SkipIfFS, SkipIfEC, SkipIfNotHdfsMinicluster
from tests.common.test_vector import ImpalaTestDimension
from tests.util.filesystem_utils import IS_HDFS

LOG = logging.getLogger('test_mt_dop')

WAIT_TIME_MS = build_flavor_timeout(60000, slow_build_timeout=100000)

# COMPUTE STATS on Parquet tables automatically sets MT_DOP=4, so include
# the value 0 to cover the non-MT path as well.
MT_DOP_VALUES = [0, 1, 2, 8]


class TestMtDop(ImpalaTestSuite):
  @classmethod
  def add_test_dimensions(cls):
    super(TestMtDop, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(ImpalaTestDimension('mt_dop', *MT_DOP_VALUES))

  @classmethod
  def get_workload(cls):
    return 'functional-query'

  def test_mt_dop(self, vector):
    new_vector = deepcopy(vector)
    new_vector.get_value('exec_option')['mt_dop'] = vector.get_value('mt_dop')
    del new_vector.get_value('exec_option')['batch_size']  # .test file sets batch_size
    self.run_test_case('QueryTest/mt-dop', new_vector)

  def test_compute_stats(self, vector, unique_database):
    vector.get_value('exec_option')['mt_dop'] = vector.get_value('mt_dop')
    file_format = vector.get_value('table_format').file_format
    fq_table_name = "%s.mt_dop" % unique_database

    # Different formats need different DDL for creating a test table, and also
    # have different expected results for compute stats.
    expected_results = None
    if file_format == 'kudu':
      # CREATE TABLE LIKE is currently not supported for Kudu tables.
      self.execute_query("create external table %s stored as kudu "
        "tblproperties('kudu.table_name'='impala::functional_kudu.alltypes')"
        % fq_table_name)
      expected_results = "Updated 1 partition(s) and 13 column(s)."
    elif file_format == 'hbase':
      self.execute_query(
        "create external table %s like functional_hbase.alltypes" % fq_table_name)
      expected_results = "Updated 1 partition(s) and 13 column(s)."
    elif HIVE_MAJOR_VERSION == 3 and file_format == 'orc':
      # TODO: Enable this test on non-HDFS filesystems once IMPALA-9365 is resolved.
      if not IS_HDFS: pytest.skip()
      self.run_stmt_in_hive(
          "create table %s like functional_orc_def.alltypes" % fq_table_name)
      self.run_stmt_in_hive(
          "insert into %s select * from functional_orc_def.alltypes" % fq_table_name)
      self.execute_query_using_client(self.client,
          "invalidate metadata %s" % fq_table_name, vector)
      expected_results = "Updated 24 partition(s) and 11 column(s)."
    else:
      # Create a second table in the same format pointing to the same data files.
      # This function switches to the format-specific DB in 'vector'.
      table_loc = self._get_table_location("alltypes", vector)
      self.execute_query_using_client(self.client,
        "create external table %s like alltypes location '%s'"
        % (fq_table_name, table_loc), vector)
      # Recover partitions for HDFS tables.
      self.execute_query("alter table %s recover partitions" % fq_table_name)
      expected_results = "Updated 24 partition(s) and 11 column(s)."

    results = self.execute_query("compute stats %s" % fq_table_name,
      vector.get_value('exec_option'))
    assert expected_results in results.data


class TestMtDopParquet(ImpalaTestSuite):
  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestMtDopParquet, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(ImpalaTestDimension('mt_dop', *MT_DOP_VALUES))
    cls.ImpalaTestMatrix.add_constraint(
        lambda v: v.get_value('table_format').file_format == 'parquet')

  def test_parquet(self, vector):
    vector.get_value('exec_option')['mt_dop'] = vector.get_value('mt_dop')
    self.run_test_case('QueryTest/mt-dop-parquet', vector)

  @pytest.mark.xfail(ImpalaTestClusterProperties.get_instance().is_remote_cluster(),
                     reason='IMPALA-4641')
  def test_parquet_nested(self, vector):
    vector.get_value('exec_option')['mt_dop'] = vector.get_value('mt_dop')
    self.run_test_case('QueryTest/mt-dop-parquet-nested', vector)

  @SkipIfEC.parquet_file_size
  def test_parquet_filtering(self, vector):
    """IMPALA-4624: Test that dictionary filtering eliminates row groups correctly."""
    vector.get_value('exec_option')['mt_dop'] = vector.get_value('mt_dop')
    # Disable min-max stats to prevent interference with directionary filtering.
    vector.get_value('exec_option')['parquet_read_statistics'] = '0'
    self.run_test_case('QueryTest/parquet-filtering', vector)

  @pytest.mark.execute_serially
  @SkipIfFS.file_or_folder_name_ends_with_period
  def test_mt_dop_insert(self, vector, unique_database):
    """Basic tests for inserts with mt_dop > 0"""
    mt_dop = vector.get_value('mt_dop')
    if mt_dop == 0:
      pytest.skip("Non-mt inserts tested elsewhere")
    self.run_test_case('QueryTest/insert', vector, unique_database,
        test_file_vars={'$ORIGINAL_DB': ImpalaTestSuite
        .get_db_name_from_format(vector.get_value('table_format'))})

  def test_mt_dop_only_joins(self, vector, unique_database):
    """MT_DOP specific tests for joins."""
    mt_dop = vector.get_value('mt_dop')
    if mt_dop == 0:
      pytest.skip("Test requires mt_dop > 0")
    vector = deepcopy(vector)
    # Allow test to override num_nodes.
    del vector.get_value('exec_option')['num_nodes']
    self.run_test_case('QueryTest/joins_mt_dop', vector,
       test_file_vars={'$RUNTIME_FILTER_WAIT_TIME_MS': str(WAIT_TIME_MS)})


class TestMtDopKudu(KuduTestSuite):
  @classmethod
  def add_test_dimensions(cls):
    super(TestMtDopKudu, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(ImpalaTestDimension('mt_dop', *MT_DOP_VALUES))

  def test_kudu(self, vector, unique_database):
    vector.get_value('exec_option')['mt_dop'] = vector.get_value('mt_dop')
    self.run_test_case('QueryTest/mt-dop-kudu', vector, use_db=unique_database)


@SkipIfNotHdfsMinicluster.tuned_for_minicluster
class TestMtDopScheduling(ImpalaTestSuite):
  """Test the number of fragment instances and admission slots computed by the scheduler
  for different queries. This is always at most mt_dop per host, but is less where there
  are fewer fragment instances per host. The mt_dop scheduling and slot calculation is
  orthogonal to file format, so we only need to test on one format."""
  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestMtDopScheduling, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(ImpalaTestDimension('mt_dop', 4))
    cls.ImpalaTestMatrix.add_constraint(
        lambda v: v.get_value('table_format').file_format == 'parquet')

  def test_scheduling(self, vector):
    vector.get_value('exec_option')['mt_dop'] = vector.get_value('mt_dop')
    self.run_test_case('QueryTest/mt-dop-parquet-scheduling', vector)
