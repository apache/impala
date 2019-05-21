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

import pytest

from copy import deepcopy
from tests.common.environ import ImpalaTestClusterProperties
from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.kudu_test_suite import KuduTestSuite
from tests.common.skip import SkipIfEC
from tests.common.test_vector import ImpalaTestDimension

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

  # Impala scans fewer row groups than it should with erasure coding.
  @SkipIfEC.fix_later
  def test_parquet_filtering(self, vector):
    """IMPALA-4624: Test that dictionary filtering eliminates row groups correctly."""
    vector.get_value('exec_option')['mt_dop'] = vector.get_value('mt_dop')
    # Disable min-max stats to prevent interference with directionary filtering.
    vector.get_value('exec_option')['parquet_read_statistics'] = '0'
    self.run_test_case('QueryTest/parquet-filtering', vector)

class TestMtDopKudu(KuduTestSuite):
  @classmethod
  def add_test_dimensions(cls):
    super(TestMtDopKudu, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(ImpalaTestDimension('mt_dop', *MT_DOP_VALUES))

  def test_kudu(self, vector, unique_database):
    vector.get_value('exec_option')['mt_dop'] = vector.get_value('mt_dop')
    self.run_test_case('QueryTest/mt-dop-kudu', vector, use_db=unique_database)

