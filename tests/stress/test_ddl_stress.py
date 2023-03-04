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

from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.skip import SkipIfFS

# Number of tables to create per thread
NUM_TBLS_PER_THREAD = 10

# Each client will get a different test id.
TEST_INDICES = range(10)


# Simple stress test for DDL operations. Attempts to create, cache,
# uncache, then drop many different tables in parallel.
class TestDdlStress(ImpalaTestSuite):
  SHARED_DATABASE = 'test_ddl_stress_db'

  @classmethod
  def get_workload(self):
    return 'targeted-stress'

  @classmethod
  def add_test_dimensions(cls):
    super(TestDdlStress, cls).add_test_dimensions()

    if cls.exploration_strategy() != 'exhaustive':
      pytest.skip("Should only run in exhaustive due to long execution time.")

    cls.ImpalaTestMatrix.add_constraint(
        lambda v: (v.get_value('table_format').file_format == 'text' and
                   v.get_value('table_format').compression_codec == 'none'))

  @SkipIfFS.hdfs_caching
  @pytest.mark.stress
  @pytest.mark.parametrize('test_index', TEST_INDICES)
  def test_create_cache_many_tables(self, vector, testid_checksum, test_index):
    self.client.set_configuration(vector.get_value('exec_option'))
    # Don't use unique_database so that we issue several "create database" statements
    # rather simultaneously on the same object.
    self.client.execute("create database if not exists {0}".format(self.SHARED_DATABASE))

    for i in range(NUM_TBLS_PER_THREAD):
      tbl_name = "{db}.test_{checksum}_{i}".format(
          db=self.SHARED_DATABASE,
          checksum=testid_checksum,
          i=i)
      # Because we're using a shared database, first clear potential tables from any
      # previous test runs that failed to properly clean up
      self.client.execute("drop table if exists {0}".format(tbl_name))
      self.client.execute("drop table if exists {0}_part".format(tbl_name))
      # Create a partitioned and unpartitioned table
      self.client.execute("create table %s (i int)" % tbl_name)
      self.client.execute(
          "create table %s_part (i int) partitioned by (j int)" % tbl_name)
      # Add some data to each
      self.client.execute(
          "insert overwrite table %s select int_col from "
          "functional.alltypestiny" % tbl_name)
      self.client.execute(
          "insert overwrite table %s_part partition(j) "
          "values (1, 1), (2, 2), (3, 3), (4, 4), (4, 4)" % tbl_name)

      # Cache the data the unpartitioned table
      self.client.execute("alter table %s set cached in 'testPool'" % tbl_name)
      # Cache, uncache, then re-cache the data in the partitioned table.
      self.client.execute("alter table %s_part set cached in 'testPool'" % tbl_name)
      self.client.execute("alter table %s_part set uncached" % tbl_name)
      self.client.execute("alter table %s_part set cached in 'testPool'" % tbl_name)
      # Drop the tables, this should remove the cache requests.
      self.client.execute("drop table %s" % tbl_name)
      self.client.execute("drop table %s_part" % tbl_name)
