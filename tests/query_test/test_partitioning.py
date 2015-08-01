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
import logging
import pytest
import shlex
import time
from tests.common.test_result_verifier import *
from subprocess import call
from tests.beeswax.impala_beeswax import ImpalaBeeswaxException
from tests.common.test_vector import *
from tests.common.test_dimensions import ALL_NODES_ONLY
from tests.common.impala_test_suite import *
from tests.common.skip import SkipIfS3, SkipIfIsilon

# Tests to validate HDFS partitioning.
class TestPartitioning(ImpalaTestSuite):
  TEST_DBS = ['hdfs_partitioning', 'bool_partitions']

  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestPartitioning, cls).add_test_dimensions()
    cls.TestMatrix.add_dimension(create_single_exec_option_dimension())

    # There is no reason to run these tests using all dimensions.
    cls.TestMatrix.add_constraint(lambda v:\
        v.get_value('table_format').file_format == 'text' and\
        v.get_value('table_format').compression_codec == 'none')

  @classmethod
  def setup_class(cls):
    super(TestPartitioning, cls).setup_class()
    map(cls.cleanup_db, cls.TEST_DBS)
    cls.hdfs_client.delete_file_dir("test-warehouse/all_insert_partition_col_types/",\
        recursive=True)

  @classmethod
  def teardown_class(cls):
    map(cls.cleanup_db, cls.TEST_DBS)
    super(TestPartitioning, cls).teardown_class()

  @pytest.mark.execute_serially
  def test_partition_col_types(self, vector):
    self.execute_query("create database hdfs_partitioning");
    self.run_test_case('QueryTest/partition-col-types', vector,
        use_db='hdfs_partitioning')

  # Missing Coverage: Impala deals with boolean partitions created by Hive on a non-hdfs
  # filesystem.
  @SkipIfS3.hive
  @SkipIfIsilon.hive
  @pytest.mark.execute_serially
  def test_boolean_partitions(self, vector):
    # This test takes about a minute to complete due to the Hive commands that are
    # executed. To cut down on runtime, limit the test to exhaustive exploration
    # strategy.
    if self.exploration_strategy() != 'exhaustive': pytest.skip()

    db_name = 'bool_partitions'
    tbl_name = 'tbl'
    self.execute_query("create database " + db_name)
    self.execute_query("use " + db_name)

    self.execute_query("create table %s (i int) partitioned by (b boolean)" % tbl_name)

    # Insert some data using Hive. Due to HIVE-6590, Hive may create multiple
    # partitions, mapping to the same boolean literal value.
    # For example, Hive may create partitions: /b=FALSE and /b=false, etc
    call(["hive", "-e", "INSERT OVERWRITE TABLE %s.%s PARTITION(b=false) SELECT 1 from "\
        "functional.alltypes limit 1" % (db_name, tbl_name)])
    call(["hive", "-e", "INSERT OVERWRITE TABLE %s.%s PARTITION(b=FALSE) SELECT 2 from "\
        "functional.alltypes limit 1" % (db_name, tbl_name)])
    call(["hive", "-e", "INSERT OVERWRITE TABLE %s.%s PARTITION(b=true) SELECT 10 from "\
        "functional.alltypes limit 1" % (db_name, tbl_name)])

    # Update the Impala metadata
    self.execute_query("refresh " + tbl_name)

    # List the partitions. Show table stats returns 1 row for each partition + 1 summary
    # row
    result = self.execute_query("show table stats %s" % tbl_name)
    assert len(result.data) == 3 + 1

    # Verify Impala properly merges the results of the bad Hive metadata.
    assert '13' == self.execute_scalar("select sum(i) from %s" % tbl_name);
    assert '10' == self.execute_scalar("select sum(i) from %s where b=true" % tbl_name)
    assert '3' == self.execute_scalar("select sum(i) from %s where b=false" % tbl_name)

    # INSERT into a boolean column is disabled in Impala due to this Hive bug.
    try:
      self.execute_query("insert into %s partition(bool_col=true) select 1" % tbl_name)
    except ImpalaBeeswaxException, e:
      assert 'AnalysisException: INSERT into table with BOOLEAN partition column (%s) '\
          'is not supported: %s.%s' % ('b', db_name, tbl_name) in str(e)

