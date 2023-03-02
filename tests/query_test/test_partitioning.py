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
import pytest

from tests.beeswax.impala_beeswax import ImpalaBeeswaxException
from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.skip import SkipIfFS, SkipIfLocal
from tests.common.test_dimensions import create_single_exec_option_dimension

# Tests to validate HDFS partitioning.
class TestPartitioning(ImpalaTestSuite):
  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestPartitioning, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(create_single_exec_option_dimension())

    # There is no reason to run these tests using all dimensions.
    cls.ImpalaTestMatrix.add_constraint(lambda v:\
        v.get_value('table_format').file_format == 'text' and\
        v.get_value('table_format').compression_codec == 'none')

  @SkipIfLocal.root_path
  def test_partition_col_types(self, vector, unique_database):
    self.run_test_case('QueryTest/partition-col-types', vector,
        use_db=unique_database)

  # Missing Coverage: Impala deals with boolean partitions created by Hive on a non-hdfs
  # filesystem.
  @SkipIfFS.hive
  def test_boolean_partitions(self, vector, unique_database):
    # This test takes about a minute to complete due to the Hive commands that are
    # executed. To cut down on runtime, limit the test to exhaustive exploration
    # strategy.
    if self.exploration_strategy() != 'exhaustive': pytest.skip()

    full_name = unique_database + ".bool_test"

    self.execute_query("create table %s (i int) partitioned by (b boolean)" % full_name)

    # Insert some data using Hive. Due to HIVE-6590, Hive 1 may create multiple
    # partitions, mapping to the same boolean literal value.
    # For example, Hive may create partitions: /b=FALSE and /b=false, etc.
    # This particular issue was fixed in Hive 2 though HIVE-6590 has not
    # been resolved as of release 2.3.1, though it has been resolved in Hive master.
    self.run_stmt_in_hive("INSERT INTO TABLE %s PARTITION(b=false) SELECT 1 from "\
        "functional.alltypes limit 1" % full_name)
    self.run_stmt_in_hive("INSERT INTO TABLE %s PARTITION(b=FALSE) SELECT 2 from "\
        "functional.alltypes limit 1" % full_name)
    self.run_stmt_in_hive("INSERT INTO TABLE %s PARTITION(b=true) SELECT 10 from "\
        "functional.alltypes limit 1" % full_name)

    # Update the Impala metadata
    self.execute_query("invalidate metadata " + full_name)

    # List the partitions. Show table stats returns 1 row for each partition + 1 summary
    # row
    result = self.execute_query("show table stats %s" % full_name)
    assert len(result.data) == 2 + 1

    # Verify Impala properly merges the results of the Hive metadata,
    # whether it be good (Hive 2) or bad (Hive 1).
    assert '13' == self.execute_scalar("select sum(i) from %s" % full_name);
    assert '10' == self.execute_scalar("select sum(i) from %s where b=true" % full_name)
    assert '3' == self.execute_scalar("select sum(i) from %s where b=false" % full_name)

    # INSERT into a boolean column is disabled in Impala due to this Hive bug.
    try:
      self.execute_query("insert into %s partition(bool_col=true) select 1" % full_name)
    except ImpalaBeeswaxException as e:
      assert 'AnalysisException: INSERT into table with BOOLEAN partition column (%s) '\
          'is not supported: %s' % ('b', full_name) in str(e)

