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
#
# Impala tests for column statistics

import logging
import pytest
import shlex
from tests.common.test_result_verifier import *
from subprocess import call
from tests.common.test_vector import *
from tests.common.impala_test_suite import *

TEST_DB = 'colstats_test_db'

# End-to-end validation of Impala column stats usage.
class TestColStats(ImpalaTestSuite):
  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestColStats, cls).add_test_dimensions()
    # There is no reason to run these tests using all dimensions.
    cls.TestMatrix.add_dimension(create_single_exec_option_dimension())
    cls.TestMatrix.add_dimension(create_uncompressed_text_dimension(cls.get_workload()))

  def setup_method(self, method):
    self.__cleanup()

  def teardown_method(self, method):
    self.__cleanup()

  def __cleanup(self):
    self.cleanup_db(TEST_DB)

  def test_incompatible_col_stats(self, vector):
    """Tests Impala is able to use tables when the column stats data is not compatible
    with the column type. Regression test for IMPALA-588."""

    # Create a test database.
    self.client.execute("create database " + TEST_DB);
    self.client.execute("use " + TEST_DB)

    # Create a table with a string column and populate it with some data.
    self.client.execute("create table badstats(s string)")
    self.client.execute("insert into table badstats select cast(int_col as string) "\
        "from functional.alltypes limit 10")

    # Compute stats for this table, they will be for the string column type.
    self.__compute_table_stats(TEST_DB, 'badstats')
    self.client.execute("refresh badstats")

    # Change the column type to int which will cause a mismatch between the column
    # stats data and the column type metadata.
    self.client.execute("alter table badstats change s s int")
    # Should still be able to query the table
    result = self.client.execute("select s from badstats")
    assert len(result.data) == 10

    # Recompute stats with the new column type. Impala should now have stats for this
    # column and should be able to access the table.
    # TODO: Currently this just verifies Impala can query the table, it does not
    # verify the stats are there or correct. Expand the verification once Impala has a
    # mechanism to expose this metadata.
    self.__compute_table_stats(TEST_DB, 'badstats')
    self.client.execute("refresh badstats")
    result = self.client.execute("select s from badstats")
    assert len(result.data) == 10

  def __compute_table_stats(self, db_name, table_name):
    compute_stats_script =\
        os.path.join(os.environ['IMPALA_HOME'],'tests/util/compute_table_stats.py')
    rval = call([compute_stats_script,
      '--db_names=' + db_name, '--table_names=' + table_name])
    assert rval == 0, 'Compute table stats failed on: %s.%s' % (db_name, table_name)
