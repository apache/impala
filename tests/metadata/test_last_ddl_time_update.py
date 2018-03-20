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

# Impala tests for DDL statements
import time

from tests.common.impala_test_suite import ImpalaTestSuite
from tests.util.filesystem_utils import WAREHOUSE, IS_S3

# Checks different statements' effect on last DDL time and last compute stats time.
class TestLastDdlTimeUpdate(ImpalaTestSuite):

  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestLastDdlTimeUpdate, cls).add_test_dimensions()
    # There is no reason to run these tests using all dimensions.
    cls.ImpalaTestMatrix.add_constraint(lambda v:\
        v.get_value('table_format').file_format == 'text' and\
        v.get_value('table_format').compression_codec == 'none')

    if cls.exploration_strategy() == 'core' and not IS_S3:
      # Don't run on core.  This test is very slow and we are unlikely
      # to regress here.
      cls.ImpalaTestMatrix.add_constraint(lambda v: False)

  # Convenience class to make calls to TestLastDdlTimeUpdate.run_test() shorter by
  # storing common arguments as members and substituting table name and HDFS warehouse
  # path to the query string.
  class TestHelper:
    def __init__(self, test_suite, db_name, tbl_name):
      self.test_suite = test_suite
      self.db_name = db_name
      self.tbl_name = tbl_name
      self.fq_tbl_name = "%s.%s" % (self.db_name, self.tbl_name)

    def expect_no_time_change(self, query):
      """Neither transient_lastDdlTime or impala.lastComputeStatsTime should be
      changed by running the query.
      The following strings are substituted in the query: "%(TBL)s" and "%(WAREHOUSE)s"
      """
      self.run_test(query, False, False)

    def expect_ddl_time_change(self, query):
      """Running the query should increase transient_lastDdlTime but
      not impala.lastComputeStatsTime.
      The following strings are substituted in the query: "%(TBL)s" and "%(WAREHOUSE)s"
      """
      self.run_test(query, True, False)

    def expect_stat_time_change(self, query):
      """Running the query should increase impala.lastComputeStatsTime but
      not transient_lastDdlTime.
      The following strings are substituted in the query: "%(TBL)s" and "%(WAREHOUSE)s"
      """
      self.run_test(query, False, True)

    def run_test(self, query, expect_changed_ddl_time, expect_changed_stats_time):
      """
      Runs the query and compares the last ddl/compute stats time before and after
      executing the query. If expect_changed_ddl_time or expect_changed_stat_time
      is true, then we expect the given table property to be increased, otherwise we
      expect it to be unchanged.
      The following strings are substituted in the query: "%(TBL)s" and "%(WAREHOUSE)s"
      """

      HIVE_LAST_DDL_TIME_PARAM_KEY = "transient_lastDdlTime"
      LAST_COMPUTE_STATS_TIME_KEY = "impala.lastComputeStatsTime"

      # Get timestamps before executing query.
      table = self.test_suite.hive_client.get_table(self.db_name, self.tbl_name)
      assert table is not None
      beforeDdlTime = table.parameters[HIVE_LAST_DDL_TIME_PARAM_KEY]
      beforeStatsTime = table.parameters[LAST_COMPUTE_STATS_TIME_KEY]
      # HMS uses a seconds granularity on the last ddl time - sleeping 1100 ms should be
      # enough to ensure that the new timestamps are strictly greater than the old ones.
      time.sleep (1.1)

      self.test_suite.execute_query(query %
        {'TBL': self.fq_tbl_name, 'WAREHOUSE': WAREHOUSE})

      # Get timestamps after executing query.
      table = self.test_suite.hive_client.get_table(self.db_name, self.tbl_name)
      afterDdlTime = table.parameters[HIVE_LAST_DDL_TIME_PARAM_KEY]
      afterStatsTime = table.parameters[LAST_COMPUTE_STATS_TIME_KEY]

      if expect_changed_ddl_time:
        # check that the new ddlTime is strictly greater than the old one.
        assert long(afterDdlTime) > long(beforeDdlTime)
      else:
        assert long(afterDdlTime) == long(beforeDdlTime)

      if expect_changed_stats_time:
        # check that the new statsTime is strictly greater than the old one.
        assert long(afterStatsTime) > long(beforeStatsTime)
      else:
        assert long(afterStatsTime) == long(beforeStatsTime)

  def test_alter(self, vector, unique_database):
    TBL_NAME = "alter_test_tbl"
    FQ_TBL_NAME = unique_database + "." + TBL_NAME

    self.execute_query("create external table %s (i int) "
                       "partitioned by (j int, s string)" % FQ_TBL_NAME)

    # compute statistics to fill table property impala.lastComputeStatsTime
    self.execute_query("compute stats %s" % FQ_TBL_NAME)

    h = TestLastDdlTimeUpdate.TestHelper(self, unique_database, TBL_NAME)

    # add/drop partitions
    h.expect_no_time_change("alter table %(TBL)s add partition (j=1, s='2012')")
    h.expect_no_time_change("alter table %(TBL)s add if not exists "
                            "partition (j=1, s='2012')")
    h.expect_no_time_change("alter table %(TBL)s drop partition (j=1, s='2012')")
    h.expect_no_time_change("alter table %(TBL)s drop if exists "
                            "partition (j=2, s='2012')")
    # change location of table
    h.expect_ddl_time_change("alter table %(TBL)s set location '%(WAREHOUSE)s'")
    # change format of table
    h.expect_ddl_time_change("alter table %(TBL)s set fileformat textfile")

    self.run_common_test_cases(h)

    # prepare for incremental statistics tests
    self.execute_query("drop stats %s" % FQ_TBL_NAME)
    self.execute_query("alter table %s add partition (j=1, s='2012')" % FQ_TBL_NAME)

    # compute incremental statistics
    h.expect_stat_time_change("compute incremental stats %(TBL)s")
    h.expect_stat_time_change("compute incremental stats %(TBL)s "
                              "partition (j=1, s='2012')")

    # drop incremental statistics
    h.expect_no_time_change("drop incremental stats %(TBL)s partition (j=1, s='2012')")

    # prepare for table sample statistics tests
    self.execute_query(
        "alter table %s set tblproperties ('impala.enable.stats.extrapolation'='true')"
        % FQ_TBL_NAME)

    # compute sampled statistics
    h.expect_stat_time_change("compute stats %(TBL)s tablesample system(20)")


  def test_insert(self, vector, unique_database):
    TBL_NAME = "insert_test_tbl"
    FQ_TBL_NAME = unique_database + "." + TBL_NAME
    self.execute_query("create external table %s (i int) "
                       "partitioned by (j int, s string)" % FQ_TBL_NAME)

    # initialize compute stats time
    self.execute_query("compute stats %s" % FQ_TBL_NAME)

    h = TestLastDdlTimeUpdate.TestHelper(self, unique_database, TBL_NAME)

    # static partition insert
    h.expect_no_time_change("insert into %(TBL)s partition(j=1, s='2012') select 10")
    # dynamic partition insert
    h.expect_no_time_change("insert into %(TBL)s partition(j, s) select 10, 2, '2013'")

    # dynamic partition insert changing no partitions (empty input)
    h.expect_no_time_change("insert into %(TBL)s partition(j, s) "
                            "select * from (select 10 as i, 2 as j, '2013' as s) as t "
                            "where t.i < 10")
    # dynamic partition insert modifying an existing partition
    h.expect_no_time_change("insert into %(TBL)s partition(j, s) select 20, 1, '2012'")

  def test_kudu(self, vector, unique_database):
    TBL_NAME = "kudu_test_tbl"
    FQ_TBL_NAME = unique_database + "." + TBL_NAME
    self.execute_query("create table %s (i int primary key) "
                       "partition by hash(i) partitions 3 stored as kudu" % FQ_TBL_NAME)

    # initialize last compute stats time
    self.execute_query("compute stats %s" % FQ_TBL_NAME)

    h = TestLastDdlTimeUpdate.TestHelper(self, unique_database, TBL_NAME)

    # insert
    h.expect_no_time_change("insert into %s values (1)" % FQ_TBL_NAME)

    self.run_common_test_cases(h)

  # Tests that should behave the same with HDFS and Kudu tables.
  def run_common_test_cases(self, test_helper):
    h = test_helper
    # rename columns
    h.expect_ddl_time_change("alter table %(TBL)s change column i k int")
    h.expect_ddl_time_change("alter table %(TBL)s change column k i int")
    # change table property
    h.expect_ddl_time_change("alter table %(TBL)s set tblproperties ('a'='b')")

    # changing table statistics manually
    h.expect_ddl_time_change("alter table %(TBL)s set tblproperties ('numRows'='1')")

    # changing column statistics manually
    h.expect_no_time_change("alter table %(TBL)s set column stats i ('numdvs'='3')")

    # compute statistics
    h.expect_stat_time_change("compute stats %(TBL)s")
    # compute statistics for a single column
    h.expect_stat_time_change("compute stats %(TBL)s (i)")

    # drop statistics
    h.expect_no_time_change("drop stats %(TBL)s")

    # invalidate metadata and reload table
    self.execute_query("invalidate metadata %s" % h.fq_tbl_name)
    # run any query to reload the table
    h.expect_no_time_change("describe %(TBL)s")
