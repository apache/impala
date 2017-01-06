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

import pytest
from subprocess import check_call

from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.skip import SkipIfS3, SkipIfIsilon, SkipIfLocal
from tests.common.test_dimensions import (
    create_exec_option_dimension,
    create_single_exec_option_dimension,
    create_uncompressed_text_dimension)

# Tests the COMPUTE STATS command for gathering table and column stats.
class TestComputeStats(ImpalaTestSuite):
  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestComputeStats, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(create_single_exec_option_dimension())
    # Do not run these tests using all dimensions because the expected results
    # are different for different file formats.
    cls.ImpalaTestMatrix.add_dimension(
        create_uncompressed_text_dimension(cls.get_workload()))

  @SkipIfLocal.hdfs_blocks
  def test_compute_stats(self, vector, unique_database):
    self.run_test_case('QueryTest/compute-stats', vector, unique_database)
    # Test compute stats on decimal columns separately so we can vary between platforms
    # with and without write support for decimals (Hive < 0.11 and >= 0.11).
    self.run_test_case('QueryTest/compute-stats-decimal', vector, unique_database)

  def test_compute_stats_incremental(self, vector, unique_database):
    self.run_test_case('QueryTest/compute-stats-incremental', vector, unique_database)

  @pytest.mark.execute_serially
  def test_compute_stats_many_partitions(self, vector):
    # To cut down on test execution time, only run the compute stats test against many
    # partitions if performing an exhaustive test run.
    if self.exploration_strategy() != 'exhaustive': pytest.skip()
    self.run_test_case('QueryTest/compute-stats-many-partitions', vector)

  @pytest.mark.execute_serially
  def test_compute_stats_keywords(self, vector):
    """IMPALA-1055: Tests compute stats with a db/table name that are keywords."""
    self.execute_query("drop database if exists `parquet` cascade")
    self.execute_query("create database `parquet`")
    self.execute_query("create table `parquet`.impala_1055 (id INT)")
    self.execute_query("create table `parquet`.`parquet` (id INT)")
    try:
      self.run_test_case('QueryTest/compute-stats-keywords', vector)
    finally:
      self.cleanup_db("parquet")

  @SkipIfS3.hive
  @SkipIfIsilon.hive
  @SkipIfLocal.hive
  def test_compute_stats_impala_2201(self, vector, unique_database):
    """IMPALA-2201: Tests that the results of compute incremental stats are properly
    persisted when the data was loaded from Hive with hive.stats.autogather=true.
    """

    # Unless something drastic changes in Hive and/or Impala, this test should
    # always succeed.
    if self.exploration_strategy() != 'exhaustive': pytest.skip()

    # Create a table and load data into a single partition with Hive with
    # stats autogathering.
    table_name = "autogather_test"
    create_load_data_stmts = """
      set hive.stats.autogather=true;
      create table {0}.{1} (c int) partitioned by (p1 int, p2 string);
      insert overwrite table {0}.{1} partition (p1=1, p2="pval")
      select id from functional.alltypestiny;
    """.format(unique_database, table_name)
    check_call(["hive", "-e", create_load_data_stmts])

    # Make the table visible in Impala.
    self.execute_query("invalidate metadata %s.%s" % (unique_database, table_name))

    # Check that the row count was populated during the insert. We expect 8 rows
    # because functional.alltypestiny has 8 rows, but Hive's auto stats gathering
    # is known to be flaky and sometimes sets the row count to 0. So we check that
    # the row count is not -1 instead of checking for 8 directly.
    show_result = \
      self.execute_query("show table stats %s.%s" % (unique_database, table_name))
    assert(len(show_result.data) == 2)
    assert("1\tpval\t-1" not in show_result.data[0])

    # Compute incremental stats on the single test partition.
    self.execute_query("compute incremental stats %s.%s partition (p1=1, p2='pval')"
      % (unique_database, table_name))

    # Invalidate metadata to force reloading the stats from the Hive Metastore.
    self.execute_query("invalidate metadata %s.%s" % (unique_database, table_name))

    # Check that the row count is still 8.
    show_result = \
      self.execute_query("show table stats %s.%s" % (unique_database, table_name))
    assert(len(show_result.data) == 2)
    assert("1\tpval\t8" in show_result.data[0])

# Tests compute stats on HBase tables. This test is separate from TestComputeStats,
# because we want to use the existing machanism to disable running tests on hbase/none
# based on the filesystem type (S3, Isilon, etc.).
class TestHbaseComputeStats(ImpalaTestSuite):
  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestHbaseComputeStats, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(create_single_exec_option_dimension())
    cls.ImpalaTestMatrix.add_constraint(
        lambda v: v.get_value('table_format').file_format == 'hbase')

  @pytest.mark.xfail(pytest.config.option.testing_remote_cluster,
                     reason=("Setting up HBase tests currently assumes a local "
                             "mini-cluster. See IMPALA-4661."))
  def test_hbase_compute_stats(self, vector, unique_database):
    self.run_test_case('QueryTest/hbase-compute-stats', vector, unique_database)

  @pytest.mark.xfail(pytest.config.option.testing_remote_cluster,
                     reason=("Setting up HBase tests currently assumes a local "
                             "mini-cluster. See IMPALA-4661."))
  def test_hbase_compute_stats_incremental(self, vector, unique_database):
    self.run_test_case('QueryTest/hbase-compute-stats-incremental', vector,
      unique_database)


class TestCorruptTableStats(ImpalaTestSuite):
  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestCorruptTableStats, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(create_exec_option_dimension(
      disable_codegen_options=[False], exec_single_node_option=[100]))
    cls.ImpalaTestMatrix.add_dimension(
        create_uncompressed_text_dimension(cls.get_workload()))

  def test_corrupt_stats(self, vector, unique_database):
    """IMPALA-1983/IMPALA-1657: Test that in the presence of corrupt table statistics a
    warning is issued and the small query optimization is disabled."""
    if self.exploration_strategy() != 'exhaustive': pytest.skip("Only run in exhaustive")
    self.run_test_case('QueryTest/corrupt-stats', vector, unique_database)


class TestIncompatibleColStats(ImpalaTestSuite):
  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestIncompatibleColStats, cls).add_test_dimensions()
    # There is no reason to run these tests using all dimensions.
    cls.ImpalaTestMatrix.add_dimension(create_single_exec_option_dimension())
    cls.ImpalaTestMatrix.add_dimension(
        create_uncompressed_text_dimension(cls.get_workload()))

  def test_incompatible_col_stats(self, vector, unique_database):
    """Tests Impala is able to use tables when the column stats data is not compatible
    with the column type. Regression test for IMPALA-588."""

    # Create a table with a string column and populate it with some data.
    table_name = unique_database + ".badstats"
    self.client.execute("create table %s (s string)" % table_name)
    self.client.execute("insert into table %s select cast(int_col as string) "
        "from functional.alltypes limit 10" % table_name)

    # Compute stats for this table, they will be for the string column type.
    self.client.execute("compute stats %s" % table_name)

    # Change the column type to int which will cause a mismatch between the column
    # stats data and the column type metadata.
    self.client.execute("alter table %s change s s int" % table_name)
    # Force a reload of the table metadata.
    self.client.execute("invalidate metadata %s" % table_name)
    # Should still be able to load the metadata and query the table.
    result = self.client.execute("select s from %s" % table_name)
    assert len(result.data) == 10

    # Recompute stats with the new column type. Impala should now have stats for this
    # column and should be able to access the table.
    # TODO: Currently this just verifies Impala can query the table, it does not
    # verify the stats are there or correct. Expand the verification once Impala has a
    # mechanism to expose this metadata.
    self.client.execute("compute stats %s" % table_name)
    result = self.client.execute("select s from %s" % table_name)
    assert len(result.data) == 10
