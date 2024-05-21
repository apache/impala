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
from hive_metastore.ttypes import (
    ColumnStatistics, ColumnStatisticsDesc, ColumnStatisticsData,
    ColumnStatisticsObj, StringColumnStatsData)

from tests.common.environ import ImpalaTestClusterProperties
from tests.common.impala_cluster import ImpalaCluster
from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.skip import SkipIfFS, SkipIfLocal, SkipIfCatalogV2
from tests.common.test_dimensions import (
    create_exec_option_dimension,
    create_single_exec_option_dimension,
    create_uncompressed_text_dimension)
from CatalogObjects.ttypes import THdfsCompression

import os


IMPALA_TEST_CLUSTER_PROPERTIES = ImpalaTestClusterProperties.get_instance()


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
  @SkipIfFS.incorrent_reported_ec
  def test_compute_stats(self, vector, unique_database):
    self.run_test_case('QueryTest/compute-stats', vector, unique_database)

  @SkipIfLocal.hdfs_blocks
  @SkipIfFS.incorrent_reported_ec
  def test_compute_stats_avro(self, vector, unique_database, cluster_properties):
    if cluster_properties.is_catalog_v2_cluster():
      # IMPALA-7308: changed behaviour of various Avro edge cases significantly in the
      # local catalog - the expected behaviour is different.
      self.run_test_case('QueryTest/compute-stats-avro-catalog-v2', vector,
                         unique_database)
    else:
      self.run_test_case('QueryTest/compute-stats-avro', vector, unique_database)

  @SkipIfLocal.hdfs_blocks
  @SkipIfFS.incorrent_reported_ec
  def test_compute_stats_decimal(self, vector, unique_database):
    # Test compute stats on decimal columns separately so we can vary between platforms
    # with and without write support for decimals (Hive < 0.11 and >= 0.11).
    self.run_test_case('QueryTest/compute-stats-decimal', vector, unique_database)

  @SkipIfLocal.hdfs_blocks
  @SkipIfFS.incorrent_reported_ec
  def test_compute_stats_date(self, vector, unique_database):
    # Test compute stats on date columns separately.
    self.run_test_case('QueryTest/compute-stats-date', vector, unique_database)

  @SkipIfFS.incorrent_reported_ec
  def test_compute_stats_incremental(self, vector, unique_database):
    self.run_test_case('QueryTest/compute-stats-incremental', vector, unique_database)

  def test_compute_stats_complextype_warning(self, vector, unique_database):
    self.run_test_case('QueryTest/compute-stats-complextype-warning', vector,
        unique_database)

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

  def test_compute_stats_compression_codec(self, vector, unique_database):
    """IMPALA-8254: Tests that running compute stats with compression_codec set
    should not throw an error."""
    table = "{0}.codec_tbl".format(unique_database)
    self.execute_query_expect_success(self.client, "create table {0}(i int)"
                                      .format(table))
    for codec in THdfsCompression._NAMES_TO_VALUES.keys():
      for c in [codec.lower(), codec.upper()]:
        self.execute_query_expect_success(self.client, "compute stats {0}".format(table),
                                          {"compression_codec": c})
        self.execute_query_expect_success(self.client, "drop stats {0}".format(table))

  @SkipIfFS.hive
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
    self.run_stmt_in_hive(create_load_data_stmts)

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

  @staticmethod
  def create_load_test_corrupt_stats(self, unique_database, create_load_stmts,
          table_name, partitions, files):
    """A helper method for tests against the fix to IMPALA-9744."""
    # Create and load the Hive table.
    self.run_stmt_in_hive(create_load_stmts)

    # Make the table visible in Impala.
    self.execute_query("invalidate metadata %s.%s" % (unique_database, table_name))

    # Formulate a simple query that scans the Hive table.
    explain_stmt = """
    explain select * from {0}.{1} where
    int_col > (select 3*stddev(int_col) from {0}.{1})
    """.format(unique_database, table_name)
    explain_result = self.execute_query(explain_stmt)

    # Formulate a template which verifies the number of partitions and the number
    # of files are per spec.
    hdfs_physical_properties_template \
      = """HDFS partitions={0}/{0} files={1}""".format(partitions, files)

    # Check that the template formulated above exists and row count of the table is
    # not zero, for all scans.
    for i in range(len(explain_result.data)):
      if ("SCAN HDFS" in explain_result.data[i]):
         assert(hdfs_physical_properties_template in explain_result.data[i + 1])
         assert("cardinality=0" not in explain_result.data[i + 2])

  @SkipIfFS.hive
  def test_corrupted_stats_in_partitioned_hive_tables(self, vector, unique_database):
    """IMPALA-9744: Tests that the partition stats corruption in Hive tables
    (row count=0, partition size>0, persisted when the data was loaded with
    hive.stats.autogather=true) is handled at the table scan level.
    """
    # Unless something drastic changes in Hive and/or Impala, this test should
    # always succeed.
    if self.exploration_strategy() != 'exhaustive': pytest.skip()

    # Load from a local data file
    local_file = os.path.join(os.environ['IMPALA_HOME'],
                 "testdata/data/alltypes_tiny_pages.parquet")
    table_name = "partitioned_table_with_corrupted_and_missing_stats"

    # Setting hive.stats.autogather=true after CRTB DDL but before LOAD DML
    # minimally reproduces the corrupt stats issue.
    create_load_stmts = """
      CREATE TABLE {0}.{1} (
        id int COMMENT 'Add a comment',
        bool_col boolean,
        tinyint_col tinyint,
        smallint_col smallint,
        int_col int,
        bigint_col bigint,
        float_col float,
        double_col double,
        date_string_col string,
        string_col string,
        timestamp_col timestamp,
        year int,
        month int )
        PARTITIONED BY (decade string)
        STORED AS PARQUET;
      set hive.stats.autogather=true;
      load data local inpath '{2}' into table {0}.{1} partition (decade="corrupt-stats");
      set hive.stats.autogather=false;
      load data local inpath '{2}' into table {0}.{1} partition (decade="missing-stats");
    """.format(unique_database, table_name, local_file)

    self.create_load_test_corrupt_stats(self, unique_database, create_load_stmts,
            table_name, 2, 2)

  @SkipIfFS.hive
  def test_corrupted_stats_in_unpartitioned_hive_tables(self, vector, unique_database):
    """IMPALA-9744: Tests that the stats corruption in unpartitioned Hive
    tables (row count=0, partition size>0, persisted when the data was loaded
    with hive.stats.autogather=true) is handled at the table scan level.
    """
    # Unless something drastic changes in Hive and/or Impala, this test should
    # always succeed.
    if self.exploration_strategy() != 'exhaustive': pytest.skip()

    # Load from a local data file
    local_file = os.path.join(os.environ['IMPALA_HOME'],
                 "testdata/data/alltypes_tiny_pages.parquet")
    table_name = "nonpartitioned_table_with_corrupted_stats"

    # Setting hive.stats.autogather=true prior to CRTB DDL minimally reproduces the
    # corrupt stats issue.
    create_load_stmts = """
      set hive.stats.autogather=true;
      CREATE TABLE {0}.{1} (
        id int COMMENT 'Add a comment',
        bool_col boolean,
        tinyint_col tinyint,
        smallint_col smallint,
        int_col int,
        bigint_col bigint,
        float_col float,
        double_col double,
        date_string_col string,
        string_col string,
        timestamp_col timestamp,
        year int,
        month int)
        STORED AS PARQUET;
      load data local inpath '{2}' into table {0}.{1};
    """.format(unique_database, table_name, local_file)

    self.create_load_test_corrupt_stats(self, unique_database, create_load_stmts,
            table_name, 1, 1)

  @SkipIfCatalogV2.stats_pulling_disabled()
  def test_pull_stats_profile(self, vector, unique_database):
    """Checks that the frontend profile includes metrics when computing
       incremental statistics.
    """
    try:
      impalad = ImpalaCluster.get_e2e_test_cluster().impalads[0]
      client = impalad.service.create_beeswax_client()
      create = "create table test like functional.alltypes"
      load = "insert into test partition(year, month) select * from functional.alltypes"
      insert = """insert into test partition(year=2009, month=1) values
                  (29349999, true, 4, 4, 4, 40,4.400000095367432,40.4,
                  "10/21/09","4","2009-10-21 03:24:09.600000000")"""
      stats_all = "compute incremental stats test"
      stats_part = "compute incremental stats test partition (year=2009,month=1)"

      # Checks that profile does not have metrics for incremental stats when
      # the operation is not 'compute incremental stats'.
      self.execute_query_expect_success(client, "use `%s`" % unique_database)
      profile = self.execute_query_expect_success(client, create).runtime_profile
      assert profile.count("StatsFetch") == 0
      # Checks that incremental stats metrics are present when 'compute incremental
      # stats' is run. Since the table has no stats, expect that no bytes are fetched.
      self.execute_query_expect_success(client, load)
      profile = self.execute_query_expect_success(client, stats_all).runtime_profile
      assert profile.count("StatsFetch") > 1
      assert profile.count("StatsFetch.CompressedBytes: 0") == 1
      # Checks that bytes fetched is non-zero since incremental stats are present now
      # and should have been fetched.
      self.execute_query_expect_success(client, insert)
      profile = self.execute_query_expect_success(client, stats_part).runtime_profile
      assert profile.count("StatsFetch") > 1
      assert profile.count("StatsFetch.CompressedBytes") == 1
      assert profile.count("StatsFetch.CompressedBytes: 0") == 0
      # Adds a partition, computes stats, and checks that the metrics in the profile
      # reflect the operation.
      alter = "alter table test add partition(year=2011, month=1)"
      insert_new_partition = """
          insert into test partition(year=2011, month=1) values
          (29349999, true, 4, 4, 4, 40,4.400000095367432,40.4,
          "10/21/09","4","2009-10-21 03:24:09.600000000")
          """
      self.execute_query_expect_success(client, alter)
      self.execute_query_expect_success(client, insert_new_partition)
      profile = self.execute_query_expect_success(client, stats_all).runtime_profile
      assert profile.count("StatsFetch.TotalPartitions: 25") == 1
      assert profile.count("StatsFetch.NumPartitionsWithStats: 24") == 1
    finally:
      client.close()

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

  @pytest.mark.xfail(IMPALA_TEST_CLUSTER_PROPERTIES.is_remote_cluster(),
                     reason=("Setting up HBase tests currently assumes a local "
                             "mini-cluster. See IMPALA-4661."))
  def test_hbase_compute_stats(self, vector, unique_database):
    self.run_test_case('QueryTest/hbase-compute-stats', vector, unique_database)

  @pytest.mark.xfail(IMPALA_TEST_CLUSTER_PROPERTIES.is_remote_cluster(),
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


# Test column min/max stats currently enabled for Parquet tables.
class TestParquetComputeColumnMinMax(ImpalaTestSuite):
  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestParquetComputeColumnMinMax, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(create_single_exec_option_dimension())
    cls.ImpalaTestMatrix.add_constraint(
        lambda v: v.get_value('table_format').file_format == 'parquet')

  def test_compute_stats(self, vector, unique_database):
    self.run_test_case('QueryTest/compute-stats-column-minmax', vector, unique_database)


class TestInvalidStatsFromHms(ImpalaTestSuite):
  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestInvalidStatsFromHms, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(create_single_exec_option_dimension())
    cls.ImpalaTestMatrix.add_constraint(
        lambda v: v.get_value('table_format').file_format == 'text'
        and v.get_value('table_format').compression_codec == 'none')

  def test_invalid_col_stats(self, unique_database):
    """Test that invalid column stats, i.e. values < -1, are normalized in Impala"""
    tbl = unique_database + ".tbl"
    self.execute_query("create table {} as select 1 as id, 'aaa' as name".format(tbl))
    # Add invalid stats in HMS
    hms_client, _ = ImpalaTestSuite.create_hive_client(9083)
    cs = ColumnStatistics()
    cs.engine = "impala"
    isTblLevel = True
    cs.statsDesc = ColumnStatisticsDesc(isTblLevel, unique_database, "tbl")
    cs_data = ColumnStatisticsData()
    maxColLen = -100
    avgColLen = -200
    numNulls = -300
    numDVs = -400
    cs_data.stringStats = StringColumnStatsData(maxColLen, avgColLen, numNulls, numDVs)
    cs_obj = ColumnStatisticsObj("name", "string", cs_data)
    cs.statsObj = [cs_obj]
    assert hms_client.update_table_column_statistics(cs)
    # REFRESH to reload the stats
    self.execute_query("refresh " + tbl)
    # Verify the invalid stats are normalized to -1
    res = self.execute_query("show column stats " + tbl)
    assert res.data == [
      'id\tTINYINT\t-1\t-1\t1\t1\t-1\t-1',
      'name\tSTRING\t-1\t-1\t-1\t-1\t-1\t-1']
