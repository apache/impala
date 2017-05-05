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

# Functional tests running EXPLAIN statements.
#
import pytest
import re

from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.skip import SkipIfLocal, SkipIfNotHdfsMinicluster
from tests.util.filesystem_utils import WAREHOUSE

# Tests the different explain levels [0-3] on a few queries.
# TODO: Clean up this test to use an explain level test dimension and appropriate
# result sub-sections for the expected explain plans.
class TestExplain(ImpalaTestSuite):
  # Value for the num_scanner_threads query option to ensure that the memory estimates of
  # scan nodes are consistent even when run on machines with different numbers of cores.
  NUM_SCANNER_THREADS = 1

  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestExplain, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_constraint(lambda v:\
        v.get_value('table_format').file_format == 'text' and\
        v.get_value('table_format').compression_codec == 'none' and\
        v.get_value('exec_option')['batch_size'] == 0 and\
        v.get_value('exec_option')['disable_codegen'] == False and\
        v.get_value('exec_option')['num_nodes'] != 1)

  @SkipIfNotHdfsMinicluster.plans
  def test_explain_level0(self, vector):
    vector.get_value('exec_option')['num_scanner_threads'] = self.NUM_SCANNER_THREADS
    vector.get_value('exec_option')['explain_level'] = 0
    self.run_test_case('QueryTest/explain-level0', vector)

  @SkipIfNotHdfsMinicluster.plans
  def test_explain_level1(self, vector):
    vector.get_value('exec_option')['num_scanner_threads'] = self.NUM_SCANNER_THREADS
    vector.get_value('exec_option')['explain_level'] = 1
    self.run_test_case('QueryTest/explain-level1', vector)

  @SkipIfNotHdfsMinicluster.plans
  def test_explain_level2(self, vector):
    vector.get_value('exec_option')['num_scanner_threads'] = self.NUM_SCANNER_THREADS
    vector.get_value('exec_option')['explain_level'] = 2
    self.run_test_case('QueryTest/explain-level2', vector)

  @SkipIfNotHdfsMinicluster.plans
  def test_explain_level3(self, vector):
    vector.get_value('exec_option')['num_scanner_threads'] = self.NUM_SCANNER_THREADS
    vector.get_value('exec_option')['explain_level'] = 3
    self.run_test_case('QueryTest/explain-level3', vector)

  def test_explain_validate_cardinality_estimates(self, vector, unique_database):
    # Tests that the cardinality estimates are correct for partitioned tables.
    # TODO Cardinality estimation tests should eventually be part of the planner tests.
    # TODO Remove this test
    db_name = 'functional'
    tbl_name = 'alltypes'

    def check_cardinality(query_result, expected_cardinality):
      regex = re.compile('tuple-ids=\d+ row-size=\d+B cardinality=(\d+)')
      for res in query_result:
        m = regex.match(res.strip())
        if m:
          assert len(m.groups()) == 1
          # The cardinality should be zero.
          assert m.groups()[0] == expected_cardinality

    # All partitions are filtered out, cardinality should be 0.
    result = self.execute_query("explain select * from %s.%s where year = 1900" % (
        db_name, tbl_name), query_options={'explain_level':3})
    check_cardinality(result.data, '0')

    # Half of the partitions are filtered out, cardinality should be 3650.
    result = self.execute_query("explain select * from %s.%s where year = 2010" % (
        db_name, tbl_name), query_options={'explain_level':3})
    check_cardinality(result.data, '3650')

    # None of the partitions are filtered out, cardinality should be 7300.
    result = self.execute_query("explain select * from %s.%s" % (db_name, tbl_name),
        query_options={'explain_level':3})
    check_cardinality(result.data, '7300')

    # Create a partitioned table with a mixed set of available stats,
    mixed_tbl = unique_database + ".t"
    self.execute_query(
      "create table %s (c int) partitioned by (p int)" % mixed_tbl)
    self.execute_query(
      "insert into table %s partition (p) values(1,1),(2,2),(3,3)" % mixed_tbl)
    # Set the number of rows at the table level.
    self.execute_query(
      "alter table %s set tblproperties('numRows'='100')" % mixed_tbl)
    # Should fall back to table-level cardinality when partitions lack stats.
    result = self.execute_query("explain select * from %s" % mixed_tbl,
        query_options={'explain_level':3})
    check_cardinality(result.data, '100')
    # Should fall back to table-level cardinality, even for a subset of partitions,
    result = self.execute_query("explain select * from %s where p = 1" % mixed_tbl,
        query_options={'explain_level':3})
    check_cardinality(result.data, '100')
    # Set the number of rows for a single partition.
    self.execute_query(
      "alter table %s partition(p=1) set tblproperties('numRows'='50')" % mixed_tbl)
    # Use partition stats when availabe. Partitions without stats are ignored.
    result = self.execute_query("explain select * from %s" % mixed_tbl,
        query_options={'explain_level':3})
    check_cardinality(result.data, '50')
    # Fall back to table-level stats when no selected partitions have stats.
    result = self.execute_query("explain select * from %s where p = 2" % mixed_tbl,
        query_options={'explain_level':3})
    check_cardinality(result.data, '100')  



class TestExplainEmptyPartition(ImpalaTestSuite):
  TEST_DB_NAME = "imp_1708"

  def setup_method(self, method):
    self.cleanup_db(self.TEST_DB_NAME)
    self.execute_query("create database if not exists {0} location '{1}/{0}.db'"
        .format(self.TEST_DB_NAME, WAREHOUSE))

  def teardown_method(self, method):
    self.cleanup_db(self.TEST_DB_NAME)

  @SkipIfLocal.hdfs_client
  def test_non_empty_partition_0_rows(self):
    """Regression test for IMPALA-1708: if a partition has 0 rows but > 0 files after
    COMPUTE STATS, don't warn the user about missing stats. The files are probably
    corrupted, or used for something else."""
    self.client.execute("SET EXPLAIN_LEVEL=3")
    self.client.execute("CREATE TABLE %s.empty_partition (col int) "
                        "partitioned by (p int)" % self.TEST_DB_NAME)
    self.client.execute(
      "ALTER TABLE %s.empty_partition ADD PARTITION (p=NULL)" % self.TEST_DB_NAME)
    # Put an empty file in the partition so we have > 0 files, but 0 rows
    self.filesystem_client.create_file(
        "test-warehouse/%s.db/empty_partition/p=__HIVE_DEFAULT_PARTITION__/empty" %
        self.TEST_DB_NAME, "")
    self.client.execute("REFRESH %s.empty_partition" % self.TEST_DB_NAME)
    self.client.execute("COMPUTE STATS %s.empty_partition" % self.TEST_DB_NAME)
    assert "NULL\t0\t1" in str(
        self.client.execute("SHOW PARTITIONS %s.empty_partition" % self.TEST_DB_NAME))
    assert "missing relevant table and/or column statistics" not in str(
        self.client.execute(
            "EXPLAIN SELECT * FROM %s.empty_partition" % self.TEST_DB_NAME))

    # Now add a partition with some data (so it gets selected into the scan), to check
    # that its lack of stats is correctly identified
    self.client.execute(
      "ALTER TABLE %s.empty_partition ADD PARTITION (p=1)" % self.TEST_DB_NAME)
    self.filesystem_client.create_file("test-warehouse/%s.db/empty_partition/p=1/rows" %
                                 self.TEST_DB_NAME, "1")
    self.client.execute("REFRESH %s.empty_partition" % self.TEST_DB_NAME)
    explain_result = str(
      self.client.execute("EXPLAIN SELECT * FROM %s.empty_partition" % self.TEST_DB_NAME))
    assert "missing relevant table and/or column statistics" in explain_result
    # Also test IMPALA-1530 - adding the number of partitions missing stats
    assert "1 partition(s) missing stats" in explain_result
