# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
# Functional tests running EXPLAIN statements.
#
import logging
import pytest
import re
from tests.common.test_vector import *
from tests.common.impala_test_suite import *
from tests.common.skip import SkipIfS3
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
    cls.TestMatrix.add_constraint(lambda v:\
        v.get_value('table_format').file_format == 'text' and\
        v.get_value('table_format').compression_codec == 'none' and\
        v.get_value('exec_option')['batch_size'] == 0 and\
        v.get_value('exec_option')['disable_codegen'] == False and\
        v.get_value('exec_option')['num_nodes'] != 1)

  @pytest.mark.xfail(run=False, reason="Expected per-host mem requirements inconsistent")
  def test_explain_level0(self, vector):
    vector.get_value('exec_option')['num_scanner_threads'] = self.NUM_SCANNER_THREADS
    vector.get_value('exec_option')['explain_level'] = 0
    self.run_test_case('QueryTest/explain-level0', vector)

  @pytest.mark.xfail(run=False, reason="Expected per-host mem requirements inconsistent")
  def test_explain_level1(self, vector):
    vector.get_value('exec_option')['num_scanner_threads'] = self.NUM_SCANNER_THREADS
    vector.get_value('exec_option')['explain_level'] = 1
    self.run_test_case('QueryTest/explain-level1', vector)

  @pytest.mark.xfail(run=False, reason="The test for missing table stats fails for avro")
  def test_explain_level2(self, vector):
    vector.get_value('exec_option')['num_scanner_threads'] = self.NUM_SCANNER_THREADS
    vector.get_value('exec_option')['explain_level'] = 2
    self.run_test_case('QueryTest/explain-level2', vector)

  @pytest.mark.xfail(run=False, reason="The test for missing table stats fails for avro")
  def test_explain_level3(self, vector):
    vector.get_value('exec_option')['num_scanner_threads'] = self.NUM_SCANNER_THREADS
    vector.get_value('exec_option')['explain_level'] = 3
    self.run_test_case('QueryTest/explain-level3', vector)

  def test_explain_validate_cardinality_estimates(self, vector):
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

class TestExplainEmptyPartition(ImpalaTestSuite):
  TEST_DB_NAME = "imp_1708"

  def setup_method(self, method):
    self.cleanup_db(self.TEST_DB_NAME)
    self.execute_query("create database if not exists {0} location '{1}/{0}.db'"
        .format(self.TEST_DB_NAME, WAREHOUSE))

  def teardown_method(self, method):
    self.cleanup_db(self.TEST_DB_NAME)

  @SkipIfS3.hdfs_client
  def test_non_empty_partition_0_rows(self):
    """Regression test for IMPALA-1708: if a partition has 0 rows but > 0 files after
    COMPUTE STATS, don't warn the user about missing stats. The files are probably
    corrupted, or used for something else."""
    self.client.execute("SET EXPLAIN_LEVEL=3")
    self.client.execute(
      "CREATE TABLE %s.empty_partition (col int) partitioned by (p int)" % self.TEST_DB_NAME);
    self.client.execute(
      "ALTER TABLE %s.empty_partition ADD PARTITION (p=NULL)" % self.TEST_DB_NAME)
    # Put an empty file in the partition so we have > 0 files, but 0 rows
    self.hdfs_client.create_file(
      "test-warehouse/%s.db/empty_partition/p=__HIVE_DEFAULT_PARTITION__/empty" %
      self.TEST_DB_NAME, "")
    self.client.execute("REFRESH %s.empty_partition" % self.TEST_DB_NAME)
    self.client.execute("COMPUTE STATS %s.empty_partition" % self.TEST_DB_NAME)
    assert "NULL\t0\t1" in str(
      self.client.execute("SHOW PARTITIONS %s.empty_partition" % self.TEST_DB_NAME))
    assert "missing relevant table and/or column statistics" not in str(
      self.client.execute("EXPLAIN SELECT * FROM %s.empty_partition" % self.TEST_DB_NAME))

    # Now add a partition with some data (so it gets selected into the scan), to check
    # that its lack of stats is correctly identified
    self.client.execute(
      "ALTER TABLE %s.empty_partition ADD PARTITION (p=1)" % self.TEST_DB_NAME)
    self.hdfs_client.create_file("test-warehouse/%s.db/empty_partition/p=1/rows" %
                                 self.TEST_DB_NAME, "1")
    self.client.execute("REFRESH %s.empty_partition" % self.TEST_DB_NAME)
    explain_result = str(
      self.client.execute("EXPLAIN SELECT * FROM %s.empty_partition" % self.TEST_DB_NAME))
    assert "missing relevant table and/or column statistics" in explain_result
    # Also test IMPALA-1530 - adding the number of partitions missing stats
    assert "1 partition(s) missing stats" in explain_result
