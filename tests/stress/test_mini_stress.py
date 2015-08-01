# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
#
import os
import pytest
import re
from time import sleep
from tests.common.impala_cluster import ImpalaCluster
from tests.common.test_vector import TestDimension
from tests.common.impala_test_suite import ImpalaTestSuite
from tests.util.test_file_parser import QueryTestSectionReader

# Number of times to execute each query
NUM_ITERATIONS = 5

# Each client will get a different test id.
TEST_IDS = xrange(0, 10)

# Runs many queries in parallel. The goal is to have this complete in a reasonable amount
# of time and be run as part of all test runs.
class TestMiniStress(ImpalaTestSuite):
  @classmethod
  def get_workload(self):
    return 'targeted-stress'

  @classmethod
  def add_test_dimensions(cls):
    super(TestMiniStress, cls).add_test_dimensions()
    cls.TestMatrix.add_dimension(TestDimension('test_id', *TEST_IDS))
    if cls.exploration_strategy() != 'exhaustive':
      cls.TestMatrix.add_constraint(lambda v:\
          v.get_value('exec_option')['batch_size'] == 0)
    else:
      cls.TestMatrix.add_constraint(lambda v:\
          v.get_value('exec_option')['batch_size'] != 1)

  @pytest.mark.stress
  def test_mini_stress(self, vector):
    for i in xrange(NUM_ITERATIONS):
      self.run_test_case('stress', vector)

  @pytest.mark.stress
  def test_sort_stress(self, vector):
    if self.exploration_strategy() == 'core':
      pytest.skip("Skipping sort stress tests for core")
    if vector.get_value('table_format').file_format != 'parquet':
      pytest.skip("skipping file format")
    vector.get_value('exec_option')['disable_outermost_topn'] = 1
    vector.get_value('exec_option')['mem_limit'] = "200m"
    self.run_test_case('sort_stress', vector)

  @pytest.mark.skipif(True,
      reason="Skip until the race in the catalog server is resolved")
  @pytest.mark.stress
  def test_run_invalidate_refresh(self, vector):
    """Verifies that running concurrent invalidate table/catalog and refresh commands
    don't cause failures with other running workloads and ensures catalog versions
    are strictly increasing."""
    target_db = self.execute_scalar('select current_database()', vector=vector)
    impala_cluster = ImpalaCluster()
    impalad = impala_cluster.impalads[0].service
    catalogd = impala_cluster.catalogd.service

    for i in xrange(NUM_ITERATIONS):
      # Get the catalog versions for the table before running the workload
      before_versions = dict()
      before_versions['catalogd'] =\
          self.get_table_version(catalogd, target_db, 'lineitem')
      before_versions['impalad'] = self.get_table_version(impalad, target_db, 'lineitem')

      self.run_test_case('stress-with-invalidate-refresh', vector)

      # Get the catalog versions for the table after running the workload
      after_versions = dict()
      after_versions['catalogd'] = self.get_table_version(catalogd, target_db, 'lineitem')
      after_versions['impalad'] = self.get_table_version(impalad, target_db, 'lineitem')

      # Catalog versions should be strictly increasing
      assert before_versions['impalad'] < after_versions['impalad']
      assert before_versions['catalogd'] < after_versions['catalogd']

  def get_table_version(self, impala_service, db_name, tbl_name):
    """Gets the given table's catalog version using the given impalad/catalogd service"""
    obj_dump = impala_service.get_catalog_object_dump('table', db_name + '.' + tbl_name)
    result = re.search(r'catalog_version \(i64\) = (\d+)', obj_dump)
    assert result, 'Unable to find catalog version in object dump: ' + obj_dump
    catalog_version = result.group(1)
    return int(catalog_version)
