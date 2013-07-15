#!/usr/bin/env python
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
# Impala tests for DDL statements
import logging
import pytest
import shlex
from subprocess import call
from tests.common.test_vector import *
from tests.common.test_dimensions import ALL_NODES_ONLY
from tests.common.impala_test_suite import *

# Validates DDL statements (create, drop)
class TestDdlStatements(ImpalaTestSuite):
  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestDdlStatements, cls).add_test_dimensions()
    # There is no reason to run these tests using all dimensions.
    cls.TestMatrix.add_constraint(lambda v:\
        v.get_value('table_format').file_format == 'text' and\
        v.get_value('table_format').compression_codec == 'none')


  def setup_method(self, method):
    self.cleanup()
    # Get the current number of queries that are in the 'EXCEPTION' state. Used for
    # verification after running each test case.
    self.start_exception_count = self.query_exception_count()

  def teardown_method(self, method):
    self.cleanup()
    end_exception_count = self.query_exception_count()
    # The number of exceptions may be < than what was in setup if the queries in the
    # EXCEPTION state were bumped out of the FINISHED list. We should never see an
    # increase in the number of queries in the exception state.
    assert end_exception_count <= self.start_exception_count

  def query_exception_count(self):
    """Returns the number of occurrences of 'EXCEPTION' on the debug /queries page"""
    return len(re.findall('EXCEPTION',
        self.impalad_test_service.read_debug_webpage('queries')))

  def cleanup(self):
    map(self.cleanup_db, ['ddl_test_db', 'alter_table_test_db', 'alter_table_test_db2'])
    self.cleanup_hdfs_dirs()

  def cleanup_hdfs_dirs(self):
    # Cleanup the test table HDFS dirs between test runs so there are no errors the next
    # time a table is created with the same location. This also helps remove any stale
    # data from the last test run.
    call(["hadoop", "fs", "-rm", "-r", "-f", "/test-warehouse/t1_tmp1/"], shell=False)
    call(["hadoop", "fs", "-rm", "-r", "-f", "/test-warehouse/t_part_tmp/"], shell=False)

  @pytest.mark.execute_serially
  def test_create(self, vector):
    vector.get_value('exec_option')['abort_on_error'] = False
    self.run_test_case('QueryTest/create', vector)

  @pytest.mark.execute_serially
  def test_alter_table(self, vector):
    vector.get_value('exec_option')['abort_on_error'] = False
    self.run_test_case('QueryTest/alter-table', vector)

  @pytest.mark.execute_serially
  def test_views_ddl(self, vector):
    vector.get_value('exec_option')['abort_on_error'] = False
    self.run_test_case('QueryTest/views-ddl', vector)
