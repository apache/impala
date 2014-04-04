#!/usr/bin/env python
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
# Functional tests for LOAD DATA statements.

import pytest
from tests.common.test_vector import *
from tests.common.impala_test_suite import *
from subprocess import call

class TestLoadData(ImpalaTestSuite):
  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestLoadData, cls).add_test_dimensions()
    cls.TestMatrix.add_dimension(create_single_exec_option_dimension())
    cls.TestMatrix.add_dimension(create_uncompressed_text_dimension(cls.get_workload()))

  def setup_method(self, method):
    # Cleanup any existing files in the test tables and staging directories.
    call(["hadoop", "fs", "-rm", "-r", "-f", "/test-warehouse/test_load*"], shell=False)
    call(["hadoop", "fs", "-rm", "-r", "-f", "/tmp/load_data/"], shell=False)

    # Create staging directories.
    for i in range(1, 6):
      call(["hadoop", "fs", "-mkdir", "-p", "/tmp/load_data/%d"  % i], shell=False)

    # Copy some data files from existing tables to validate load.
    for i in range(1, 4):
      call(["hadoop", "fs", "-cp",
          "/test-warehouse/alltypes/year=2010/month=1/100101.txt",
          "/tmp/load_data/%d" % i], shell=False)

    # Each partition in alltypesaggmultifiles should have 4 data files.
    for i in range(4, 6):
      call(["hadoop", "fs", "-cp",
          '/test-warehouse/alltypesaggmultifiles/year=2010/month=1/day=1/*',
          '/tmp/load_data/%d/' % i], shell=False)

    # Make some hidden files.
    call(["hadoop", "fs", "-cp",
        "/test-warehouse/alltypes/year=2010/month=1/100101.txt",
        "/tmp/load_data/3/.100101.txt"], shell=False)
    call(["hadoop", "fs", "-cp",
        "/test-warehouse/alltypes/year=2010/month=1/100101.txt",
        "/tmp/load_data/3/_100101.txt"], shell=False)

  @classmethod
  def __assert_hdfs_path_exists(cls, path):
    assert 0 == call(["hadoop", "fs", "-test", "-e", path], shell=False),\
        "Path does not exist."

  def test_load(self, vector):
    self.run_test_case('QueryTest/load', vector)
    # The hidden files should not have been moved as part of the load operation.
    self.__assert_hdfs_path_exists("/tmp/load_data/3/.100101.txt")
    self.__assert_hdfs_path_exists("/tmp/load_data/3/_100101.txt")
