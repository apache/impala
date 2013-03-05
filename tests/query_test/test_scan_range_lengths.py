#!/usr/bin/env python
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
# Validates running with different scan range length values
#
import pytest
from copy import copy
from tests.common.test_vector import TestDimension
from tests.common.impala_test_suite import ImpalaTestSuite, ALL_NODES_ONLY

# We use very small scan ranges to exercise corner cases in the HDFS scanner more
# thoroughly. In particular, it will exercise:
# 1. scan range with no tuple
# 2. tuple that span across multiple scan ranges
MAX_SCAN_RANGE_LENGTHS = [1, 2, 5]

class TestScanRangeLengths(ImpalaTestSuite):
  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestScanRangeLengths, cls).add_test_dimensions()
    cls.TestMatrix.add_dimension(
        TestDimension('max_scan_range_length', *MAX_SCAN_RANGE_LENGTHS))

  def test_scan_ranges(self, vector):
    if vector.get_value('table_format').file_format != 'text':
      pytest.xfail(reason='IMP-636')
    elif vector.get_value('table_format').compression_codec != 'none':
      pytest.xfail(reason='IMPALA-122')

    vector.get_value('exec_option')['max_scan_range_length'] =\
        vector.get_value('max_scan_range_length')
    self.run_test_case('QueryTest/hdfs-tiny-scan', vector)
