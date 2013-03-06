#!/usr/bin/env python
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
#
# This test suite validates the scanners by running queries against ALL file formats and
# their permutations (e.g. compression codec/compression type). This works by exhaustively
# generating the table format test vectors for this specific test suite. This way, other
# tests can run with the normal exploration strategy and the overall test runtime doesn't
# explode.

import logging
import pytest
from tests.common.test_vector import *
from tests.common.impala_test_suite import *

class TestScannersAllTableFormats(ImpalaTestSuite):
  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestScannersAllTableFormats, cls).add_test_dimensions()
    # Exhaustively generate all table format vectors. This can still be overridden
    # using the --table_formats flag.
    cls.TestMatrix.add_dimension(cls.create_table_info_dimension('exhaustive'))

  def test_scanners(self, vector):
    self.run_test_case('QueryTest/scanners', vector)
