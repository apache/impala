# Copyright (c) 2015 Cloudera, Inc. All rights reserved.
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
from tests.common.test_dimensions import create_single_exec_option_dimension

class TestTimeZones(ImpalaTestSuite):
  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestTimeZones, cls).add_test_dimensions()

    cls.TestMatrix.add_constraint(lambda v:\
        v.get_value('table_format').file_format == 'text' and\
        v.get_value('table_format').compression_codec == 'none')

  def test_timezones(self, vector):
    result = self.client.execute("select timezone, utctime, localtime, \
        from_utc_timestamp(utctime,timezone) as impalaresult from functional.alltimezones \
        where localtime != from_utc_timestamp(utctime,timezone)")
    assert(len(result.data) == 0)
