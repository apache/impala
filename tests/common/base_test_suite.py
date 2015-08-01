# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
#
# The base class that should be used for tests.
import logging
import os
import pytest
from tests.common.test_dimensions import *
from tests.common.test_result_verifier import *
from tests.common.test_vector import *
LOG = logging.getLogger('base_test_suite')

# Base class for tests.
class BaseTestSuite(object):
  TestMatrix = TestMatrix()

  @classmethod
  def add_test_dimensions(cls):
    """
    A hook for adding additional dimensions.

    By default load the table_info and exec_option dimensions, but if a test wants to
    add more dimensions or different dimensions they can override this function.
    """
    cls.TestMatrix = TestMatrix()
