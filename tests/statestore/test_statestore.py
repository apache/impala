#!/usr/bin/env python
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
#
import pytest
import os
from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.impala_cluster import Process

class SimpleSubscriberProcess(Process):
  """Runs a subscriber binary that registers with the statestore and immediately exits,
  indicating its sucesss in the exit code"""
  def __init__(self):
    binary_path = os.path.join(
      os.environ['IMPALA_HOME'], "be/build/debug/statestore/statestore-test-client")
    Process.__init__(self, [binary_path])

class TestStatestore(ImpalaTestSuite):
  def test_subscriber_restart(self):
    """Start several clients with the same subscriber ID to confirm that re-registration
    after a process restart works correctly (see IMPALA-620)"""
    s = SimpleSubscriberProcess()
    for i in xrange(5):
      s.start()
      rc, _, _ = s.wait()
      assert rc == 0
