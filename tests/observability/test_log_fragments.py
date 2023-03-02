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

from __future__ import absolute_import, division, print_function
from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.skip import SkipIfDockerizedCluster

import re
import time


class TestLogFragments(ImpalaTestSuite):
  """Checks that logging includes query/fragment ids when available."""

  @SkipIfDockerizedCluster.daemon_logs_not_exposed
  def test_log_fragments(self):
    """Tests that fragment ids percolate through to the logs.

    Runs two queries that have more than one fragment; looks for logs annotated
    as expected. Also checks that some log statements are *not* tagged
    with anything, to ensure that threads "release" their tagging
    when they're done.
    """
    # This query produces more than one fragment when there
    # is more than one impalad running.
    result = self.execute_query("select count(*) from functional.alltypes")
    query_id = re.search("id=([0-9a-f]+:[0-9a-f]+)",
        result.runtime_profile).groups()[0]
    self.execute_query("select 1")
    self.assert_impalad_log_contains('INFO', query_id +
      "] Analysis and authorization finished.")
    assert query_id.endswith("000")
    # Looks for a fragment instance that doesn't end with "0" to make sure instances
    # are getting propagated too.
    self.assert_impalad_log_contains('INFO',
        query_id[:-1] + "[1-9]" + "] Instance completed")
    # This log entry should exist (many times), but not be tagged
    self.assert_impalad_log_contains('INFO',
        "impala-beeswax-server.cc:[0-9]+\] query: Query", -1)
    # Check that the version with the query_id doesn't exist!
    try:
      self.assert_impalad_log_contains('INFO',
          "impala-beeswax-server.*" + query_id + "].*query: Query")
      raise Exception("query-id still attached to thread after query finished")
    except AssertionError:
      pass
