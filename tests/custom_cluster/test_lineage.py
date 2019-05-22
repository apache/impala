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
#
# Tests for column lineage.
# TODO: add verification for more fields.

import json
import logging
import os
import pytest
import re
import shutil
import stat
import tempfile
import time

from tests.common.custom_cluster_test_suite import CustomClusterTestSuite

LOG = logging.getLogger(__name__)


class TestLineage(CustomClusterTestSuite):
  lineage_log_dir = tempfile.mkdtemp()

  @classmethod
  def setup_class(cls):
    super(TestLineage, cls).setup_class()

  @classmethod
  def teardown_class(cls):
    shutil.rmtree(cls.lineage_log_dir)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args("--lineage_event_log_dir=%s" % lineage_log_dir)
  def test_start_end_timestamp(self, vector):
    """Test that 'timestamp' and 'endTime' in the lineage graph are populated with valid
       UNIX times."""
    LOG.info("lineage_event_log_dir is " + self.lineage_log_dir)
    before_time = int(time.time())
    query = "select count(*) from functional.alltypes"
    result = self.execute_query_expect_success(self.client, query)
    profile_query_id = re.search("Query \(id=(.*)\):", result.runtime_profile).group(1)
    after_time = int(time.time())
    LOG.info("before_time " + str(before_time) + " after_time " + str(after_time))

    # Stop the cluster in order to flush the lineage log files.
    self._stop_impala_cluster()

    for log_filename in os.listdir(self.lineage_log_dir):
      log_path = os.path.join(self.lineage_log_dir, log_filename)
      # Only the coordinator's log file will be populated.
      if os.path.getsize(log_path) > 0:
        LOG.info("examining file: " + log_path)
        with open(log_path) as log_file:
          lineage_json = json.load(log_file)
          assert lineage_json["queryId"] == profile_query_id
          timestamp = int(lineage_json["timestamp"])
          end_time = int(lineage_json["endTime"])
          assert before_time <= timestamp
          assert timestamp <= end_time
          assert end_time <= after_time
      else:
        LOG.info("empty file: " + log_path)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args("--lineage_event_log_dir={0}".format(lineage_log_dir))
  def test_create_table_timestamp(self, vector, unique_database):
    """Test that 'createTableTime' in the lineage graph are populated with valid value
       from HMS."""
    query = "create table {0}.lineage_test_tbl as select int_col, tinyint_col " \
            "from functional.alltypes".format(unique_database)
    result = self.execute_query_expect_success(self.client, query)
    profile_query_id = re.search("Query \(id=(.*)\):", result.runtime_profile).group(1)

    # Wait to flush the lineage log files.
    time.sleep(3)

    for log_filename in os.listdir(self.lineage_log_dir):
      log_path = os.path.join(self.lineage_log_dir, log_filename)
      # Only the coordinator's log file will be populated.
      if os.path.getsize(log_path) > 0:
        with open(log_path) as log_file:
          lineage_json = json.load(log_file)
          assert lineage_json["queryId"] == profile_query_id
          vertices = lineage_json["vertices"]
          for vertex in vertices:
            if vertex["vertexId"] == "int_col":
              assert "metadata" in vertex
              table_name = vertex["metadata"]["tableName"]
              table_create_time = int(vertex["metadata"]["tableCreateTime"])
              assert "{0}.lineage_test_tbl".format(unique_database) == table_name
              assert table_create_time != -1
