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

from __future__ import absolute_import, division, print_function
import json
import logging
import os
import pytest
import re
import tempfile
import time

from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.skip import SkipIfFS

LOG = logging.getLogger(__name__)


class TestLineage(CustomClusterTestSuite):
  START_END_TIME_LINEAGE_LOG_DIR = tempfile.mkdtemp(prefix="start_end_time")
  CREATE_TABLE_TIME_LINEAGE_LOG_DIR = tempfile.mkdtemp(prefix="create_table_time")
  DDL_LINEAGE_LOG_DIR = tempfile.mkdtemp(prefix="ddl_lineage")
  LINEAGE_TESTS_DIR = tempfile.mkdtemp(prefix="test_lineage")

  @classmethod
  def setup_class(cls):
    super(TestLineage, cls).setup_class()

  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args("--lineage_event_log_dir={0}"
                                    .format(START_END_TIME_LINEAGE_LOG_DIR))
  def test_start_end_timestamp(self, vector):
    """Test that 'timestamp' and 'endTime' in the lineage graph are populated with valid
       UNIX times."""
    LOG.info("lineage_event_log_dir is {0}".format(self.START_END_TIME_LINEAGE_LOG_DIR))
    before_time = int(time.time())
    query = "select count(*) from functional.alltypes"
    result = self.execute_query_expect_success(self.client, query)
    profile_query_id = re.search("Query \(id=(.*)\):", result.runtime_profile).group(1)
    after_time = int(time.time())
    LOG.info("before_time " + str(before_time) + " after_time " + str(after_time))

    # Stop the cluster in order to flush the lineage log files.
    self._stop_impala_cluster()

    for log_filename in os.listdir(self.START_END_TIME_LINEAGE_LOG_DIR):
      log_path = os.path.join(self.START_END_TIME_LINEAGE_LOG_DIR, log_filename)
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
  @CustomClusterTestSuite.with_args("--lineage_event_log_dir={0}"
                                    .format(CREATE_TABLE_TIME_LINEAGE_LOG_DIR))
  def test_create_table_timestamp(self, unique_database):
    for table_format in ['textfile', 'kudu', 'iceberg']:
      self.run_test_create_table_timestamp(unique_database, table_format)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      "--lineage_event_log_dir={0}"
      .format(CREATE_TABLE_TIME_LINEAGE_LOG_DIR),
      catalogd_args="--hms_event_polling_interval_s=0")
  def test_create_table_timestamp_without_hms_events(self, unique_database):
    for table_format in ['textfile', 'kudu', 'iceberg']:
      self.run_test_create_table_timestamp(unique_database, table_format)

  def run_test_create_table_timestamp(self, unique_database, table_format):
    """Test that 'createTableTime' in the lineage graph are populated with valid value
       from HMS."""
    not_enforced = ""
    if table_format == "iceberg":
      not_enforced = " NOT ENFORCED"
    query = "create table {0}.lineage_test_tbl_{1} primary key (int_col) {2} " \
        "stored as {1} as select int_col, bigint_col from functional.alltypes".format(
            unique_database, table_format, not_enforced)
    result = self.execute_query_expect_success(self.client, query)
    profile_query_id = re.search("Query \(id=(.*)\):", result.runtime_profile).group(1)

    # Wait to flush the lineage log files.
    time.sleep(3)

    for log_filename in os.listdir(self.CREATE_TABLE_TIME_LINEAGE_LOG_DIR):
      log_path = os.path.join(self.CREATE_TABLE_TIME_LINEAGE_LOG_DIR, log_filename)
      # Only the coordinator's log file will be populated.
      if os.path.getsize(log_path) > 0:
        with open(log_path) as log_file:
          for line in log_file:
            # Now that the test is executed multiple times we need to take a look at
            # only the line that contains the expected table name.
            expected_table_name =\
                "{0}.lineage_test_tbl_{1}".format(unique_database, table_format)
            if expected_table_name not in line: continue

            lineage_json = json.loads(line)
            assert lineage_json["queryId"] == profile_query_id
            vertices = lineage_json["vertices"]
            for vertex in vertices:
              if vertex["vertexId"] == "int_col":
                assert "metadata" in vertex
                table_name = vertex["metadata"]["tableName"]
                table_create_time = int(vertex["metadata"]["tableCreateTime"])
                assert expected_table_name == table_name
                assert table_create_time != -1

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args("--lineage_event_log_dir={0}"
                                    .format(DDL_LINEAGE_LOG_DIR))
  def test_ddl_lineage(self, unique_database):
    """ Test that DDLs like 'create table' have query text populated in the lineage
    graph."""
    query = "create external table {0}.ddl_lineage_tbl (id int)".format(unique_database)
    result = self.execute_query_expect_success(self.client, query)
    profile_query_id = re.search("Query \(id=(.*)\):", result.runtime_profile).group(1)

    # Wait to flush the lineage log files.
    time.sleep(3)

    for log_filename in os.listdir(self.DDL_LINEAGE_LOG_DIR):
      log_path = os.path.join(self.DDL_LINEAGE_LOG_DIR, log_filename)
      # Only the coordinator's log file will be populated.
      if os.path.getsize(log_path) > 0:
        with open(log_path) as log_file:
          lineage_json = json.load(log_file)
          assert lineage_json["queryId"] == profile_query_id
          assert lineage_json["queryText"] is not None
          assert lineage_json["queryText"] == query
          assert lineage_json["tableLocation"] is not None

    # Test explain statements don't create lineages.
    query = "explain create table {0}.lineage_test_tbl as select int_col, " \
            "tinyint_col from functional.alltypes".format(unique_database)
    result = self.execute_query_expect_success(self.client, query)
    profile_query_id = re.search("Query \(id=(.*)\):", result.runtime_profile).group(1)

    # Wait to flush the lineage log files.
    time.sleep(3)

    for log_filename in os.listdir(self.DDL_LINEAGE_LOG_DIR):
      log_path = os.path.join(self.DDL_LINEAGE_LOG_DIR, log_filename)
      # Only the coordinator's log file will be populated.
      if os.path.getsize(log_path) > 0:
        with open(log_path) as log_file:
          lineage_json = json.load(log_file)
          assert lineage_json["queryId"] is not profile_query_id

  @SkipIfFS.hbase
  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args("--lineage_event_log_dir={0}"
                                    .format(LINEAGE_TESTS_DIR))
  def test_lineage_output(self, vector):
    try:
      self.run_test_case('QueryTest/lineage', vector)
    finally:
      # Clean up the test database
      db_cleanup = "drop database if exists lineage_test_db cascade"
      self.execute_query_expect_success(self.client, db_cleanup)
