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
import datetime

from tests.common.impala_test_suite import ImpalaTestSuite
from tests.util.iceberg_util import parse_timestamp, get_snapshots


class IcebergTestSuite(ImpalaTestSuite):

  @classmethod
  def execute_query_ts(cls, impalad_client, query):
    """Executes the given query then returns the time when it finished."""
    impalad_client.execute(query)
    return datetime.datetime.now()

  @classmethod
  def expect_num_snapshots_from(cls, impalad_client, tbl_name, ts, expected_result_size):
    """Executes DESCRIBE HISTORY <tbl> FROM through the given client. Verifies if the
       result snapshots are newer than the provided timestamp and checks the expected
       number of results."""
    snapshots = get_snapshots(impalad_client, tbl_name, ts_start=ts,
        expected_result_size=expected_result_size)
    for snapshot in snapshots:
      assert snapshot.get_creation_time() >= ts

  def expect_results_between(cls, impalad_client, tbl_name, ts_start, ts_end,
      expected_result_size):
    snapshots = get_snapshots(impalad_client, tbl_name, ts_start=ts_start,
        ts_end=ts_end, expected_result_size=expected_result_size)
    for snapshot in snapshots:
      assert snapshot.get_creation_time() >= ts_start
      assert snapshot.get_creation_time() <= ts_end

  @classmethod
  def impala_now(cls, impalad_client):
    now_data = impalad_client.execute("select now()")
    now_data_ts_dt = parse_timestamp(now_data.data[0])
    return now_data_ts_dt
