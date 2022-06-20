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

import datetime

from tests.common.impala_test_suite import ImpalaTestSuite


class IcebergTestSuite(ImpalaTestSuite):

  @classmethod
  def quote(cls, s):
    return "'{0}'".format(s)

  @classmethod
  def cast_ts(cls, ts):
    return "CAST({0} as timestamp)".format(cls.quote(ts))

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
    query = "DESCRIBE HISTORY {0} FROM {1};".format(tbl_name, cls.cast_ts(ts))
    data = impalad_client.execute(query)
    assert len(data.data) == expected_result_size
    for i in range(len(data.data)):
      result_ts_dt = cls.parse_timestamp(data.data[i].split('\t')[0])
      assert result_ts_dt >= ts

  @classmethod
  def parse_timestamp(cls, ts_string):
    """The client can receive the timestamp in two formats, if the timestamp has
    fractional seconds "yyyy-MM-dd HH:mm:ss.SSSSSSSSS" pattern is used, otherwise
    "yyyy-MM-dd HH:mm:ss". Additionally, Python's datetime library cannot handle
    nanoseconds, therefore in that case the timestamp has to be trimmed."""
    if len(ts_string.split('.')) > 1:
      return datetime.datetime.strptime(ts_string[:-3], '%Y-%m-%d %H:%M:%S.%f')
    else:
      return datetime.datetime.strptime(ts_string, '%Y-%m-%d %H:%M:%S')

  @classmethod
  def impala_now(cls, impalad_client):
    now_data = impalad_client.execute("select now()")
    now_data_ts_dt = cls.parse_timestamp(now_data.data[0])
    return now_data_ts_dt
