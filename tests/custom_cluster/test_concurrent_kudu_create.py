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
from builtins import range
import pytest
import threading
import time

from multiprocessing.pool import ThreadPool

from tests.common.custom_cluster_test_suite import CustomClusterTestSuite

TBL_NAME = "test_concurrent_kudu_create"


class TestConcurrentKuduCreate(CustomClusterTestSuite):
  """Test concurrent create kudu managed table"""

  @pytest.mark.execute_serially
  def test_concurrent_create_kudu_table(self, unique_database):
    table_name = unique_database + "." + TBL_NAME
    test_self = self

    class ThreadLocalClient(threading.local):
      def __init__(self):
        self.client = test_self.create_impala_client()

    tls = ThreadLocalClient()

    def run_create_table_if_not_exists():
      self.execute_query_expect_success(
        tls.client, "create table if not exists %s "
                    "(id int, primary key(id)) stored as kudu" % table_name)

    # Drop table before run test if exists
    self.execute_query("drop table if exists %s" % table_name)
    NUM_ITERS = 20
    pool = ThreadPool(processes=3)
    for i in range(NUM_ITERS):
      # Run several commands by specific time interval to reproduce this bug
      r1 = pool.apply_async(run_create_table_if_not_exists)
      r2 = pool.apply_async(run_create_table_if_not_exists)
      # Sleep to make race conflict happens in different places
      time.sleep(1)
      r3 = pool.apply_async(run_create_table_if_not_exists)
      r1.get()
      r2.get()
      r3.get()
      # If hit IMPALA-8984, this query would be failed due to table been deleted in kudu
      self.execute_query_expect_success(tls.client, "select * from %s" % table_name)
      self.execute_query("drop table if exists %s" % table_name)
    pool.terminate()
