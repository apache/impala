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
import time

from copy import deepcopy
from tests.common.custom_cluster_test_suite import CustomClusterTestSuite


@CustomClusterTestSuite.with_args(
  catalogd_args="--hms_event_polling_interval_s=0",
  # Requires longer update frequency so invalidate->drop can happen within an update.
  # Still shorter than default so tests are quicker.
  statestored_args="--statestore_update_frequency_ms=1000",
  cluster_size=1)
class TestConcurrentRename(CustomClusterTestSuite):
  """Test concurrent rename with invalidate and other DDLs."""

  def test_rename_drop(self, vector, unique_database):
    name = "{}.tbl".format(unique_database)
    self.execute_query("create table {} (s string)".format(name))
    self.execute_query("describe {}".format(name))

    new_vector = deepcopy(vector)
    new_vector.get_value('exec_option')['debug_action'] = \
        "catalogd_table_rename_delay:SLEEP@5000"
    with self.create_impala_client_from_vector(new_vector) as alter_client, \
         self.create_impala_client() as reset_client:
      alter_handle = alter_client.execute_async(
        "alter table {0} rename to {0}2".format(name))
      time.sleep(0.1)
      reset_handle = reset_client.execute_async("invalidate metadata {}".format(name))
      self.execute_query("invalidate metadata {}2".format(name))
      self.execute_query("drop table {}2".format(name))

      try:
        alter_client.wait_for_finished_timeout(alter_handle, timeout=10)
        alter_client.close_query(alter_handle)
        assert False, "Expected alter to fail"
      except Exception as e:
        assert "The new table/view {}2 was concurrently removed during rename."\
            .format(name) in str(e)
      finally:
        reset_client.wait_for_finished_timeout(reset_handle, timeout=10)
        reset_client.close_query(reset_handle)

  def test_rename_invalidate(self, vector, unique_database):
    name = "{}.tbl".format(unique_database)
    self.execute_query("create table {} (s string)".format(name))
    self.execute_query("describe {}".format(name))

    new_vector = deepcopy(vector)
    new_vector.get_value('exec_option')['debug_action'] = \
        "catalogd_table_rename_delay:SLEEP@5000"
    with self.create_impala_client_from_vector(new_vector) as alter_client:
      alter_handle = alter_client.execute_async(
          "alter table {0} rename to {0}2".format(name))
      time.sleep(0.1)
      self.execute_query("invalidate metadata {}".format(name))

      alter_client.wait_for_finished_timeout(alter_handle, timeout=10)
      alter_client.close_query(alter_handle)
