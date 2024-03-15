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

from tests.beeswax.impala_beeswax import ImpalaBeeswaxException
from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.test_dimensions import create_single_exec_option_dimension


class TestSysDb(CustomClusterTestSuite):
  """Tests that are specific to the 'sys' database."""

  SYS_DB_NAME = "sys"

  @classmethod
  def add_test_dimensions(cls):
    super(TestSysDb, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(create_single_exec_option_dimension())

  @CustomClusterTestSuite.with_args()
  def test_query_log_table_create_sys_db_blocked(self, vector):
    """Asserts that the sys db cannot be created."""

    try:
      self.client.execute("create database {0}".format(self.SYS_DB_NAME))
      assert False, "database '{0}' should have failed to create but was created" \
          .format(self.SYS_DB_NAME)
    except ImpalaBeeswaxException as e:
      assert "Invalid db name: {0}. It has been blacklisted using --blacklisted_dbs" \
          .format(self.SYS_DB_NAME) in str(e), "database '{0}' failed to create but " \
          "for the wrong reason".format(self.SYS_DB_NAME)

  @CustomClusterTestSuite.with_args(impalad_args="--enable_workload_mgmt",
                                    catalogd_args="--enable_workload_mgmt")
  def test_query_log_table_create_table_sys_db_blocked(self, vector):
    """Asserts that no other tables can be created in the sys db."""

    table_name = "{0}.should_not_create".format(self.SYS_DB_NAME)

    try:
      self.client.execute("create table {0} (id STRING)".format(table_name))
      assert False, "table '{0}' should have failed to create but was created" \
          .format(table_name)
    except ImpalaBeeswaxException as e:
      assert "Query aborted:IllegalStateException: Can't create blacklisted table: {0}" \
          .format(table_name) in str(e), "table '{0}' failed to create but for the " \
          "wrong reason".format(table_name)
