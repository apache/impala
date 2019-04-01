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
# Client tests for SQL statement authorization

import grp
import pytest
import time
from getpass import getuser

from tests.common.custom_cluster_test_suite import CustomClusterTestSuite


class TestRanger(CustomClusterTestSuite):
  """
  Tests for Apache Ranger integration with Apache Impala.
  """
  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args="--server-name=server1 --ranger_service_type=hive "
                 "--ranger_app_id=impala "
                 "--authorization_factory_class="
                 "org.apache.impala.authorization.ranger.RangerAuthorizationFactory",
    catalogd_args="--server-name=server1 --ranger_service_type=hive "
                  "--ranger_app_id=impala "
                  "--authorization_factory_class="
                  "org.apache.impala.authorization.ranger.RangerAuthorizationFactory")
  def test_grant_revoke(self, unique_name):
    user = getuser()
    admin = "admin"
    admin_client = self.create_impala_client()
    user_client = self.create_impala_client()
    unique_database = unique_name + "_db"
    unique_table = unique_name + "_tbl"
    group = grp.getgrnam(getuser()).gr_name
    test_data = [(user, "user"), (group, "group")]

    for data in test_data:
      ident = data[0]
      kw = data[1]

      try:
        # Set-up temp database/table
        admin_client.execute("drop database if exists {0} cascade"
                             .format(unique_database), user=admin)
        admin_client.execute("create database {0}".format(unique_database), user=admin)
        admin_client.execute("create table {0}.{1} (x int)"
                             .format(unique_database, unique_table), user=admin)

        self.execute_query_expect_success(admin_client,
                                          "grant select on database {0} to {1} {2}"
                                          .format(unique_database, kw, ident), user=admin)
        # TODO: IMPALA-8293 use refresh authorization
        time.sleep(10)
        self.execute_query_expect_success(user_client, "show tables in {0}"
                                          .format(unique_database), user=user)
        self.execute_query_expect_success(admin_client,
                                          "revoke select on database {0} from {1} "
                                          "{2}".format(unique_database, kw, ident),
                                          user=admin)
        # TODO: IMPALA-8293 use refresh authorization
        time.sleep(10)
        self.execute_query_expect_failure(user_client, "show tables in {0}"
                                          .format(unique_database))
      finally:
        admin_client.execute("revoke select on database {0} from {1} {2}"
                             .format(unique_database, kw, ident), user=admin)
        admin_client.execute("drop database if exists {0} cascade"
                             .format(unique_database), user=admin)
