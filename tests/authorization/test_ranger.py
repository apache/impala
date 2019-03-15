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
  def test_grant_revoke(self):
    user = getuser()
    admin_client = self.create_impala_client()
    user_client = self.create_impala_client()

    try:
      self.execute_query_expect_success(admin_client,
                                        "grant select on database functional to user {0}"
                                        .format(user), user="admin")
      # TODO: IMPALA-8293 use refresh authorization
      time.sleep(35)
      self.execute_query_expect_success(user_client, "show tables in functional",
                                        user=user)
      self.execute_query_expect_success(admin_client,
                                        "revoke select on database functional from user "
                                        "{0}".format(getuser()), user="admin")
      # TODO: IMPALA-8293 use refresh authorization
      time.sleep(35)
      self.execute_query_expect_failure(user_client, "show tables in functional")
    finally:
      admin_client.execute("revoke select on database functional from user {0}"
                           .format(getuser()), user="admin")
