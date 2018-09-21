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
# These tests verify the functionality of SHOW GRANT USER. We
# create several users and groups to verify clear separation.

import grp
import pytest
from getpass import getuser
from os import getenv

from tests.common.test_dimensions import create_uncompressed_text_dimension
from tests.common.custom_cluster_test_suite import CustomClusterTestSuite

SENTRY_CONFIG_FILE = getenv('IMPALA_HOME') + \
    '/fe/src/test/resources/sentry-site_oo.xml'


class TestShowGrantUser(CustomClusterTestSuite):
  @classmethod
  def add_test_dimensions(cls):
    super(TestShowGrantUser, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(
        create_uncompressed_text_dimension(cls.get_workload()))

  @classmethod
  def get_workload(cls):
    return 'functional-query'

  def setup_method(self, method):
    super(TestShowGrantUser, self).setup_method(method)
    self.__test_cleanup()

  def teardown_method(self, method):
    self.__test_cleanup()
    self.client.execute('drop role sgu_test_admin')
    super(TestShowGrantUser, self).teardown_method(method)

  def __test_cleanup(self):
    # Clean up any old roles created by this test
    for role_name in self.client.execute('show roles').data:
      if 'sgu_test' in role_name:
        self.client.execute('drop role %s' % role_name)

    # Cleanup any other roles that were granted to this user.
    # TODO: Update Sentry Service config and authorization tests to use LocalGroupMapping
    # for resolving users -> groups. This way we can specify custom test users that don't
    # actually exist in the system.
    group_name = grp.getgrnam(getuser()).gr_name
    for role_name in self.client.execute('show role grant group `%s`' % group_name).data:
      self.client.execute('drop role %s' % role_name)

    # Create a temporary admin user so we can actually view/clean up the test db.
    self.client.execute('create role sgu_test_admin')
    self.client.execute('grant all on server to sgu_test_admin')
    self.client.execute('grant role sgu_test_admin to group `%s`'
        % group_name)

  @classmethod
  def restart_first_impalad(cls):
    impalad = cls.cluster.impalads[0]
    impalad.restart()
    cls.client = impalad.service.create_beeswax_client()

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      impalad_args='--server_name=server1 --authorization_policy_provider_class='
      'org.apache.impala.service.CustomClusterResourceAuthorizationProvider '
      '--sentry_config={0}'.format(SENTRY_CONFIG_FILE),
      catalogd_args='--sentry_config={0} --authorization_policy_provider_class='
      'org.apache.impala.service.CustomClusterResourceAuthorizationProvider'
      .format(SENTRY_CONFIG_FILE),
      sentry_config=SENTRY_CONFIG_FILE)
  def test_show_grant_user(self, vector, unique_database):
    group_name = grp.getgrnam(getuser()).gr_name
    self.client.execute('create role sgu_test_primary')
    self.client.execute('grant all on server to sgu_test_primary')
    self.client.execute('grant role sgu_test_primary to group `%s`' % group_name)
    self.run_test_case('QueryTest/show_grant_user', vector, use_db=unique_database)
