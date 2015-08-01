# Copyright (c) 2014 Cloudera, Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Client tests for SQL statement authorization

import pytest
import logging
import grp
from getpass import getuser
from os import getenv

from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.test_dimensions import create_uncompressed_text_dimension
from tests.util.filesystem_utils import IS_S3
from tests.util.test_file_parser import QueryTestSectionReader

SENTRY_CONFIG_FILE = getenv('IMPALA_HOME') + '/fe/src/test/resources/sentry-site.xml'

class TestGrantRevoke(CustomClusterTestSuite, ImpalaTestSuite):
  @classmethod
  def add_test_dimensions(cls):
    super(TestGrantRevoke, cls).add_test_dimensions()
    cls.TestMatrix.add_dimension(create_uncompressed_text_dimension(cls.get_workload()))

  @classmethod
  def get_workload(cls):
    return 'functional-query'

  def setup_method(self, method):
    super(TestGrantRevoke, self).setup_method(method)
    self.__test_cleanup()

  def teardown_method(self, method):
    self.__test_cleanup()
    super(TestGrantRevoke, self).teardown_method(method)

  def __test_cleanup(self):
    # Clean up any old roles created by this test
    for role_name in self.client.execute("show roles").data:
      if 'grant_revoke_test' in role_name:
        self.client.execute("drop role %s" % role_name)

    # Cleanup any other roles that were granted to this user.
    # TODO: Update Sentry Service config and authorization tests to use LocalGroupMapping
    # for resolving users -> groups. This way we can specify custom test users that don't
    # actually exist in the system.
    group_name = grp.getgrnam(getuser()).gr_name
    for role_name in self.client.execute("show role grant group `%s`" % group_name).data:
      self.client.execute("drop role %s" % role_name)

    # Create a temporary admin user so we can actually view/clean up the test
    # db.
    self.client.execute("create role grant_revoke_test_admin")
    try:
      self.client.execute("grant all on server to grant_revoke_test_admin")
      self.client.execute("grant role grant_revoke_test_admin to group %s" % group_name)
      self.cleanup_db('grant_rev_db', sync_ddl=0)
    finally:
      self.client.execute("drop role grant_revoke_test_admin")

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      impalad_args="--server_name=server1",
      catalogd_args="--sentry_config=" + SENTRY_CONFIG_FILE)
  def test_grant_revoke(self, vector):
    if IS_S3:
      self.run_test_case('QueryTest/grant_revoke_no_insert', vector, use_db="default")
    else:
      self.run_test_case('QueryTest/grant_revoke', vector, use_db="default")
