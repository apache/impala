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
# Base class for test that need to run with both just privilege
# cache as well as Sentry privilege refresh.
# There are two variables (delay_s timeout) used in these methods.
# The first is the timeout when validating privileges. This is
# needed to ensure that the privileges returned have been updated
# from Sentry. The second is the delay_s before executing a query.
# This is needed to ensure Sentry has been updated before running
# the query. The reason for both is because the timeout can
# be short circuited upon successful results. Using the delay_s
# for every query and test would add significant time. As an
# example, if a revoke is called, the expectation is the privilege
# would not be in the result. If the cache is updated correctly,
# but Sentry was not, using the timeout check, we would miss that
# Sentry was not updated correctly.

from time import sleep, time
from tests.common.custom_cluster_test_suite import CustomClusterTestSuite


class SentryCacheTestSuite(CustomClusterTestSuite):
  @staticmethod
  def str_to_bool(val):
    return val.lower() == 'true'

  @staticmethod
  def __check_privileges(result, test_obj, null_create_date=True):
    """
    This method validates privileges. Most validations are assertions, but for
    null_create_date, we just return False to indicate the privilege cannot
    be validated because it has not been refreshed from Sentry yet.
    """
    # results should have the following columns
    # scope, database, table, column, uri, privilege, grant_option, create_time
    for row in result.data:
      col = row.split('\t')
      assert col[0] == test_obj.grant_name
      assert col[1] == test_obj.db_name
      if test_obj.table_name is not None and len(test_obj.table_name) > 0:
        assert col[2] == test_obj.table_name
      assert SentryCacheTestSuite.str_to_bool(col[6]) == test_obj.grant
      if not null_create_date and str(col[7]) == 'NULL':
        return False
    return True

  def validate_privileges(self, client, query, test_obj, timeout_sec=None, user=None,
      delay_s=0):
    """Validate privileges. If timeout_sec is > 0 then retry until create_date is not null
    or the timeout_sec is reached. If delay_s is > 0 then wait that long before running.
    """
    if delay_s > 0:
      sleep(delay_s)
    if timeout_sec is None or timeout_sec <= 0:
      self.__check_privileges(self.execute_query_expect_success(client, query,
          user=user), test_obj)
    else:
      start_time = time()
      while time() - start_time < timeout_sec:
        result = self.execute_query_expect_success(client, query, user=user)
        success = self.__check_privileges(result, test_obj, null_create_date=False)
        if success:
          return True
        sleep(1)
      return False

  def user_query(self, client, query, user=None, delay_s=0, error_msg=None):
    """
    Executes a query with the root user client. If delay_s is > 0 then wait before
    running the query. This is used to wait for Sentry refresh. If error_msg is
    set, then expect a failure. Returns None when there is no error_msg.
    """
    if delay_s > 0:
      sleep(delay_s)
    if error_msg is not None:
      e = self.execute_query_expect_failure(client, query, user=user)
      self.verify_exceptions(error_msg, str(e))
      return None
    return self.execute_query_expect_success(client, query, user=user)

  @staticmethod
  def verify_exceptions(expected_str, actual_str):
    """
    Verifies that 'expected_str' is a substring of the actual exception string
    'actual_str'.
    """
    actual_str = actual_str.replace('\n', '')
    # Strip newlines so we can split error message into multiple lines
    expected_str = expected_str.replace('\n', '')
    if expected_str in actual_str: return
    assert False, 'Unexpected exception string. Expected: %s\nNot found in actual: %s' % \
      (expected_str, actual_str)


class TestObject(object):
  SERVER = "server"
  DATABASE = "database"
  TABLE = "table"
  VIEW = "view"

  def __init__(self, obj_type, obj_name="", grant=False):
    self.obj_name = obj_name
    self.obj_type = obj_type
    parts = obj_name.split(".")
    self.db_name = parts[0]
    self.table_name = None
    self.table_def = ""
    self.view_select = ""
    if len(parts) > 1:
      self.table_name = parts[1]
    if obj_type == TestObject.VIEW:
      self.grant_name = TestObject.TABLE
      self.view_select = "as select * from functional.alltypes"
    elif obj_type == TestObject.TABLE:
      self.grant_name = TestObject.TABLE
      self.table_def = "(col1 int)"
    else:
      self.grant_name = obj_type
    self.grant = grant
