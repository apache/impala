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

from tests.common.custom_cluster_test_suite import CustomClusterTestSuite


class SentryCacheTestSuite(CustomClusterTestSuite):
  @staticmethod
  def str_to_bool(val):
    return val.lower() == 'true'

  @staticmethod
  def check_privileges(result, test_obj, null_create_date, show_user):
    """
    This method validates privileges. If the query is for "show grant user" then we need
    to account for the two extra columns.
    """
    # results should have the following columns for offset 0
    # scope, database, table, column, uri, privilege, grant_option, create_time
    # results should have the following columns for offset 2
    # principal name, principal type, scope, database, table, column, uri, privilege,
    # grant_option, create_time
    for row in result.data:
      col = row.split('\t')
      # If we're running show grant user, we ignore any columns that have "ROLE".
      # This is because this method currently only validates single user privileges.
      if show_user and col[0] == 'ROLE':
        continue
      if show_user:
        col = col[2:]
      assert col[0] == test_obj.grant_name
      assert col[1] == test_obj.db_name
      if test_obj.table_name is not None and len(test_obj.table_name) > 0:
        assert col[2] == test_obj.table_name
      assert SentryCacheTestSuite.str_to_bool(col[6]) == test_obj.grant
      if not null_create_date and str(col[7]) == 'NULL':
        return False
    return True

  def validate_privileges(self, client, query, test_obj, user=None,
                          refresh_authorization=False):
    """Validate privileges. When refresh_authorization is set to True, this function
    will call "refresh authorization" to ensure the privileges get refreshed from Sentry.
    """
    show_user = True if 'show grant user' in query else False
    if refresh_authorization: self.execute_query('refresh authorization')
    result = self.execute_query_expect_success(client, query, user=user)
    return SentryCacheTestSuite.check_privileges(result, test_obj,
                                                 null_create_date=(not
                                                                   refresh_authorization),
                                                 show_user=show_user)

  def user_query(self, client, query, user=None, error_msg=None):
    """
    Executes a query with the specified user client. If error_msg is set, then expect a
    failure. Returns None when there is no error_msg.
    """
    if error_msg is not None:
      e = self.execute_query_expect_failure(client, query, query_options={"sync_ddl": 1},
                                            user=user)
      SentryCacheTestSuite.verify_exceptions(error_msg, str(e))
      return None
    return self.execute_query_expect_success(client, query, query_options={"sync_ddl": 1},
                                             user=user)

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
