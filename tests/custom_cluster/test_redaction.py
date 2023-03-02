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
import getpass
import logging
import os
import pytest
import re
import shutil
import unittest

from tempfile import mkdtemp
from time import sleep

from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.file_utils import grep_file, assert_file_in_dir_contains,\
    assert_no_files_in_dir_contain

LOG = logging.getLogger(__name__)

# This class needs to inherit from unittest.TestCase otherwise py.test will ignore it
# because a __init__ method is used.
class TestRedaction(CustomClusterTestSuite, unittest.TestCase):
  '''Test various redaction related functionality.

     Redaction is about preventing sensitive data from leaking into logs, the web ui,
     or any other place that is not a result set. The definition of sensitive data is
     limited to table data and query text since queries may refer to table data.
  '''

  @classmethod
  def get_workload(cls):
    return 'functional-query'

  def __init__(self, *args, **kwargs):
    super(TestRedaction, self).__init__(*args, **kwargs)

    # Parent dir for various file output such as logging. The value is set to a new value
    # just before each test run.
    self.tmp_dir = None

  @property
  def log_dir(self):
    return os.path.join(self.tmp_dir, "logs")

  @property
  def audit_dir(self):
    return os.path.join(self.tmp_dir, "audits")

  @property
  def profile_dir(self):
    return os.path.join(self.tmp_dir, "profiles")

  @property
  def rules_file(self):
    return os.path.join(self.tmp_dir, "redaction_rules.json")

  def setup_method(self, method):
    # Override parent
    pass

  def teardown_method(self, method):
    # Parent method would fail, nothing needs to be done. The tests are responsible
    # for deleting self.tmp_dir after tests pass.
    pass

  def start_cluster_using_rules(self, redaction_rules, log_level=2, vmodule=""):
    '''Start Impala with a custom log dir and redaction rules.'''
    self.tmp_dir = mkdtemp(prefix="test_redaction_", dir=os.getenv("LOG_DIR"))
    os.chmod(self.tmp_dir, 0o777)
    LOG.info("tmp_dir is " + self.tmp_dir)
    os.mkdir(self.log_dir)
    os.mkdir(self.audit_dir)
    os.mkdir(self.profile_dir)

    # Write the redaction rules as set in @using_redaction_rules.
    with open(self.rules_file, 'w') as file:
      file.write(redaction_rules)

    self._start_impala_cluster(
        ["""--impalad_args=-audit_event_log_dir=%s
                           -profile_log_dir=%s
                           -redaction_rules_file=%s
                           -vmodule=%s"""
            % (self.audit_dir, self.profile_dir, self.rules_file, vmodule)],
        impala_log_dir=self.log_dir,
        log_level=log_level)
    self.client = self.create_impala_client()

  def find_last_query_id(self):
    '''Return the id of the most recent query. Usually the id can be obtained through
       the API but if query analysis fails the id is not available.
    '''
    # Scrape the web ui....
    # TODO: The HS2 interface may be better about exposing the query handle even if a
    #       query fails. Maybe investigate that after the switch to HS2.
    regex = re.compile(r'query_id=(\w+:\w+)')
    match = regex.search(self.create_impala_service().read_debug_webpage('queries'))
    if match:
      return match.group(1)
    raise Exception('Unable to find any query id')

  def assert_server_fails_to_start(self, rules, start_options, expected_error_message):
    try:
      self.start_cluster_using_rules(rules, **start_options)
      self.fail('Cluster should not have started but did')
    except Exception:
      if self.cluster.impalads:
        raise Exception("No impalads should have started")
    with open(os.path.join(self.log_dir, 'impalad-out.log')) as file:
      result = grep_file(file, expected_error_message)
    assert result, 'The expected error message was not found'

  def assert_log_redaction(self, unredacted_value, redacted_value, expect_audit=True):
    '''Asserts that the 'unredacted_value' is not present but the 'redacted_value' is.'''
    # Logs should not contain the unredacted value.
    assert_no_files_in_dir_contain(self.log_dir, unredacted_value)
    assert_no_files_in_dir_contain(self.audit_dir, unredacted_value)
    assert_no_files_in_dir_contain(self.profile_dir, unredacted_value)
    # But the redacted value should be there except for the profile since that is
    # encoded.
    assert_file_in_dir_contains(self.log_dir, redacted_value)
    if expect_audit:
      assert_file_in_dir_contains(self.audit_dir, redacted_value)

  def assert_web_ui_redaction(self, query_id, unredacted_value, redacted_value):
    '''Asserts that the 'unredacted_value' is not present but the 'redacted_value' is.'''
    impala_service = self.create_impala_service()
    # The web ui should not show the unredacted value.
    for page in ('queries', 'query_stmt', 'query_plan_text', 'query_summary',
        'query_profile', 'query_plan'):
      for response_format in ('html', 'json'):
        # The 'html' param is actually ignored by the server.
        url = page + '?query_id=' + query_id + "&" + response_format
        assert unredacted_value not in impala_service.read_debug_webpage(url), \
            "Web page %s should not contain '%s' but does" % (url, unredacted_value)
    # But the redacted value should be shown.
    self.assert_web_ui_contains(query_id, redacted_value)

  def assert_web_ui_contains(self, query_id, search):
    '''Asserts that the 'search' term is present in all the pages that show user queries.
    '''
    impala_service = self.create_impala_service()
    for page in ('queries', 'query_stmt', 'query_plan_text', 'query_profile'):
      url = '%s?query_id=%s' % (page, query_id)
      assert search in impala_service.read_debug_webpage(url), \
          "Web page %s should contain '%s' but does not" % (url, search)

  def assert_query_profile_contains(self, query_id, search):
    ''' Asserts that the query profile for 'query_id' contains 'search' string'''
    impala_service = self.create_impala_service()
    url = 'query_profile?query_id=%s' % query_id
    assert search in impala_service.read_debug_webpage(url), \
        "Query profile %s should contain '%s' but does not" % (url, search)

  @pytest.mark.execute_serially
  def test_bad_rules(self):
    '''Check that the server fails to start if the redaction rules are bad.'''
    if self.exploration_strategy() != 'exhaustive':
      pytest.skip('runs only in exhaustive')
    startup_options = dict()
    self.assert_server_fails_to_start('{ "version": 100 }', startup_options,
        'Error parsing redaction rules; only version 1 is supported')
    # Since the tests passed, the log dir shouldn't be of interest and can be deleted.
    shutil.rmtree(self.tmp_dir)

  @pytest.mark.execute_serially
  def test_very_verbose_logging(self):
    '''Check that the server fails to start if logging is configured at a level that
       could dump table data. Row logging would be enabled with "-v=3" or could be
       enabled  with the -vmodule option. In either case the server should not start.
    '''
    if self.exploration_strategy() != 'exhaustive':
      pytest.skip('runs only in exhaustive')
    rules = r"""
        {
          "version": 1,
          "rules": [
            {
              "description": "Don't show emails",
              "caseSensitive": false,
              "search": "[a-z]+@[a-z]+.[a-z]{3}",
              "replace": "*email*"
            }
          ]
        }"""
    error_message = "Redaction cannot be used in combination with log level 3 or " \
        "higher or the -vmodule option"
    self.assert_server_fails_to_start(rules, {"log_level": 3}, error_message)
    # Since the tests passed, the log dir shouldn't be of interest and can be deleted.
    shutil.rmtree(self.tmp_dir)

    self.assert_server_fails_to_start(rules, {"vmodule": "foo"}, error_message)
    shutil.rmtree(self.tmp_dir)

    self.assert_server_fails_to_start(
        rules, {"log_level": 3, "vmodule": "foo"}, error_message)
    shutil.rmtree(self.tmp_dir)

  @pytest.mark.execute_serially
  def test_unredacted(self):
    '''Do a sanity check to verify that the system behaves as expected when no redaction
       rules are set. The expectation is the full query text will show up in the logs
       and the web ui.
    '''
    if self.exploration_strategy() != 'exhaustive':
      pytest.skip('runs only in exhaustive')
    self.start_cluster_using_rules('')
    email = 'foo@bar.com'
    self.execute_query_expect_success(self.client,
        "SELECT COUNT(*) FROM functional.alltypes WHERE string_col = '%s'" % email)

    # The query should also be found in the web ui.
    self.assert_web_ui_contains(self.find_last_query_id(), email)

    # Wait for the audit logs to be written. 5 seconds is an arbitrary value, typically
    # only a second is needed.
    sleep(5)
    # The query should show up in both the audit and non-audit logs
    assert_file_in_dir_contains(self.log_dir, email)
    assert_file_in_dir_contains(self.audit_dir, email)
    # The profile is encoded so the email won't be found.
    assert_no_files_in_dir_contain(self.profile_dir, email)

    # Since all the tests passed, the log dir shouldn't be of interest and can be
    # deleted.
    shutil.rmtree(self.tmp_dir)

  @pytest.mark.execute_serially
  def test_redacted(self):
    '''Check that redaction rules prevent 'sensitive' data from leaking into the
       logs and web ui.
    '''
    current_user = getpass.getuser()
    self.start_cluster_using_rules(r"""
      {
        "version": 1,
        "rules": [
          {
            "description": "Don't show emails",
            "caseSensitive": false,
            "search": "[a-z]+@[a-z]+.[a-z]{3}",
            "replace": "*email*"
          },
          {
            "description": "Don't show credit cards numbers",
            "search": "\\d{4}-\\d{4}-\\d{4}-\\d{4}",
            "replace": "*credit card*"
          },
          {
            "description": "Don't show current username in queries",
            "search": "%s",
            "replace": "redacted user"
          }
        ]
      }""" % current_user)
    email = 'FOO@bar.com'
    # GROUP BY an expr containing the email so the expr will also be shown in the exec
    # node summary, ie HASH(string_col = ...).
    query_template =\
        "SELECT string_col = '%s', COUNT(*) FROM functional.alltypes GROUP BY 1"
    self.execute_query_expect_success(self.client, query_template % email)

    user_profile_pattern = "User: %s" % current_user
    # The email should be replaced with '*email*'.
    self.assert_web_ui_redaction(self.find_last_query_id(), email, "*email*")
    # User field should not be redacted from the query profile.
    self.assert_query_profile_contains(self.find_last_query_id(), user_profile_pattern)
    # Wait for the logs to be written.
    sleep(5)
    self.assert_log_redaction(email, "\*email\*")

    # Even if the query is invalid, redaction should still be applied.
    credit_card = '1234-5678-1234-5678'
    self.execute_query_expect_failure(self.client, credit_card)
    # This assertion below relies on the fact that there is a syntax error be near
    # the credit card number so the number would have appeared in the message.
    self.assert_web_ui_redaction(self.find_last_query_id(), credit_card, "*credit card*")
    # User field should not be redacted from the query profile.
    self.assert_query_profile_contains(self.find_last_query_id(), user_profile_pattern)
    sleep(5)
    # Apparently an invalid query doesn't generate an audit log entry.
    self.assert_log_redaction(credit_card, "\*credit card\*", expect_audit=False)

    # Assert that the username in the query stmt is redacted but not from the user fields.
    self.execute_query_expect_success(self.client, query_template % current_user)
    self.assert_query_profile_contains(self.find_last_query_id(), user_profile_pattern)
    self.assert_query_profile_contains(self.find_last_query_id(), "redacted user")

    # Since all the tests passed, the log dir shouldn't be of interest and can be
    # deleted.
    shutil.rmtree(self.tmp_dir)
