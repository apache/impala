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
import threading
from time import sleep
from tests.beeswax.impala_beeswax import ImpalaBeeswaxException
from tests.common.impala_connection import create_connection
from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.test_result_verifier import error_msg_expected


class QueryToKill:
  def __init__(self, test_suite, protocol, check_on_exit=True, user=None, nth_impalad=0):
    self.client = test_suite.create_client_for_nth_impalad(nth_impalad, protocol)
    self.sql = 'SELECT sleep(1000)'
    self.check_on_exit = check_on_exit
    self.user = user

  def poll(self):
    while True:
      try:
        results = self.client.fetch(self.sql, self.handle)
        if len(results.data) > 0:
          raise Exception("Failed to kill query within time limit.")
      except Exception as e:
        self.exc = e
        return

  def __enter__(self):
    self.handle = self.client.execute_async(self.sql, user=self.user)
    self.poll_thread = threading.Thread(target=lambda: self.poll())
    self.poll_thread.start()
    return self.client.get_query_id(self.handle)

  def __exit__(self, exc_type, exc_value, traceback):  # noqa: U100
    self.poll_thread.join()
    if not self.check_on_exit:
      self.client.close()
      return
    # If ImpalaServer::UnregisterQuery() happens before the last polling, the error
    # message will be "Invalid or unknown query handle". Otherwise, the error message
    # will be "Cancelled".
    assert error_msg_expected(
        str(self.exc),
        "Invalid or unknown query handle",
        self.client.get_query_id(self.handle),
    ) or error_msg_expected(
        str(self.exc),
        "Cancelled",
        self.client.get_query_id(self.handle),
    )
    try:
      self.client.fetch(self.sql, self.handle)
    except Exception as ex:
      assert "Invalid or unknown query handle" in str(ex)
    finally:
      self.client.close()


def assert_kill_ok(client, query_id, user=None):
  sql = "KILL QUERY '{0}'".format(query_id)
  result = client.execute(sql, user=user)
  assert result.success and len(result.data) == 1
  assert result.data[0] == "Query {0} is killed.".format(query_id)


def assert_kill_error(client, error_msg, query_id=None, sql=None, user=None):
  if sql is None:
    sql = "KILL QUERY '{0}'".format(query_id)
  try:
    client.execute(sql, user=user)
    assert False, "Failed to catch the exception."
  except Exception as exc:
    assert error_msg_expected(str(exc), error_msg)


def cancel_query_and_validate_state(client, query, exec_option, table_format,
    cancel_delay, join_before_close=False, use_kill_query_statement=False):
  """Runs the given query asynchronously and then cancels it after the specified delay.
  The query is run with the given 'exec_options' against the specified 'table_format'. A
  separate async thread is launched to fetch the results of the query. The method
  validates that the query was successfully cancelled and that the error messages for the
  calls to ImpalaConnection#fetch and #close are consistent. If 'join_before_close' is
  True the method will join against the fetch results thread before closing the query.

  If 'use_kill_query_statement' is True and 'join_before_close' is False, a KILL QUERY
  statement will be executed to cancel and close the query, instead of sending the Thrift
  RPCs directly.
  """
  assert not (join_before_close and use_kill_query_statement)

  if table_format: ImpalaTestSuite.change_database(client, table_format)
  if exec_option: client.set_configuration(exec_option)
  handle = client.execute_async(query)

  thread = threading.Thread(target=__fetch_results, args=(query, handle))
  thread.start()

  sleep(cancel_delay)
  if client.get_state(handle) == client.QUERY_STATES['EXCEPTION']:
      # If some error occurred before trying to cancel the query then we put an error
      # message together and fail the test.
      thread.join()
      error_msg = "The following query returned an error: %s\n" % query
      if thread.fetch_results_error is not None:
          error_msg += str(thread.fetch_results_error) + "\n"
      profile_lines = client.get_runtime_profile(handle).splitlines()
      for line in profile_lines:
          if "Query Status:" in line:
              error_msg += line
      assert False, error_msg
  if use_kill_query_statement:
    with create_connection(
        host_port=client.get_host_port(),
        protocol=client.get_test_protocol(),
    ) as kill_client:
      kill_client.connect()
      if exec_option:
        kill_client.set_configuration(exec_option)
      assert_kill_ok(kill_client, client.get_query_id(handle))
  else:
    cancel_result = client.cancel(handle)
    assert cancel_result.status_code == 0, \
        'Unexpected status code from cancel request: %s' % cancel_result

  if join_before_close:
    thread.join()

  close_error = None
  # The KILL QUERY statement will also close the query.
  if not use_kill_query_statement:
    try:
      client.close_query(handle)
    except ImpalaBeeswaxException as e:
      close_error = e

  # Before accessing fetch_results_error we need to join the fetch thread
  thread.join()

  # IMPALA-9756: Make sure query summary info has been added to profile for queries
  # that proceeded far enough into execution that it should have been added to profile.
  # The logic in ClientRequestState/Coordinator is convoluted, but the summary info
  # should be added if the query has got to the point where rows can be fetched. We
  # need to do this after both close_query() and fetch() have returned to ensure
  # that the synchronous phase of query unregistration has finished and the profile
  # is final.
  profile = client.get_runtime_profile(handle)
  if ("- Completed admission: " in profile and
      ("- First row fetched:" in profile or "- Request finished:" in profile)):
    # TotalBytesRead is a sentinel that will only be created if ComputeQuerySummary()
    # has been run by the cancelling thread.
    assert "- TotalBytesRead:" in profile, profile

  if thread.fetch_results_error is None:
    # If the fetch rpc didn't result in CANCELLED (and auto-close the query) then
    # the close rpc should have succeeded.
    assert close_error is None
  elif close_error is None:
    # If the close rpc succeeded, then the fetch rpc should have either succeeded,
    # failed with 'Cancelled' or failed with 'Invalid or unknown query handle'
    # (if the close rpc occured before the fetch rpc).
    if thread.fetch_results_error is not None:
      assert 'Cancelled' in str(thread.fetch_results_error) or \
        ('Invalid or unknown query handle' in str(thread.fetch_results_error)
         and not join_before_close), str(thread.fetch_results_error)
  else:
    # If the close rpc encountered an exception, then it must be due to fetch
    # noticing the cancellation and doing the auto-close.
    assert 'Invalid or unknown query handle' in str(close_error)
    assert 'Cancelled' in str(thread.fetch_results_error)

  # TODO: Add some additional verification to check to make sure the query was
  # actually canceled


def __fetch_results(query, handle):
  threading.current_thread().fetch_results_error = None
  threading.current_thread().query_profile = None
  try:
    new_client = ImpalaTestSuite.create_impala_client()
    new_client.fetch(query, handle)
  except ImpalaBeeswaxException as e:
    threading.current_thread().fetch_results_error = e
