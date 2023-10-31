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
from tests.common.impala_test_suite import ImpalaTestSuite


def cancel_query_and_validate_state(client, query, exec_option, table_format,
    cancel_delay, join_before_close=False):
  """Runs the given query asynchronously and then cancels it after the specified delay.
  The query is run with the given 'exec_options' against the specified 'table_format'. A
  separate async thread is launched to fetch the results of the query. The method
  validates that the query was successfully cancelled and that the error messages for the
  calls to ImpalaConnection#fetch and #close are consistent. If 'join_before_close' is
  True the method will join against the fetch results thread before closing the query.
  """
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
  cancel_result = client.cancel(handle)
  assert cancel_result.status_code == 0,\
      'Unexpected status code from cancel request: %s' % cancel_result

  if join_before_close:
    thread.join()

  close_error = None
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
