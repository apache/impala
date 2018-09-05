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

import logging
import pytest
import psutil
import re
import socket
import time

from tests.common.environ import build_flavor_timeout
from time import sleep

from impala.error import HiveServer2Error
from TCLIService import TCLIService

from tests.beeswax.impala_beeswax import ImpalaBeeswaxException
from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.skip import SkipIfEC
from tests.hs2.hs2_test_suite import HS2TestSuite, needs_session

LOG = logging.getLogger(__name__)

class TestRestart(CustomClusterTestSuite):
  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @pytest.mark.execute_serially
  def test_restart_statestore(self, cursor):
    """ Regression test of IMPALA-6973. After the statestore restarts, the metadata should
        eventually recover after being cleared by the new statestore.
    """

    self.cluster.statestored.restart()
    # We need to wait for the impalad to register to the new statestored and for a
    # non-empty catalog update from the new statestored. It cannot be expressed with the
    # existing metrics yet so we wait for some time here.
    wait_time_s = build_flavor_timeout(60, slow_build_timeout=100)
    sleep(wait_time_s)
    for retry in xrange(wait_time_s):
      try:
        cursor.execute("describe database functional")
        return
      except HiveServer2Error, e:
        assert "AnalysisException: Database does not exist: functional" in e.message,\
               "Unexpected exception: " + e.message
        sleep(1)
    assert False, "Coordinator never received non-empty metadata from the restarted " \
           "statestore after {0} seconds".format(wait_time_s)

  @pytest.mark.execute_serially
  def test_restart_impala(self):
      """ This test aims to restart Impalad executor nodes between queries to exercise
      the cluster membership callback which removes stale connections to the restarted
      nodes."""

      self._start_impala_cluster([], num_coordinators=1, cluster_size=3)
      assert len(self.cluster.impalads) == 3

      client = self.cluster.impalads[0].service.create_beeswax_client()
      assert client is not None

      for i in xrange(5):
        self.execute_query_expect_success(client, "select * from functional.alltypes")
        node_to_restart = 1 + (i % 2)
        self.cluster.impalads[node_to_restart].restart()
        # Sleep for a bit for the statestore change in membership to propagate. The min
        # update frequency for statestore is 100ms but using a larger sleep time here
        # as certain builds (e.g. ASAN) can be really slow.
        sleep(3)

      client.close()


def parse_shutdown_result(result):
  """Parse the shutdown result string and return the strings (grace left,
  deadline left, fragment instances, queries registered)."""
  assert len(result.data) == 1
  summary = result.data[0]
  match = re.match(r'startup grace period left: ([0-9ms]*), deadline left: ([0-9ms]*), ' +
      r'fragment instances: ([0-9]*), queries registered: ([0-9]*)', summary)
  assert match is not None, summary
  return match.groups()


class TestShutdownCommand(CustomClusterTestSuite, HS2TestSuite):
  IDLE_SHUTDOWN_GRACE_PERIOD_S = 1

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      impalad_args="--shutdown_grace_period_s={grace_period} \
          --hostname={hostname}".format(grace_period=IDLE_SHUTDOWN_GRACE_PERIOD_S,
            hostname=socket.gethostname()))
  def test_shutdown_idle(self):
    """Test that idle impalads shut down in a timely manner after the startup grace period
    elapses."""
    impalad1 = psutil.Process(self.cluster.impalads[0].get_pid())
    impalad2 = psutil.Process(self.cluster.impalads[1].get_pid())
    impalad3 = psutil.Process(self.cluster.impalads[2].get_pid())

    # Test that a failed shut down from a bogus host or port fails gracefully.
    ex = self.execute_query_expect_failure(self.client,
        ":shutdown('e6c00ca5cd67b567eb96c6ecfb26f05')")
    assert "Couldn't open transport" in str(ex)
    ex = self.execute_query_expect_failure(self.client, ":shutdown('localhost:100000')")
    assert "Couldn't open transport" in str(ex)
    # Test that pointing to the wrong thrift service (the HS2 port) fails gracefully.
    ex = self.execute_query_expect_failure(self.client, ":shutdown('localhost:21050')")
    assert ("RPC Error: Client for localhost:21050 hit an unexpected exception: " +
            "Invalid method name: 'RemoteShutdown'") in str(ex)
    # Test RPC error handling with debug action.
    ex = self.execute_query_expect_failure(self.client, ":shutdown('localhost:22001')",
        query_options={'debug_action': 'CRS_SHUTDOWN_RPC:FAIL'})
    assert 'Debug Action: CRS_SHUTDOWN_RPC:FAIL' in str(ex)

    # Test remote shutdown.
    LOG.info("Start remote shutdown {0}".format(time.time()))
    self.execute_query_expect_success(self.client, ":shutdown('localhost:22001')",
        query_options={})

    # Remote shutdown does not require statestore.
    self.cluster.statestored.kill()
    self.cluster.statestored.wait_for_exit()
    self.execute_query_expect_success(self.client, ":shutdown('localhost:22002')",
        query_options={})

    # Test local shutdown, which should succeed even with injected RPC error.
    LOG.info("Start local shutdown {0}".format(time.time()))
    self.execute_query_expect_success(self.client,
        ":shutdown('{0}:22000')".format(socket.gethostname()),
        query_options={'debug_action': 'CRS_SHUTDOWN_RPC:FAIL'})

    # Make sure that the impala daemons exit after the startup grace period plus a 10
    # second margin of error.
    start_time = time.time()
    LOG.info("Waiting for impalads to exit {0}".format(start_time))
    impalad1.wait()
    LOG.info("First impalad exited {0}".format(time.time()))
    impalad2.wait()
    LOG.info("Second impalad exited {0}".format(time.time()))
    impalad3.wait()
    LOG.info("Third impalad exited {0}".format(time.time()))
    shutdown_duration = time.time() - start_time
    assert shutdown_duration <= self.IDLE_SHUTDOWN_GRACE_PERIOD_S + 10

  EXEC_SHUTDOWN_GRACE_PERIOD_S = 5
  EXEC_SHUTDOWN_DEADLINE_S = 10

  @pytest.mark.execute_serially
  @SkipIfEC.scheduling
  @CustomClusterTestSuite.with_args(
      impalad_args="--shutdown_grace_period_s={grace_period} \
          --shutdown_deadline_s={deadline} \
          --hostname={hostname}".format(grace_period=EXEC_SHUTDOWN_GRACE_PERIOD_S,
            deadline=EXEC_SHUTDOWN_DEADLINE_S, hostname=socket.gethostname()))
  def test_shutdown_executor(self):
    """Test that shuts down and then restarts an executor. This should not disrupt any
    queries that start after the shutdown or complete before the shutdown time limit."""
    # Add sleeps to make sure that the query takes a couple of seconds to execute on the
    # executors.
    QUERY = "select count(*) from functional_parquet.alltypes where sleep(1) = bool_col"
    # Subtle: use a splittable file format like text for lineitem so that each backend
    # is guaranteed to get scan ranges that contain some actual rows. With Parquet on
    # S3, the files get broken into 32MB scan ranges and a backend might get unlucky
    # and only get scan ranges that don't contain the midpoint of any row group, and
    # therefore not actually produce any rows.
    SLOW_QUERY = "select count(*) from tpch.lineitem where sleep(1) = l_orderkey"
    SHUTDOWN_EXEC2 = ": shutdown('localhost:22001')"

    # Run this query before shutdown and make sure that it executes successfully on
    # all executors through the startup grace period without disruption.
    before_shutdown_handle = self.__exec_and_wait_until_running(QUERY)

    # Shut down and wait for the shutdown state to propagate through statestore.
    result = self.execute_query_expect_success(self.client, SHUTDOWN_EXEC2)
    assert parse_shutdown_result(result) == (
        "{0}s000ms".format(self.EXEC_SHUTDOWN_GRACE_PERIOD_S),
        "{0}s000ms".format(self.EXEC_SHUTDOWN_DEADLINE_S), "1", "0")

    # Check that the status is reflected on the debug page.
    web_json = self.cluster.impalads[1].service.get_debug_webpage_json("")
    assert web_json.get('is_quiescing', None) is True, web_json
    assert 'shutdown_status' in web_json, web_json

    self.impalad_test_service.wait_for_num_known_live_backends(2,
        timeout=self.EXEC_SHUTDOWN_GRACE_PERIOD_S + 5, interval=0.2,
        include_shutting_down=False)

    # Run another query, which shouldn't get scheduled on the new executor. We'll let
    # this query continue running through the full shutdown and restart cycle.
    after_shutdown_handle = self.__exec_and_wait_until_running(QUERY)

    # Finish executing the first query before the backend exits.
    assert self.__fetch_and_get_num_backends(QUERY, before_shutdown_handle) == 3

    # Wait for the impalad to exit, then start it back up and run another query, which
    # should be scheduled on it again.
    self.cluster.impalads[1].wait_for_exit()
    self.cluster.impalads[1].start()
    self.impalad_test_service.wait_for_num_known_live_backends(
        3, timeout=30, interval=0.2, include_shutting_down=False)
    after_restart_handle = self.__exec_and_wait_until_running(QUERY)

    # The query started while the backend was shut down should not run on that backend.
    assert self.__fetch_and_get_num_backends(QUERY, after_shutdown_handle) == 2
    assert self.__fetch_and_get_num_backends(QUERY, after_restart_handle) == 3

    # Test that a query will fail when the executor shuts down after the limit.
    deadline_expiry_handle = self.__exec_and_wait_until_running(SLOW_QUERY)
    result = self.execute_query_expect_success(self.client, SHUTDOWN_EXEC2)
    assert parse_shutdown_result(result) == (
        "{0}s000ms".format(self.EXEC_SHUTDOWN_GRACE_PERIOD_S),
        "{0}s000ms".format(self.EXEC_SHUTDOWN_DEADLINE_S), "1", "0")
    self.cluster.impalads[1].wait_for_exit()
    self.__check_deadline_expired(SLOW_QUERY, deadline_expiry_handle)

    # Test that we can reduce the deadline after setting it to a high value.
    # Run a query that will fail as a result of the reduced deadline.
    deadline_expiry_handle = self.__exec_and_wait_until_running(SLOW_QUERY)
    SHUTDOWN_EXEC3 = ": shutdown('localhost:22002', {0})"
    VERY_HIGH_DEADLINE = 5000
    HIGH_DEADLINE = 1000
    LOW_DEADLINE = 5
    result = self.execute_query_expect_success(
        self.client, SHUTDOWN_EXEC3.format(HIGH_DEADLINE))
    grace, deadline, _, _ = parse_shutdown_result(result)
    assert grace == "{0}s000ms".format(self.EXEC_SHUTDOWN_GRACE_PERIOD_S)
    assert deadline == "{0}m{1}s".format(HIGH_DEADLINE / 60, HIGH_DEADLINE % 60)

    result = self.execute_query_expect_success(
        self.client, SHUTDOWN_EXEC3.format(VERY_HIGH_DEADLINE))
    _, deadline, _, _ = parse_shutdown_result(result)
    LOG.info("Deadline is {0}".format(deadline))
    min_string, sec_string = re.match("([0-9]*)m([0-9]*)s", deadline).groups()
    assert int(min_string) * 60 + int(sec_string) <= HIGH_DEADLINE, \
        "Cannot increase deadline " + deadline

    result = self.execute_query_expect_success(
        self.client, SHUTDOWN_EXEC3.format(LOW_DEADLINE))
    _, deadline, finstances, _ = parse_shutdown_result(result)
    assert deadline == "{0}s000ms".format(LOW_DEADLINE)
    assert int(finstances) > 0, "Slow query should still be running."
    self.cluster.impalads[2].wait_for_exit()
    self.__check_deadline_expired(SLOW_QUERY, deadline_expiry_handle)

  COORD_SHUTDOWN_GRACE_PERIOD_S = 5
  COORD_SHUTDOWN_DEADLINE_S = 120

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      impalad_args="--shutdown_grace_period_s={grace_period} \
          --shutdown_deadline_s={deadline} \
          --hostname={hostname}".format(
          grace_period=COORD_SHUTDOWN_GRACE_PERIOD_S,
          deadline=COORD_SHUTDOWN_DEADLINE_S, hostname=socket.gethostname()),
      default_query_options=[("num_scanner_threads", "1")])
  @needs_session(TCLIService.TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V6,
                 close_session=False)
  def test_shutdown_coordinator(self):
    """Test that shuts down the coordinator. Running queries should finish but new
    requests should be rejected."""
    # Start a query running. This should complete successfully and keep the coordinator
    # up until it finishes. We set NUM_SCANNER_THREADS=1 above to make the runtime more
    # predictable.
    SLOW_QUERY = """select * from tpch_parquet.lineitem where sleep(1) < l_orderkey"""
    SHUTDOWN = ": shutdown()"
    SHUTDOWN_ERROR_PREFIX = 'Server is being shut down:'

    before_shutdown_handle = self.__exec_and_wait_until_running(SLOW_QUERY)
    before_shutdown_hs2_handle = self.execute_statement(SLOW_QUERY).operationHandle

    # Shut down the coordinator. Operations that start after this point should fail.
    result = self.execute_query_expect_success(self.client, SHUTDOWN)
    grace, deadline, _, registered = parse_shutdown_result(result)
    assert grace == "{0}s000ms".format(self.COORD_SHUTDOWN_GRACE_PERIOD_S)
    assert deadline == "{0}m".format(self.COORD_SHUTDOWN_DEADLINE_S / 60), "4"
    assert registered == "3"

    # Expect that the beeswax shutdown error occurs when calling fn()
    def expect_beeswax_shutdown_error(fn):
      try:
        fn()
      except ImpalaBeeswaxException, e:
        assert SHUTDOWN_ERROR_PREFIX in str(e)
    expect_beeswax_shutdown_error(lambda: self.client.execute("select 1"))
    expect_beeswax_shutdown_error(lambda: self.client.execute_async("select 1"))

    # Test that the HS2 shutdown error occurs for various HS2 operations.
    self.execute_statement("select 1", None, TCLIService.TStatusCode.ERROR_STATUS,
        SHUTDOWN_ERROR_PREFIX)

    def check_hs2_shutdown_error(hs2_response):
      HS2TestSuite.check_response(hs2_response, TCLIService.TStatusCode.ERROR_STATUS,
        SHUTDOWN_ERROR_PREFIX)
    check_hs2_shutdown_error(self.hs2_client.OpenSession(TCLIService.TOpenSessionReq()))
    check_hs2_shutdown_error(self.hs2_client.GetInfo(TCLIService.TGetInfoReq(
        self.session_handle, TCLIService.TGetInfoType.CLI_MAX_DRIVER_CONNECTIONS)))
    check_hs2_shutdown_error(self.hs2_client.GetTypeInfo(
        TCLIService.TGetTypeInfoReq(self.session_handle)))
    check_hs2_shutdown_error(self.hs2_client.GetCatalogs(
        TCLIService.TGetCatalogsReq(self.session_handle)))
    check_hs2_shutdown_error(self.hs2_client.GetSchemas(
        TCLIService.TGetSchemasReq(self.session_handle)))
    check_hs2_shutdown_error(self.hs2_client.GetTables(
        TCLIService.TGetTablesReq(self.session_handle)))
    check_hs2_shutdown_error(self.hs2_client.GetTableTypes(
        TCLIService.TGetTableTypesReq(self.session_handle)))
    check_hs2_shutdown_error(self.hs2_client.GetColumns(
        TCLIService.TGetColumnsReq(self.session_handle)))
    check_hs2_shutdown_error(self.hs2_client.GetFunctions(
        TCLIService.TGetFunctionsReq(self.session_handle, functionName="")))

    # Operations on running HS2 query still work.
    self.fetch_until(before_shutdown_hs2_handle,
        TCLIService.TFetchOrientation.FETCH_NEXT, 10)
    HS2TestSuite.check_response(self.hs2_client.CancelOperation(
        TCLIService.TCancelOperationReq(before_shutdown_hs2_handle)))
    HS2TestSuite.check_response(self.hs2_client.CloseOperation(
        TCLIService.TCloseOperationReq(before_shutdown_hs2_handle)))

    # Make sure that the beeswax query is still executing, then close it to allow the
    # coordinator to shut down.
    self.impalad_test_service.wait_for_query_state(self.client, before_shutdown_handle,
          self.client.QUERY_STATES['FINISHED'], timeout=20)
    self.client.close_query(before_shutdown_handle)
    self.cluster.impalads[0].wait_for_exit()

  def __exec_and_wait_until_running(self, query, timeout=20):
    """Execute 'query' with self.client and wait until it is in the RUNNING state.
    'timeout' controls how long we will wait"""
    # Fix number of scanner threads to make runtime more deterministic.
    handle = self.execute_query_async(query, {'num_scanner_threads': 1})
    self.impalad_test_service.wait_for_query_state(self.client, handle,
                self.client.QUERY_STATES['RUNNING'], timeout=20)
    return handle

  def __fetch_and_get_num_backends(self, query, handle):
    """Fetch the results of 'query' from the beeswax handle 'handle', close the
    query and return the number of backends obtained from the profile."""
    self.impalad_test_service.wait_for_query_state(self.client, handle,
                self.client.QUERY_STATES['FINISHED'], timeout=20)
    self.client.fetch(query, handle)
    profile = self.client.get_runtime_profile(handle)
    self.client.close_query(handle)
    backends_match = re.search("NumBackends: ([0-9]*)", profile)
    assert backends_match is not None, profile
    return int(backends_match.group(1))

  def __check_deadline_expired(self, query, handle):
    """Check that the query with 'handle' fails because of a backend hitting the
    deadline and shutting down."""
    try:
      self.client.fetch(query, handle)
      assert False, "Expected query to fail"
    except Exception, e:
      assert 'Failed due to unreachable impalad(s)' in str(e)
