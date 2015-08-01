# Copyright (c) 2014 Cloudera, Inc. All rights reserved.
# Tests admission control

import pytest
import threading
import re

from time import sleep, time
from tests.beeswax.impala_beeswax import ImpalaBeeswaxException
from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.impala_cluster import ImpalaCluster
from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.test_dimensions import create_single_exec_option_dimension
from tests.common.test_dimensions import create_uncompressed_text_dimension
from tests.common.test_vector import TestDimension

import logging
import os

LOG = logging.getLogger('admission_test')

# We set a WAIT debug action so it doesn't complete the execution of this query. The
# limit is a parameter for debugging purposes; each thread will insert its id so
# that running queries can be correlated with the thread that submitted them.
QUERY = "select * from alltypes limit %s"

# Time to sleep (in milliseconds) between issuing queries. The default statestore
# heartbeat is 500ms, so the lower the delay the more we can submit before the global
# state is updated. When the delay is at least the statestore heartbeat frequency, all
# state should be visible by every impalad by the time the next query is submitted.
SUBMISSION_DELAY_MS = [0, 50, 100, 600]

# The number of queries to submit. The test does not support fewer queries than
# MAX_NUM_CONCURRENT_QUERIES + MAX_NUM_QUEUED_QUERIES to keep some validation logic
# simple.
NUM_QUERIES = [15, 30, 50]

# Whether we will submit queries to all available impalads (in a round-robin fashion)
ROUND_ROBIN_SUBMISSION = [True, False]

# The query pool to use. The impalads should be configured to recognize this
# pool with the parameters below.
POOL_NAME = "default-pool"

# The statestore heartbeat of the impala cluster the test is executing against
STATESTORE_HEARTBEAT_MS = 500

# The number of queries that can execute concurrently in the pool POOL_NAME.
MAX_NUM_CONCURRENT_QUERIES = 5

# The number of queries that can be queued in the pool POOL_NAME
MAX_NUM_QUEUED_QUERIES = 10

# Mem limit (bytes) used in the mem limit test
MEM_TEST_LIMIT = 100000 * 1024 * 1024

_STATESTORED_ARGS = "-statestore_heartbeat_frequency_ms=%s" % (STATESTORE_HEARTBEAT_MS)

def impalad_admission_ctrl_flags(max_requests, max_queued, mem_limit):
  return ("-vmodule admission-controller=3 -default_pool_max_requests %s "
      "-default_pool_max_queued %s -default_pool_mem_limit %s "
      "-disable_admission_control=false" %\
      (max_requests, max_queued, mem_limit))


def impalad_admission_ctrl_config_args():
  impalad_home = os.environ['IMPALA_HOME']
  resources_dir = os.path.join(impalad_home, "fe", "src", "test", "resources")
  fs_allocation_path = os.path.join(resources_dir, "fair-scheduler-test2.xml")
  llama_site_path = os.path.join(resources_dir, "llama-site-test2.xml")
  return ("-vmodule admission-controller=3 -fair_scheduler_allocation_path %s "
        "-llama_site_path %s -disable_admission_control=false" %\
        (fs_allocation_path, llama_site_path))

def log_metrics(log_prefix, metrics, log_level=logging.DEBUG):
  LOG.log(log_level, "%sadmitted=%s, queued=%s, dequeued=%s, rejected=%s, "\
      "completed=%s, timed-out=%s", log_prefix, metrics['admitted'], metrics['queued'],
      metrics['dequeued'], metrics['rejected'], metrics['completed'],
      metrics['timed-out'])

def compute_metric_deltas(m2, m1, metric_names):
  """Returns a dictionary of the differences of metrics in m2 and m1 (m2 - m1)"""
  return dict((n, m2.get(n, 0) - m1.get(n, 0)) for n in metric_names)

def metric_key(pool_name, metric_name):
  """Helper method to construct the admission controller metric keys"""
  return "admission-controller.%s.%s" % (pool_name, metric_name)

class TestAdmissionController(CustomClusterTestSuite):
  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestAdmissionController, cls).add_test_dimensions()
    cls.TestMatrix.add_dimension(create_single_exec_option_dimension())
    # There's no reason to test this on other file formats/compression codecs right now
    cls.TestMatrix.add_dimension(create_uncompressed_text_dimension(cls.get_workload()))

  def __check_pool_rejected(self, client, pool, expected_error_re):
    try:
      client.set_configuration({'request_pool': pool})
      client.execute("select 1")
      assert False, "Query should return error"
    except ImpalaBeeswaxException as e:
      assert re.search(expected_error_re, str(e))

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      impalad_args=impalad_admission_ctrl_config_args(),
      statestored_args=_STATESTORED_ARGS)
  def test_set_request_pool(self, vector):
    """Tests setting the REQUEST_POOL with the pool placement policy configured
    to require a specific pool (IMPALA-1050)."""
    impalad = self.cluster.impalads[0]
    client = impalad.service.create_beeswax_client()
    try:
      for pool in ['', 'not_a_pool_name']:
        expected_error =\
            "No mapping found for request from user '\w+' with requested pool '%s'"\
            % (pool)
        self.__check_pool_rejected(client, pool, expected_error)

      # Check rejected if user does not have access.
      expected_error = "Request from user '\w+' with requested pool 'root.queueC' "\
          "denied access to assigned pool 'root.queueC'"
      self.__check_pool_rejected(client, 'root.queueC', expected_error)

      # Also try setting a valid pool
      client.set_configuration({'request_pool': 'root.queueB'})
      client.execute("select 1") # Query should execute in queueB
    finally:
      client.close()

class TestAdmissionControllerStress(TestAdmissionController):
  """Submits a number of queries (parameterized) with some delay between submissions
  (parameterized) and the ability to submit to one impalad or many in a round-robin
  fashion. The queries are set with the WAIT debug action so that we have more control
  over the state that the admission controller uses to make decisions.  Each query is
  submitted on a separate thread. Depending on the test parameters a varying number of
  queries will be admitted, queued, and rejected. Once queries are admitted, the query
  execution blocks and we can cancel the query in order to allow another queued query to
  be admitted.

  The test tracks the state of the admission controller using the metrics from each
  impalad to do the following:
  (1) After submitting all queries, the change in metrics for the number of admitted,
      queued, and rejected requests should sum to the number of queries and that the
      values are reasonable given the test parameters.
  (2) While there are running queries:
      * Cancel the currently running queries (they are blocked with the WAIT debug action)
        and verify the metric for the number of completed queries. The threads that
        submitted those queries should complete.
      * Check that queued requests are then dequeued and verify using the metric for the
        number of dequeued requests. The threads that were waiting to submit the query
        should then insert themselves into a list of currently running queries and then
        fetch() the results (which will block).
  (3) After all queries have completed, check that the final number of admitted,
      queued, and rejected requests are reasonable given the test parameters. When
      submitting to a single impalad, we know exactly what the values should be,
      otherwise we just check that they are within reasonable bounds.
  """

  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestAdmissionControllerStress, cls).add_test_dimensions()
    cls.TestMatrix.add_dimension(TestDimension('num_queries', *NUM_QUERIES))
    cls.TestMatrix.add_dimension(
        TestDimension('round_robin_submission', *ROUND_ROBIN_SUBMISSION))

    cls.TestMatrix.add_dimension(
        TestDimension('submission_delay_ms', *SUBMISSION_DELAY_MS))
    if cls.exploration_strategy() == 'core':
      cls.TestMatrix.add_constraint(lambda v: v.get_value('submission_delay_ms') == 0)
      cls.TestMatrix.add_constraint(lambda v: v.get_value('num_queries') == 30)
      cls.TestMatrix.add_constraint(\
          lambda v: v.get_value('round_robin_submission') == True)

  def setup(self):
    # All threads are stored in this list and it's used just to make sure we clean up
    # properly in teardown.
    self.all_threads = list()
    # Each submission thread will append() itself to this list if the query begins
    # execution.  The main thread will access this list to determine which threads are
    # executing queries that can be cancelled (it will pop() elements from the front of
    # the list). The individual operations on the list are atomic and thread-safe thanks
    # to the GIL.
    self.executing_threads = list()

  def teardown(self):
    for thread in self.all_threads:
      try:
        thread.lock.acquire()
        thread.shutdown = True
        if thread.query_handle is not None:
          LOG.debug("Attempt to clean up thread executing query %s (state %s)",
              thread.query_num, thread.query_state)
          client = thread.impalad.service.create_beeswax_client()
          try:
            client.cancel(thread.query_handle)
          finally:
            client.close()
      finally:
        thread.lock.release()
      thread.join(5)
      LOG.debug("Join thread for query num %s %s", thread.query_num,
          "TIMED OUT" if thread.isAlive() else "")

  def get_admission_metrics(self):
    """
    Returns a map of the admission metrics, aggregated across all of the impalads.

    The metrics names are shortened for brevity: 'admitted', 'queued', 'dequeued',
    'rejected', 'completed', and 'timed-out'.
    """
    metrics = {'admitted': 0, 'queued': 0, 'dequeued': 0, 'rejected' : 0,
        'completed': 0, 'timed-out': 0}
    for impalad in self.impalads:
      for short_name in metrics.keys():
        metrics[short_name] += impalad.service.get_metric_value(\
            metric_key(self.pool_name, 'local-%s' % short_name), 0)
    return metrics

  def wait_for_metric_changes(self, metric_names, initial, expected_delta, timeout=30):
    """
    Waits for the sum of metrics in metric_names to change by at least expected_delta.

    This is similar to ImpalaService.wait_for_metric_value(), but it uses one or more
    metrics aggregated across all impalads, e.g. we want to wait for the total number of
    admitted, queued, and rejected metrics to change some amount in total, but we don't
    know exactly how the metrics will change individually.
    'metric_names' is a list of the keys returned by get_admission_metrics() which are
    expected to change.
    'initial' is the initial set of metrics returned by get_admission_metrics() to
    compare against.
    'expected_delta' is the total change expected across all impalads for the specified
    metrics.
    """
    log_metrics("wait_for_metric_changes, initial=", initial)
    current = initial
    start_time = time()
    while True:
      current = self.get_admission_metrics()
      log_metrics("wait_for_metric_changes, current=", current)
      deltas = compute_metric_deltas(current, initial, metric_names)
      delta_sum = sum([ deltas[x] for x in metric_names ])
      LOG.debug("DeltaSum=%s Deltas=%s (Expected=%s for metrics=%s)",\
          delta_sum, deltas, expected_delta, metric_names)
      if delta_sum >= expected_delta:
        LOG.debug("Found all %s metrics after %s seconds", delta_sum,
            round(time() - start_time, 1))
        return (deltas, current)
      assert (time() - start_time < timeout),\
          "Timed out waiting %s seconds for metrics" % (timeout,)
      sleep(1)

  def wait_for_heartbeats(self, heartbeats, timeout=30):
    """Waits for a number of statestore heartbeats from all impalads."""
    start_time = time()
    num_impalads = len(self.impalads)
    init = dict()
    curr = dict()
    for impalad in self.impalads:
      init[impalad] = impalad.service.get_metric_value(\
          'statestore-subscriber.topic-update-interval-time')['count']
      curr[impalad] = init[impalad]

    while True:
      LOG.debug("wait_for_heartbeats: curr=%s, init=%s, d=%s", curr.values(),
          init.values(), [curr[i] - init[i] for i in self.impalads])
      if all([curr[i] - init[i] >= heartbeats for i in self.impalads]): break
      for impalad in self.impalads:
        curr[impalad] = impalad.service.get_metric_value(\
            'statestore-subscriber.topic-update-interval-time')['count']
      assert (time() - start_time < timeout),\
          "Timed out waiting %s seconds for heartbeats" % (timeout,)
      sleep(0.5)
    LOG.debug("Waited %s for %s heartbeats", round(time() - start_time, 1), heartbeats)

  def wait_for_admitted_threads(self, num_threads, timeout=30):
    """
    Wait for query submission threads to update after being admitted, as determined
    by observing metric changes. This is necessary because the metrics may change
    before the execute_async() calls on the query threads return and add themselves
    to self.executing_threads.
    """
    start_time = time()
    LOG.debug("Waiting for %s threads to begin execution", num_threads)
    # All individual list operations are thread-safe, so we don't need to use a
    # lock to synchronize before checking the list length (on which another thread
    # may call append() concurrently).
    while len(self.executing_threads) < num_threads:
      assert (time() - start_time < timeout),\
          "Timed out waiting %s seconds for %s admitted client rpcs to return" %\
              (timeout, num_threads)
      sleep(0.1)
    LOG.debug("Found all %s admitted threads after %s seconds", num_threads,
        round(time() - start_time, 1))

  def cancel_admitted_queries(self, num_queries):
    """
    Cancels queries on threads that are currently blocked on query execution.
    """
    assert len(self.executing_threads) >= num_queries
    LOG.debug("Cancelling %s queries", num_queries)
    for i in xrange(num_queries):
      # pop() is thread-safe, it's OK if another thread is appending concurrently.
      thread = self.executing_threads.pop(0)
      LOG.debug("Cancelling query %s", thread.query_num)
      # The other thread sets the query_state before appending itself to the list,
      # and will not change its state until it is cancelled by this thread.
      assert thread.query_state == 'ADMITTED'
      client = thread.impalad.service.create_beeswax_client()
      try:
        cancel_result = client.cancel(thread.query_handle)
        assert cancel_result.status_code == 0,\
            'Unexpected status code from cancel request: %s' % cancel_result
        # Wait for the query to be cancelled and return
        thread.join(20)
        LOG.debug("Cancelled admitted query %s %s",
            thread.query_num, "TIMED OUT" if thread.isAlive() else "")
        assert not thread.isAlive()
        assert thread.query_state == 'COMPLETED'
      finally:
        client.close()

  class SubmitQueryThread(threading.Thread):
    def __init__(self, impalad, additional_query_options, vector, query_num,
        executing_threads):
      """
      executing_threads must be provided so that this thread can add itself when the
      query is admitted and begins execution.
      """
      super(self.__class__, self).__init__()
      self.executing_threads = executing_threads
      self.vector = vector
      self.additional_query_options = additional_query_options
      self.query_num = query_num
      self.impalad = impalad
      self.error = None
      # query_state is defined and used only by the test code, not a property exposed by
      # the server
      self.query_state = 'NOT_SUBMITTED'

      # lock protects query_handle and shutdown, used by the main thread in teardown()
      self.lock = threading.RLock()
      self.query_handle = None
      self.shutdown = False # Set by the main thread when tearing down

    def run(self):
      client = None
      try:
        try:
          # Take the lock while query_handle is being created to avoid an unlikely race
          # condition with teardown() (i.e. if an error occurs on the main thread), and
          # check if the test is already shut down.
          self.lock.acquire()
          if self.shutdown:
            return

          exec_options = self.vector.get_value('exec_option')
          exec_options['debug_action'] = '0:GETNEXT:WAIT'
          exec_options.update(self.additional_query_options)
          query = QUERY % (self.query_num,)
          self.query_state = 'SUBMITTING'
          client = self.impalad.service.create_beeswax_client()
          ImpalaTestSuite.change_database(client, self.vector.get_value('table_format'))
          client.set_configuration(exec_options)

          LOG.debug("Submitting query %s", self.query_num)
          self.query_handle = client.execute_async(query)
        except ImpalaBeeswaxException as e:
          if "Rejected" in str(e):
            LOG.debug("Rejected query %s", self.query_num)
            self.query_state = 'REJECTED'
            return
          elif "exceeded timeout" in str(e):
            LOG.debug("Query %s timed out", self.query_num)
            self.query_state = 'TIMED OUT'
            return
          else:
            raise e
        finally:
          self.lock.release()
        LOG.debug("Admitted query %s", self.query_num)
        self.query_state = 'ADMITTED'
        # The thread becomes visible to the main thread when it is added to the
        # shared list of executing_threads. append() is atomic and thread-safe.
        self.executing_threads.append(self)
        try:
          # fetch() will block until we cancel the query from the main thread
          # (unless an unexpected error occurs). If an error occurs on the main therad,
          # it is possible that teardown() cancels this query before we call fetch(). In
          # that case a different exception is thrown and we handle it gracefully.
          client.fetch(query, self.query_handle)
        except ImpalaBeeswaxException as e:
          if "Cancelled" in str(e):
            LOG.debug("Query %s completed", self.query_num)
            self.query_state = 'COMPLETED'
            self.query_handle = None
          elif "Invalid or unknown query handle" in str(e):
            # May happen if the test is being torn down early (i.e. an error occurred).
            LOG.debug("Query %s already cancelled in test shutdown.")
          else:
            raise e
      except Exception as e:
        LOG.exception(e)
        # Unknown errors will be raised later
        self.error = e
        self.query_state = 'ERROR'
      finally:
        LOG.debug("Thread terminating in state=%s", self.query_state)
        if client is not None:
          client.close()

  def run_admission_test(self, vector, additional_query_options):
    LOG.debug("Starting test case with parameters: %s", vector)
    self.impalads = self.cluster.impalads
    round_robin_submission = vector.get_value('round_robin_submission')
    submission_delay_ms = vector.get_value('submission_delay_ms')
    if not round_robin_submission:
      self.impalads = [self.impalads[0]]

    num_queries = vector.get_value('num_queries')
    assert num_queries >= MAX_NUM_CONCURRENT_QUERIES + MAX_NUM_QUEUED_QUERIES
    initial_metrics = self.get_admission_metrics();
    log_metrics("Initial metrics: ", initial_metrics);

    # Want query_num to start at 1 because this gets used as the limit in the query to
    # help debugging (we can associate a running query with a thread). If we start at 0,
    # that query would be evaluated as a constant expression and never hit the WAIT debug
    # action.
    for query_num in xrange(1, num_queries + 1):
      impalad = self.impalads[query_num % len(self.impalads)]
      thread = self.SubmitQueryThread(impalad, additional_query_options, vector,
          query_num, self.executing_threads)
      thread.start()
      self.all_threads.append(thread)
      sleep(submission_delay_ms / 1000.0)

    # Wait for all of the queries to be admitted, queued, or rejected (as reported
    # by the impalad metrics).
    LOG.debug("Wait for initial admission decisions")
    (metric_deltas, curr_metrics) = self.wait_for_metric_changes(\
        ['admitted', 'queued', 'rejected'], initial_metrics, num_queries)
    # Also wait for the threads that submitted the queries to start executing
    self.wait_for_admitted_threads(metric_deltas['admitted'])

    # Check that the admission decisions are reasonable given the test parameters
    # The number of admitted and queued requests should be at least the configured limits
    # but less than or equal to those limits times the number of impalads.
    assert metric_deltas['admitted'] >= MAX_NUM_CONCURRENT_QUERIES
    assert metric_deltas['admitted'] <= MAX_NUM_CONCURRENT_QUERIES * len(self.impalads)
    assert metric_deltas['queued'] >=\
        min(num_queries - metric_deltas['admitted'], MAX_NUM_QUEUED_QUERIES)
    assert metric_deltas['queued'] <= MAX_NUM_QUEUED_QUERIES * len(self.impalads)
    assert metric_deltas['rejected'] ==\
        num_queries - metric_deltas['admitted'] - metric_deltas['queued']
    initial_metric_deltas = metric_deltas

    while len(self.executing_threads) > 0:
      curr_metrics = self.get_admission_metrics();
      log_metrics("Main loop, curr_metrics: ", curr_metrics);
      num_to_cancel = len(self.executing_threads)
      LOG.debug("Main loop, will cancel %s queries", num_to_cancel)
      self.cancel_admitted_queries(num_to_cancel)
      self.wait_for_metric_changes(['completed'], curr_metrics, num_to_cancel)

      num_queued_remaining =\
          curr_metrics['queued'] - curr_metrics['dequeued'] - curr_metrics['timed-out']
      expected_admitted = min(num_queued_remaining, MAX_NUM_CONCURRENT_QUERIES)
      (metric_deltas, _) = self.wait_for_metric_changes(['admitted'], curr_metrics,
          expected_admitted)
      self.wait_for_admitted_threads(metric_deltas['admitted'])
      # Wait a few heartbeats to ensure the admission controllers have reached a steady
      # state or we may find an impalad dequeue more requests after we capture metrics.
      self.wait_for_heartbeats(4)

    final_metrics = self.get_admission_metrics();
    log_metrics("Final metrics: ", final_metrics, logging.INFO);
    metric_deltas = compute_metric_deltas(final_metrics, initial_metrics,
        final_metrics.keys())
    assert metric_deltas['timed-out'] == 0

    if round_robin_submission:
      min_expected_admitted = MAX_NUM_CONCURRENT_QUERIES + MAX_NUM_QUEUED_QUERIES
      assert metric_deltas['admitted'] >= min_expected_admitted
      assert metric_deltas['admitted'] <= min_expected_admitted * len(self.impalads)
      assert metric_deltas['admitted'] ==\
          initial_metric_deltas['admitted'] + initial_metric_deltas['queued']
      assert metric_deltas['queued'] == initial_metric_deltas['queued']
      assert metric_deltas['rejected'] == initial_metric_deltas['rejected']
    else:
      # We shouldn't go over the max number of queries or queue size so we can compute
      # the expected number of queries that should have been admitted (which includes the
      # number queued as they eventually get admitted as well), queued, and rejected
      expected_admitted = MAX_NUM_CONCURRENT_QUERIES + MAX_NUM_QUEUED_QUERIES
      assert metric_deltas['admitted'] == expected_admitted
      assert metric_deltas['queued'] == MAX_NUM_QUEUED_QUERIES
      assert metric_deltas['rejected'] == num_queries - expected_admitted

    for thread in self.all_threads:
      if thread.error is not None:
        raise thread.error

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      impalad_args=impalad_admission_ctrl_flags(MAX_NUM_CONCURRENT_QUERIES,
        MAX_NUM_QUEUED_QUERIES, -1),
      statestored_args=_STATESTORED_ARGS)
  def test_admission_controller_with_flags(self, vector):
    self.pool_name = 'default-pool'
    self.run_admission_test(vector, {'request_pool': self.pool_name})

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      impalad_args=impalad_admission_ctrl_config_args(),
      statestored_args=_STATESTORED_ARGS)
  def test_admission_controller_with_configs(self, vector):
    self.pool_name = 'root.queueB'
    self.run_admission_test(vector, {'request_pool': self.pool_name})

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      impalad_args=impalad_admission_ctrl_flags(MAX_NUM_CONCURRENT_QUERIES * 100,
        MAX_NUM_QUEUED_QUERIES, MEM_TEST_LIMIT),
      statestored_args=_STATESTORED_ARGS)
  def test_mem_limit(self, vector):
    self.pool_name = 'default-pool'
    # Each query mem limit (set the query option to override the per-host memory
    # estimate) should use a bit less than (total pool mem limit) / #queries so that
    # once #queries are running, the total pool mem usage is about at the limit and
    # additional incoming requests will be rejected. The actual pool limit on the number
    # of running requests is very high so that requests are only queued/rejected due to
    # the mem limit.
    num_impalads = len(self.cluster.impalads)
    query_mem_limit = (MEM_TEST_LIMIT / MAX_NUM_CONCURRENT_QUERIES / num_impalads) - 1
    self.run_admission_test(vector,
        {'request_pool': self.pool_name, 'mem_limit': query_mem_limit})
