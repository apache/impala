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
# The WorkloadExecutor class encapsulates the execution of a workload. A workload is
# defined as a set of queries for a given  data set, scale factor and a specific test
# vector. It treats a workload an the unit of parallelism.

from __future__ import absolute_import, division, print_function
from builtins import range
import logging

from collections import defaultdict
from copy import deepcopy
from random import shuffle
from sys import exit
from threading import Lock, Thread, Event
import threading

LOG = logging.getLogger('scheduler')
LOG.setLevel(level=logging.DEBUG)

class Scheduler(object):
  """Schedules the submission of workloads across one of more clients.

  Args:
    query_executors (list of QueryExecutor): the objects should be initialized.
    shuffle (boolean): If True, change the order of execution of queries in a workload.
      By default, the queries are executed sorted by query name.
    num_clients (int): Number of concurrent clients.
    impalads (list of str): A list of impalads to connect to. Ignored when the executor
      is hive.
    plan_first (boolean): EXPLAIN queries before executing them

  Attributes:
    query_executors (list of QueryExecutor): initialized query executors
    shuffle (boolean): shuffle query executors
    iterations (int): number of iterations ALL query executors will run
    query_iterations (int): number of times each query executor will execute
    impalads (list of str?): list of impalads for execution. It is rotated after each execution.
    num_clients (int): Number of concurrent clients
    plan_first (boolean): EXPLAIN queries before executing them
  """
  def __init__(self, **kwargs):
    self.query_executors = kwargs.get('query_executors')
    self.shuffle = kwargs.get('shuffle', False)
    self.iterations = kwargs.get('iterations', 1)
    self.query_iterations = kwargs.get('query_iterations', 1)
    self.impalads = kwargs.get('impalads')
    self.num_clients = kwargs.get('num_clients', 1)
    self.plan_first = kwargs.get('plan_first', False)
    self._exit = Event()
    self._results = list()
    self._result_dict_lock = Lock()
    self._thread_name = "[%s " % self.query_executors[0].query.db + "Thread %d]"
    self._threads = []
    self._create_workload_threads()

  @property
  def results(self):
    """Return execution results."""
    return self._results

  def _create_workload_threads(self):
    """Create workload threads.

    Each workload thread is analogus to a client name, and is identified by a unique ID,
    the workload that's being run and the table formats it's being run on."""
    for thread_num in range(self.num_clients):
      thread = Thread(target=self._run_queries, args=[thread_num],
          name=self._thread_name % thread_num)
      thread.daemon = True
      self._threads.append(thread)

  def _get_next_impalad(self):
    """Maintains a rotating list of impalads"""
    self.impalads.rotate(-1)
    return self.impalads[-1]

  def _run_queries(self, thread_num):
    """This method is run by every thread concurrently.

    Args:
      thread_num (int): Thread number. Used for setting the client name in the result.
    """

    # each thread gets its own copy of query_executors
    query_executors = deepcopy(sorted(self.query_executors, key=lambda x: x.query.name))
    for j in range(self.iterations):
      # Randomize the order of execution for each iteration if specified.
      if self.shuffle: shuffle(query_executors)
      results = defaultdict(list)
      workload_time_sec = 0
      for query_executor in query_executors:
        query_name = query_executor.query.name
        LOG.info("Running Query: %s" % query_name)
        for i in range(self.query_iterations):
          if self._exit.isSet():
            LOG.error("Another thread failed, exiting.")
            exit(1)
          try:
            query_executor.prepare(self._get_next_impalad())
            query_executor.execute(plan_first=self.plan_first)
          # QueryExecutor only throws an exception if the query fails and exit_on_error
          # is set to True. If exit_on_error is False, then the exception is logged on
          # the console and execution moves on to the next query.
          except Exception as e:
            LOG.error("Query %s Failed: %s" % (query_name, str(e)))
            self._exit.set()
          finally:
            if query_executor.result:
              LOG.info("%s query iteration %d finished in %.2f seconds" %
                       (query_name, i + 1, query_executor.result.time_taken))
              result = query_executor.result
              result.client_name = thread_num + 1
              self._results.append(result)
              workload_time_sec += query_executor.result.time_taken
      if self.query_iterations == 1:
        LOG.info("Workload iteration %d finished in %s seconds" % (j+1, workload_time_sec))
      cursor = getattr(threading.current_thread(), 'cursor', None)
      if cursor is not None:
        cursor.close()

  def run(self):
    """Run the query pipelines concurrently"""
    for thread_num, t in enumerate(self._threads):
      LOG.info("Starting %s" % self._thread_name % thread_num)
      t.start()
    for thread_num,t in enumerate(self._threads):
      t.join()
      LOG.info("Finished %s" % self._thread_name % thread_num)
    num_expected_results = len(self._threads) * self.iterations * \
        self.query_iterations * len(self.query_executors)
    if len(self._results) != num_expected_results:
      raise RuntimeError("Unexpected number of results generated (%s vs. %s)." %
          (len(self._results), num_expected_results))
