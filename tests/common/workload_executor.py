#!/usr/bin/env python
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
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
# The WorkloadExecutor class encapsulates the execution of a workload. A workload is
# defined as a set of queries for a given  data set, scale factor and a specific test
# vector. It treats a workload an the unit of parallelism.

import logging
import os

from collections import defaultdict
from random import shuffle
from threading import Lock, Thread

logging.basicConfig(level=logging.INFO, format='%(name)s %(threadName)s: %(message)s')
LOG = logging.getLogger('workload_executor')
LOG.setLevel(level=logging.DEBUG)


class WorkloadExecutor(object):
  """Execute a workload in parallel.

  A workload execution expects the following arguments:
  query_pipelines: Consists of a list of query pipelines. A query pipeline is defined
                   as a mapping of a Query to its QueryExecutor. Each query pipeline
                   contains the same Query objects as keys.
  shuffle: Change the order of execution of queries in a workload. By default, the queries
           are executed sorted by name.
  """
  # TODO: Revisit the class name. The current naming scheme is ambigious.
  def __init__(self, **kwargs):
    self.query_pipelines = kwargs.get('query_pipelines')
    self.shuffle = kwargs.get('shuffle', False)
    self.iterations = kwargs.get('iterations', 1)
    # The result dict maps a query object to a list of QueryExecResult objects.
    self.__results = defaultdict(list)
    self.__result_dict_lock = Lock()
    self.__thread_name = "[%s] " % self.query_pipelines[0].keys()[0].db + "Thread %d"
    self.__workload_threads = []
    self.__create_workload_threads()

  def __create_workload_threads(self):
    """Create a workload thread per query pipeline"""
    for thread_num, query_pipeline in enumerate(self.query_pipelines):
      self.__workload_threads.append(Thread(target=self.__run_queries,
        args=[query_pipeline, thread_num], name=self.__thread_name % thread_num))

  def __update_results(self, results):
    """Update the results dictionary maintaining thread safety"""
    self.__result_dict_lock.acquire()
    try:
      for query, result in results.iteritems():
        self.__results[query].extend(result)
        self.__results[query] = list(set(self.__results[query]))
    finally:
      self.__result_dict_lock.release()

  def __run_queries(self, query_pipeline, thread_num):
    """Runs a query pipeline and updates results."""
    queries = sorted(query_pipeline.keys())
    # Randomize the order of execution if specified.
    if shuffle: shuffle(queries)
    for i in xrange(self.iterations):
      results = dict()
      for query in queries:
        query_executor = query_pipeline[query]
        if i == 0:
          query_executor.thread_name = "%s %s" % (self.__thread_name % thread_num, \
              query_executor.thread_name)
        query_executor.run()
        results[query] = query_executor.get_results()
      # Store the results. This has to be thread safe. Multiple threads can write
      # to the same key (query object).
      self.__update_results(results)

  def run(self):
    """Run the query pipelines concurrently"""
    for thread_num, t in enumerate(self.__workload_threads):
      LOG.info("Starting %s" % self.__thread_name % thread_num)
      t.start()
    for thread_num,t in enumerate(self.__workload_threads):
      t.join()
      LOG.info("Finished %s" % self.__thread_name % thread_num)

  def get_results(self):
    """Return execution results."""
    return self.__results
