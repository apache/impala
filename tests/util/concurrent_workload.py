#!/usr/bin/env impala-python
#
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

# This class can be used to drive a concurrent workload against a local minicluster

from __future__ import absolute_import, division, print_function
from builtins import range
import argparse
import logging
# Needed to work around datetime threading bug:
# https://stackoverflow.com/questions/32245560/module-object-has-no-attribute-strptime-with-several-threads-python
import _strptime  # noqa: F401
import sys
import time
from queue import Queue
from threading import current_thread, Event, Thread

from tests.common.impala_cluster import ImpalaCluster


class ConcurrentWorkload(object):
  """This class can be used to drive concurrent streams of queries against a cluster. It
  is useful when trying to carefully control the number of queries running on a cluster
  concurrently. The queries typically involve some sleep statement to allow for larger
  numbers of concurrently running queries.

  This class should not be used for performance benchmarks, e.g. to evaluate query
  throughput.

  Users of this class need to make sure to call start() and stop(). Optionally, the class
  supports printing the current throughput rate. The class also requires that the first
  node in the cluster is a dedicated coordinator and it must already be running when
  calling start().
  """
  def __init__(self, query, num_streams):
    self.query = query
    self.num_streams = num_streams
    self.stop_ev = Event()
    self.output_q = Queue()
    self.threads = []
    self.query_rate = 0
    self.query_rate_thread = Thread(target=self.compute_query_rate,
                                    args=(self.output_q, self.stop_ev))

  def execute(self, query):
    """Executes a query on the coordinator of the local minicluster."""
    cluster = ImpalaCluster.get_e2e_test_cluster()
    if len(cluster.impalads) == 0:
      raise Exception("Coordinator not running")
    client = cluster.get_first_impalad().service.create_hs2_client()
    return client.execute(query)

  def loop_query(self, query, output_q, stop_ev):
    """Executes 'query' in a loop while 'stop_ev' is not set and inserts the result into
    'output_q'."""
    while not stop_ev.is_set():
      try:
        output_q.put(self.execute(query))
      except Exception:
        if not stop_ev.is_set():
          stop_ev.set()
          logging.exception("Caught error, stopping")
    logging.info("%s exiting" % current_thread().name)

  def compute_query_rate(self, queue_obj, stop_ev):
    """Computes the query throughput rate in queries per second averaged over the last 5
    seconds. This method only returns when 'stop_ev' is set by the caller."""
    AVG_WINDOW_S = 5
    times = []
    while not stop_ev.is_set():
      # Don't block to check for stop_ev
      if queue_obj.empty():
        time.sleep(0.1)
        continue
      queue_obj.get()
      now = time.time()
      times.append(now)
      # Keep only timestamps within the averaging window
      start = now - AVG_WINDOW_S
      times = [t for t in times if t >= start]
      self.query_rate = float(len(times)) / AVG_WINDOW_S

  def get_query_rate(self):
    """Returns the query rate as computed by compute_query_rate. This is thread-safe
    because assignments in Python are atomic."""
    return self.query_rate

  def start(self):
    """Starts worker threads to execute queries."""
    # Start workers
    for i in range(self.num_streams):
      t = Thread(target=self.loop_query, args=(self.query, self.output_q, self.stop_ev))
      self.threads.append(t)
      t.start()
    self.query_rate_thread.start()

  def print_query_rate(self):
    """Prints the current query throughput until user presses ctrl-c."""
    try:
      self._print_query_rate(self.output_q, self.stop_ev)
    except KeyboardInterrupt:
      self.stop()
    assert self.stop_ev.is_set(), "Stop event expected to be set but it isn't"

  def _print_query_rate(self, queue_obj, stop_ev):
    """Prints the query throughput rate until 'stop_ev' is set by the caller."""
    PERIOD_S = 1

    print_time = time.time()
    while not stop_ev.is_set():
      sys.stdout.write("\rQuery rate %.2f/s" % self.query_rate)
      sys.stdout.flush()
      print_time += PERIOD_S
      time.sleep(print_time - time.time())
    sys.stdout.write("\n")

  def stop(self):
    """Stops all worker threads and waits for them to finish."""
    if self.stop_ev is None or self.stop_ev.is_set():
      return
    self.stop_ev.set()
    # Wait for all workers to exit
    for t in self.threads:
      logging.info("Waiting for %s" % t.name)
      t.join()
    self.threads = []
    if self.query_rate_thread:
      self.query_rate_thread.join()
      self.query_rate = None


def main():
  parser = argparse.ArgumentParser()
  parser.add_argument("-q", "--query", help="Run this query",
                      default="select * from functional_parquet.alltypestiny "
                              "where month < 3 and id + random() < sleep(500);")
  parser.add_argument("-n", "--num_streams", help="Run this many in parallel", type=int,
                      default=5)
  args = parser.parse_args()

  # Restrict logging so it doesn't interfere with print_query_rate()
  logging.basicConfig(level=logging.INFO)
  # Also restrict other modules' debug output
  logging.getLogger("impala_cluster").setLevel(logging.INFO)
  logging.getLogger("impala_connection").setLevel(logging.WARNING)
  logging.getLogger("impala.hiveserver2").setLevel(logging.CRITICAL)

  s = ConcurrentWorkload(args.query, args.num_streams)

  s.start()
  s.print_query_rate()


if __name__ == "__main__":
  main()
