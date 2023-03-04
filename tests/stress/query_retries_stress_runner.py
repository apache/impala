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

# Runs a stress test for transparent query retries. See the usage of the script for an
# explanation of what the job does and how it works.

# TODO: Add results validation, this likely requires IMPALA-9225 first.
# TODO: Make the script cancellable; more of a nice to have, but Ctrl+C does not kill
# the script, it has to be killed manually (e.g. kill [pid]).

from __future__ import absolute_import, division, print_function
from builtins import map, range
import logging
import pipes
import os
import random
import subprocess
import sys
import threading
import traceback
import queue

from argparse import ArgumentParser
from argparse import RawDescriptionHelpFormatter
from time import sleep

from tests.common.impala_cluster import ImpalaCluster
from tests.stress.util import create_and_start_daemon_thread
from tests.util.test_file_parser import load_tpc_queries

IMPALA_HOME = os.environ["IMPALA_HOME"]

LOG = logging.getLogger('query_retries_stress_test')


class QueryRetryLatch(object):
  """
  Ensures that the impalad killer thread waits until all queries that are being retried
  to complete before killing another impalad. Each thread running a stream of the defined
  TPC workload calls 'on_query_completion' whenever it completes a query. The latch then
  adds the given stream id to an internal set. The impalad killer thread waits until the
  size of the set reaches the total number of concurrent streams before killing another
  impalad. The same latch is used multiple times and is reset by the impalad killer
  thread each time it kills an impalad.
  """

  def __init__(self, num_streams):
    self.num_streams = num_streams
    self.stream_ids = set()
    self.lock = threading.Condition()

  def on_query_completion(self, stream_id):
    self.lock.acquire()
    self.stream_ids.add(stream_id)
    if len(self.stream_ids) == self.num_streams:
      self.lock.notifyAll()
    self.lock.release()

  def wait_for_retrying_queries(self):
    self.lock.acquire()
    while len(self.stream_ids) != self.num_streams:
      self.lock.wait()
    self.lock.release()

  def reset(self):
    self.lock.acquire()
    self.stream_ids.clear()
    self.lock.release()


# All of these parameters need to be global because they are shared amongst threads.
# 'total_queries_retried' is protected by 'total_queries_retried_lock'.
total_queries_retried_lock = threading.Lock()
total_queries_retried = 0
completed_queries_latch = None


def configured_call(cmd):
  """Call a command in a shell with config-impala.sh."""
  if type(cmd) is list:
    cmd = " ".join([pipes.quote(arg) for arg in cmd])
  cmd = "source {0}/bin/impala-config.sh && {1}".format(IMPALA_HOME, cmd)
  return subprocess.check_call(["bash", "-c", cmd])


def start_impala_cluster(num_impalads):
  """Start an impalad cluster with 'num_impalads' where there is one exclusive
  coordinator and 'num_impalds' - 1 executors."""
  configured_call(["{0}/bin/start-impala-cluster.py".format(IMPALA_HOME), "-s",
                   str(num_impalads), "-c", "1", "--use_exclusive_coordinators"])


def run_concurrent_workloads(concurrency, coordinator, database, queries):
  """Launches 'concurrency' threads, where each thread runs the given set of queries
  against the given database in a loop against the given impalad coordinator. The method
  waits until all the threads have completed."""

  # The exception queue is used to pass errors from the workload threads back to the main
  # thread.
  exception_queue = queue.Queue()

  # The main method for the workload runner threads.
  def __run_workload(stream_id):
    global completed_queries_latch
    global total_queries_retried_lock
    global total_queries_retried
    handle = None
    num_queries_retried = 0
    client = None
    try:
      # Create and setup the client.
      client = coordinator.service.create_beeswax_client()
      LOG.info("Running workload: database={0} and coordinator=localhost:{1}, pid={2}"
          .format(database, coordinator.get_webserver_port(), coordinator.get_pid()))
      client.execute("use {0}".format(database))
      client.set_configuration_option('retry_failed_queries', 'true')

      # Shuffle the queries in a random order.
      shuffled_queries = list(queries.values())
      random.shuffle(shuffled_queries)

      # Run each query sequentially.
      for query in shuffled_queries:
        handle = None
        try:
          # Don't use client.execute as it eagerly fetches results, which causes retries
          # to be disabled.
          handle = client.execute_async(query)
          if not client.wait_for_finished_timeout(handle, 3600):
            raise Exception("Timeout while waiting for query to finish")
          completed_queries_latch.on_query_completion(stream_id)

          # Check if the query was retried, and update any relevant counters.
          runtime_profile = client.get_runtime_profile(handle)
          if "Original Query Id" in runtime_profile:
            LOG.info("Query {0} was retried".format(handle.get_handle().id))
            num_queries_retried += 1
            total_queries_retried_lock.acquire()
            total_queries_retried += 1
            total_queries_retried_lock.release()
        finally:
          if handle:
            try:
              client.close_query(handle)
            except Exception:
              pass  # suppress any exceptions when closing the query handle

      LOG.info("Finished workload, retried {0} queries".format(num_queries_retried))
    except Exception:
      if handle and handle.get_handle() and handle.get_handle().id:
        LOG.exception("Query query_id={0} failed".format(handle.get_handle().id))
        exception_queue.put((handle.get_handle().id, sys.exc_info()))
      else:
        LOG.exception("An unknown query failed")
        exception_queue.put(("unknown", sys.exc_info()))
    finally:
      if client:
        client.close()

  # Start 'concurrency' number of workload runner threads, and then wait until they all
  # complete.
  workload_threads = []
  LOG.info("Starting {0} concurrent workloads".format(concurrency))
  for i in range(concurrency):
    workload_thread = threading.Thread(target=__run_workload, args=[i],
        name="workload_thread_{0}".format(i))
    workload_thread.start()
    workload_threads.append(workload_thread)
  list(map(lambda thread: thread.join(), workload_threads))

  # Check if any of the workload runner threads hit an exception, if one did then print
  # the error and exit.
  if exception_queue.empty():
    LOG.info("All workloads completed")
  else:
    while not exception_queue.empty():
      query_id, exception = exception_queue.get_nowait()
      exc_type, exc_value, exc_traceback = exception
      LOG.error("A workload failed due to a query failure: query_id={0}\n{1}".format(
          query_id, ''.join(traceback.format_exception(
              exc_type, exc_value, exc_traceback))))
    sys.exit(1)


def start_random_impalad_killer(kill_frequency, start_delay, cluster):
  """Start the impalad killer thread. The thread executes in a constant loop and is
  created as a daemon thread so it does not need to complete for the process to
  shutdown."""

  # The impalad killer thread main method.
  def __kill_random_killer():
    global completed_queries_latch
    while True:
      try:
        # Pick a random impalad to kill, wait until it is safe to kill the impalad, and
        # then kill it.
        target_impalad = cluster.impalads[random.randint(1, len(cluster.impalads) - 1)]
        sleep(kill_frequency)
        completed_queries_latch.wait_for_retrying_queries()
        LOG.info("Killing impalad localhost:{0} pid={1}"
            .format(target_impalad.get_webserver_port(), target_impalad.get_pid()))
        target_impalad.kill()
        completed_queries_latch.reset()

        # Wait for 'start_delay' seconds before starting the impalad again.
        sleep(start_delay)
        LOG.info("Starting impalad localhost:{0}"
            .format(target_impalad.get_webserver_port()))
        target_impalad.start(timeout=300)
      except Exception:
        LOG.error("Error while running the impalad killer thread", exc_info=True)
        # Hard exit the process if the killer thread fails.
        sys.exit(1)

  # Start the impalad killer thread.
  create_and_start_daemon_thread(__kill_random_killer, "impalad_killer_thread")
  LOG.info("Started impalad killer with kill frequency {0} and start delay {1}"
      .format(kill_frequency, start_delay))


def run_stress_workload(queries, database, workload, start_delay,
        kill_frequency, concurrency, iterations, num_impalads):
  """Runs the given set of queries against the the given database. 'concurrency' controls
  how many concurrent streams of the queries are run, and 'iterations' controls how many
  times the workload is run. 'num_impalads' controls the number of impalads to launch.
  The 'kill_frequency' and 'start_delay' are used to configure the impalad killer thread.
  'workload' is purely used for debugging purposes."""

  # Create the global QueryRetryLatch.
  global completed_queries_latch
  completed_queries_latch = QueryRetryLatch(concurrency)

  # Start the Impala cluster and set the coordinator.
  start_impala_cluster(num_impalads)
  cluster = ImpalaCluster()
  impala_coordinator = cluster.impalads[0]

  # Start the 'random impalad killer' thread.
  start_random_impalad_killer(kill_frequency, start_delay, cluster)

  # Run the stress test 'iterations' times.
  for i in range(iterations):
    LOG.info("Starting iteration {0} of workload {1}".format(i, workload))
    run_concurrent_workloads(concurrency, impala_coordinator, database,
        queries)

  # Print the total number of queries retried.
  global total_queries_retried_lock
  global total_queries_retried
  total_queries_retried_lock.acquire()
  LOG.info("Total queries retried {0}".format(total_queries_retried))
  total_queries_retried_lock.release()


def parse_args(parser):
  """Parse command line arguments."""

  parser.add_argument('-w', '--workload', default='tpch', help="""The target workload to
      run. Choices: tpch, tpcds. Default: tpch""")
  parser.add_argument('-s', '--scale', default='', help="""The scale factor for the
      workload. Default: the scale of the dataload databases - e.g. 'tpch_parquet'""")
  parser.add_argument('-t', '--table_format', default='parquet', help="""The file format
      to use. Choices: parquet, text. Default: parquet""")
  parser.add_argument('-i', '--num_impalads', default='5', help="""The number of impalads
      to run. One impalad will be a dedicated coordinator. Default: 5""")
  parser.add_argument('-f', '--kill_frequency', default='30', help="""How often, in
      seconds, a random impalad should be killed. Default: 30""")
  parser.add_argument('-d', '--start_delay', default='10', help="""Number of seconds to
      wait before restarting a killed impalad. Default: 10""")
  parser.add_argument('-c', '--concurrency', default='4', help="""The number of
      concurrent streams of the workload to run. Default: 4""")
  parser.add_argument('-r', '--iterations', default='4', help="""The number of
      times each workload will be run. Each concurrent stream will execute the workload
      this many times. Default: 4""")

  args = parser.parse_args()
  return args


def main():
  logging.basicConfig(level=logging.INFO, format='[%(name)s][%(threadName)s]: %(message)s')
  # Parse the command line args.
  parser = ArgumentParser(description="""
Runs a stress test for transparent query retries. Starts an impala cluster with a
single dedicated coordinator, and a specified number of impalads. Launches multiple
concurrent streams of a TPC workload and randomly kills and starts a single impalad
in the cluster. Only validates that all queries are successful. Prints out a count
of the number of queries retried. A query is considered retried if it has the text
'Original Query Id' in its runtime profile.

The 'iterations' flag controls how many iterations of the TPC workload is run. Each
iteration launches a specified number of concurrent streams of TPC. Each stream runs
all queries in the TPC workload one-by-one, in a random order. A iteration is
considered complete when all concurrent streams successfully finish.

A background thread randomly kills one of the impalads in the cluster, but never
kills the coordinator. The 'kill-frequency' flag controls how often an impalad is
killed, but it is only a lower bound on the actual frequency used. Since query
retries only support retrying a query once, when an impalad is killed, the impalad
killer thread waits until all retried queries complete before killing another
impalad. The 'start-delay' flag controls how long to wait before restarting the
killed impalad. Only one impalad is ever killed at a time.

When specifying a non-default scale, the job will look for a database of the form
'[workload][scale-factor]_parquet' if 'table-format' is parquet or
'[workload][scale-factor] if 'table-format' is text.""",
      formatter_class=RawDescriptionHelpFormatter)

  args = parse_args(parser)

  # Set args to local variables and cast to appropriate types.
  scale = args.scale
  start_delay = float(args.start_delay)
  kill_frequency = float(args.kill_frequency)
  concurrency = int(args.concurrency)
  iterations = int(args.iterations)
  workload = args.workload
  table_format = args.table_format
  num_impalads = int(args.num_impalads)

  # Load TPC queries.
  if workload.strip().lower() == 'tpch':
    queries = load_tpc_queries('tpch')
  elif workload.strip().lower() == 'tpcds':
    queries = load_tpc_queries('tpcds')
  else:
    parser.print_usage()
    LOG.error("'--workload' must be either 'tpch' or 'tpcds'")
    sys.exit(1)

  # Set the correct database.
  if table_format is 'parquet':
    database = workload + scale + '_parquet'
  elif workload is 'text':
    database = workload + scale
  else:
    parser.print_usage()
    LOG.info("'--table_format' must be either 'parquet' or 'text'")
    sys.exit(1)

  # Run the actual stress test.
  run_stress_workload(queries, database, workload, start_delay,
          kill_frequency, concurrency, iterations, num_impalads)


if __name__ == "__main__":
  main()
