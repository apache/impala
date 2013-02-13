#!/usr/bin/env python
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
# Targeted Impala failure tests
#
import logging
import os
import sys
import random
import time
import unittest
from tests.common.impala_cluster import *
from tests.common.query_executor import *
from tests.common.run_workload import WorkloadRunner
from random import choice

IMPALA_HOME = os.environ['IMPALA_HOME']

# This is a basic targeted test for failure scenarios
class TestImpalaFailures(unittest.TestCase):
  def setUp(self):
    self.cluster = ImpalaCluster(cm_server_host, cm_cluster_name,
                                 username=cm_username, password=cm_password)

  def test_kill_coordinator(self):
    impala_service = self.cluster.get_impala_service()
    impalad_proc = choice(impala_service.get_all_impalad_processes())
    print 'Coordinator is: %s' % impalad_proc
    exec_result = self.__execute_query_kill_process(coordinator_impalad_proc,
        coordinator_impalad_proc, 'select count(*) from functional.alltypes', 10)
    self.assertFalse(exec_result.success, 'Expected query to fail, instead it succeeded')

    # Wait for things to recover
    self.__execute_query_expect_success(impalad_proc,
        'select count(*) from functional.alltypes', 5)

  def test_kill_impalad_worker(self):
    all_impalad_procs = self.cluster.get_impala_service().get_all_impalad_processes()
    coordinator_impalad_proc = choice(all_impalad_procs)
    worker_impalad_proc  = first(all_impalad_procs,\
        lambda impalad: impalad.hostname != coordinator_impalad_proc.hostname)
    self.assertTrue(worker_impalad_proc.is_running(),
                   'Expected impala service to be running.')

    exec_result = self.__execute_query_kill_process( coordinator_impalad_proc,
        worker_impalad_proc, 'select count(*) from functional.alltypes', 10)
    self.assertFalse(exec_result.success, 'Expected query to fail, instead it succeeded')

    # Same query should succeed when no failure is injected.
    __execute_query_expect_success(coordinator_impalad_proc,
        'select count(*) from functional.alltypes', 10)
    self.assertTrue(coordinator_impalad_proc.is_running(),
                    'Expected coordinator service to be online.')

  def test_kill_state_store(self):
    impala_service = self.cluster.get_impala_service()
    state_store_proc = impala_service.get_state_store_process()
    print 'State Store is: %s' % state_store_proc.hostname
    impalad_proc = choice(impala_service.get_all_impalad_processes())
    print 'Coordinator is: %s' % impalad_proc.hostname

    state_store_proc.kill()
    workload_runner = WorkloadRunner(impalad='%s:21000' % impalad_proc.hostname)
    workload_runner.run_workload('tpch', scale_factor='')

  def __execute_query_kill_process(self, coordinator_impalad,  impala_process_to_kill,
                                   query, iterations):
    executor = create_executor(coordinator_impalad, iterations, query)
    executor.start()
    # TODO: Need a test hook to cause query to continue running.
    time.sleep(1)
    impala_process_to_kill.kill()
    executor.join()
    output, exec_result = executor.get_results()
    print output
    return exec_result

  def __execute_query_expect_success(self, impalad, query, iterations):
    executor = create_executor(impalad, iterations, query)
    executor.start()
    executor.join()
    output, exec_result = executor.get_results()
    print output
    self.assertTrue(exec_result.success, 'Expected query to succeed')
    return exec_result

def create_executor(impalad_proc, iterations, query):
  # Move to use beeswax when available.
  options = RunQueryExecOptions(
                      iterations=iterations,
                      impalad='%s:%s' % (impalad_proc.hostname, impalad_proc.beeswax_port),
                      runquery_cmd='run-query.sh ')

  return QueryExecutor('test',execute_using_runquery, options, query)

if __name__ == '__main__':
  parser = OptionParser()
  parser.add_option("--cluster_name", dest="cluster_name",
                    help="The CM name of the cluster under test")
  parser.add_option("--cm_server_host", dest="cm_server_host",
                    help="The host of the CM server.")
  parser.add_option("--cm_username", dest="cm_username", default='admin',
                    help="The username to the CM server")
  parser.add_option("--cm_password", dest="cm_password", default='admin',
                    help="The password to the CM server")
  options, args = parser.parse_args()
  cm_cluster_name = options.cluster_name
  cm_server_host = options.cm_server_host
  cm_username = options.cm_username
  cm_password = options.cm_password

  # unittest.main() also looks for args in sys.argv and it gets mad when it sees
  # unexpected parameters. To make this work we delete the arguments after we have read
  # them. TODO: Find a better way to do this because it means we can't pass params to
  # unittest.
  print sys.argv
  del sys.argv[1:]
  sys.exit(1)
  unittest.main()
