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
import logging

from tests.common.test_dimensions import (
    TableFormatInfo,
    get_dataset_from_workload,
    load_table_info_dimension)
from tests.performance.query_executor import (
    BeeswaxQueryExecConfig,
    HiveHS2QueryConfig,
    ImpalaHS2QueryConfig,
    JdbcQueryExecConfig,
    QueryExecutor)
from tests.performance.query_exec_functions import (
    execute_using_hive_hs2,
    execute_using_impala_beeswax,
    execute_using_impala_hs2,
    execute_using_jdbc)
from tests.performance.scheduler import Scheduler

LOG = logging.getLogger('workload_runner')


class WorkloadRunner(object):
  """Runs query files and captures results from the specified workload(s)

  The usage is:
   1) Initialize WorkloadRunner with desired execution parameters.
   2) Call workload_runner.run()

   Internally, for each workload, this module looks up and parses that workload's
   query files and reads the workload's test vector to determine what combination(s)
   of file format / compression to run with.

  Args:
    workload (Workload)
    scale_factor (str): eg. "300gb"
    config (WorkloadConfig)

  Attributes:
    workload (Workload)
    scale_factor (str): eg. "300gb"
    config (WorkloadConfig)
    exit_on_error (boolean)
    results (list of ImpalaQueryResult)
    _test_vectors (list of ?)
  """

  def __init__(self, workload, scale_factor, config):
    self.workload = workload
    self.scale_factor = scale_factor
    self.config = config
    self.exit_on_error = not self.config.continue_on_query_error
    if self.config.verbose: LOG.setLevel(level=logging.DEBUG)
    self._generate_test_vectors()
    self._results = list()

  @property
  def results(self):
    return self._results

  def _generate_test_vectors(self):
    """Generate test vector objects

    If the user has specified a set for table_formats, generate them, otherwise generate
    vectors for all table formats within the specified exploration strategy.
    """
    self._test_vectors = []
    if self.config.table_formats:
      dataset = get_dataset_from_workload(self.workload.name)
      for tf in self.config.table_formats:
        self._test_vectors.append(TableFormatInfo.create_from_string(dataset, tf))
    else:
      vectors = load_table_info_dimension(self.workload.name,
          self.config.exploration_strategy)
      self._test_vectors = [vector.value for vector in vectors]

  def _create_executor(self, executor_name):
    query_options = {
        'impala_beeswax': lambda: (execute_using_impala_beeswax,
          BeeswaxQueryExecConfig(plugin_runner=self.config.plugin_runner,
          exec_options=self.config.exec_options,
          use_kerberos=self.config.use_kerberos,
          user=self.config.user if self.config.password else None,
          password=self.config.password,
          use_ssl=self.config.use_ssl
          )),
        'impala_jdbc': lambda: (execute_using_jdbc,
          JdbcQueryExecConfig(plugin_runner=self.config.plugin_runner)
          ),
        'impala_hs2': lambda: (execute_using_impala_hs2,
          ImpalaHS2QueryConfig(plugin_runner=self.config.plugin_runner,
            use_kerberos=self.config.use_kerberos
          )),
        'hive_hs2': lambda: (execute_using_hive_hs2,
          HiveHS2QueryConfig(hiveserver=self.config.hiveserver,
            plugin_runner=self.config.plugin_runner,
            exec_options=self.config.exec_options,
            user=self.config.user,
            use_kerberos=self.config.use_kerberos
          ))
    } [executor_name]()
    return query_options

  def _execute_queries(self, queries):
    """Execute a set of queries.

    Create query executors for each query, and pass them along with config information to
    the scheduler.
    """
    executor_name = "{0}_{1}".format(self.config.exec_engine, self.config.client_type)
    exec_func, exec_config = self._create_executor(executor_name)
    query_executors = []
    # Build an executor for each query
    for query in queries:
      query_executor = QueryExecutor(executor_name,
          query,
          exec_func,
          exec_config,
          self.exit_on_error)
      query_executors.append(query_executor)
    # Initialize the scheduler.
    scheduler = Scheduler(query_executors=query_executors,
        shuffle=self.config.shuffle_queries,
        iterations=self.config.workload_iterations,
        query_iterations=self.config.query_iterations,
        impalads=self.config.impalads,
        num_clients=self.config.num_clients,
        plan_first=getattr(self.config, 'plan_first', False))

    scheduler.run()
    self._results.extend(scheduler.results)

  def run(self):
    """
    Runs the workload against all test vectors serially and stores the results.
    """
    for test_vector in self._test_vectors:
      # Transform the query strings to Query objects for a combination of scale factor and
      # the test vector.
      queries = self.workload.construct_queries(test_vector, self.scale_factor)
      self._execute_queries(queries)
