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

import logging
from tests.common.test_dimensions import (TableFormatInfo,
    load_table_info_dimension,
    get_dataset_from_workload)
from tests.performance.query import Query, QueryResult
from tests.performance.query_executor import (BeeswaxQueryExecConfig,
    HiveQueryExecConfig,
    JdbcQueryExecConfig,
    execute_using_impala_beeswax,
    execute_using_jdbc,
    execute_using_hive, QueryExecutor)
from tests.performance.scheduler import Scheduler

# Setup Logging
logging.basicConfig(level=logging.INFO, format='[%(name)s]: %(message)s')
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
    results (list of QueryResult)
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

  def _get_executor_name(self):
    executor_name = self.config.client_type
    # We want to indicate this is IMPALA beeswax.
    # We currently don't support hive beeswax.
    return 'impala_beeswax' if executor_name == 'beeswax' else executor_name

  def _create_executor(self, executor_name):
    query_options = {
        'hive': lambda: (execute_using_hive,
          HiveQueryExecConfig(self.config.query_iterations,
          hive_cmd=self.config.hive_cmd,
          plugin_runner=self.config.plugin_runner
          )),
        'impala_beeswax': lambda: (execute_using_impala_beeswax,
          BeeswaxQueryExecConfig(plugin_runner=self.config.plugin_runner,
          exec_options=self.config.exec_options,
          use_kerberos=self.config.use_kerberos,
          )),
        'jdbc': lambda: (execute_using_jdbc,
          JdbcQueryExecConfig(plugin_runner=self.config.plugin_runner)
          )
    } [executor_name]()
    return query_options

  def _execute_queries(self, queries):
    """Execute a set of queries.

    Create query executors for each query, and pass them along with config information to
    the scheduler, which then runs the queries.
    """
    executor_name = self._get_executor_name()
    exec_func, exec_config = self._create_executor(executor_name)
    query_executors = []
    # Build an executor for each query
    for query in queries:
      query_executor = QueryExecutor(executor_name, query, exec_func, exec_config,
          self.exit_on_error)
      query_executors.append(query_executor)
    # Initialize the scheduler.
    scheduler = Scheduler(query_executors=query_executors,
        shuffle=self.config.shuffle_queries,
        iterations=self.config.workload_iterations,
        query_iterations=self.config.query_iterations,
        impalads=self.config.impalads,
        num_clients=self.config.num_clients)

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
