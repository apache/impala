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
# Module used for executing queries and gathering results.
# The QueryExecutor is meant to be generic and doesn't
# have the knowledge of how to actually execute a query. It takes a query and its config
# and executes is against a executor function.
# For example (in pseudo-code):
#
# def exec_func(query, config):
# ...
#
# config = ImpalaBeeswaxQueryExecConfig()
# executor = QueryExecutor('beeswax', query, config, exec_func)
# executor.run()
# result = executor.result

from __future__ import absolute_import, division, print_function
import logging
import os
import re

from tests.performance.query import Query

# Setup logging for this module.
LOG = logging.getLogger('query_executor')
LOG.setLevel(level=logging.INFO)

# Globals.
hive_result_regex = 'Time taken: (\d*).(\d*) seconds'
# Match any CRUD statement that can follow EXPLAIN.
# The statement may begin with SQL line comments starting with --
COMMENT_LINES_REGEX = r'(?:\s*--.*\n)*'
DDL_CRUD_PATTERN = \
  re.compile(COMMENT_LINES_REGEX +
      r'\s*((CREATE|DELETE|INSERT|SELECT|UPDATE|UPSERT|WITH)\s|VALUES\s*\()',
      re.IGNORECASE)


## TODO: Split executors into their own modules.
class QueryExecConfig(object):
  """Base Class for Execution Configs

  Attributes:
    plugin_runner (PluginRunner?)
  """
  def __init__(self, plugin_runner=None):
    self.plugin_runner = plugin_runner


class ImpalaQueryExecConfig(QueryExecConfig):
  """Base class for Impala query execution config

  Attributes:
    impalad (str): address of impalad <host>:<port>
  """

  def __init__(self, plugin_runner=None, impalad='localhost:21000'):
    super(ImpalaQueryExecConfig, self).__init__(plugin_runner=plugin_runner)
    self._impalad = impalad

  @property
  def impalad(self):
    return self._impalad

  @impalad.setter
  def impalad(self, value):
    self._impalad = value


class JdbcQueryExecConfig(ImpalaQueryExecConfig):
  """Impala query execution config for jdbc

  Attributes:
    tranport (?): ?
  """

  JDBC_CLIENT_PATH = os.path.join(os.environ['IMPALA_HOME'], 'bin/run-jdbc-client.sh')

  def __init__(self, plugin_runner=None, impalad='localhost:21050', transport=None):
    super(JdbcQueryExecConfig, self).__init__(plugin_runner=plugin_runner,
        impalad=impalad)
    self.transport = transport

  @property
  def jdbc_client_cmd(self):
    """The args to run the jdbc client.

    Constructed on the fly, since the impalad it points to can change.
    """
    assert self.transport is not None
    return JdbcQueryExecConfig.JDBC_CLIENT_PATH + ' -i "%s" -t %s' % (self._impalad,
                                                                      self.transport)

class ImpalaHS2QueryConfig(ImpalaQueryExecConfig):
  def __init__(self, use_kerberos=False, impalad="localhost:21050", plugin_runner=None):
    super(ImpalaHS2QueryConfig, self).__init__(plugin_runner=plugin_runner,
        impalad=impalad)
    # TODO Use a config dict for query execution options similar to HS2
    self.use_kerberos = use_kerberos


class HiveHS2QueryConfig(QueryExecConfig):
  def __init__(self,
      plugin_runner=None,
      exec_options = None,
      use_kerberos=False,
      user=None,
      hiveserver='localhost'):
    super(HiveHS2QueryConfig, self).__init__()
    self.exec_options = dict()
    self._build_options(exec_options)
    self.use_kerberos = use_kerberos
    self.user = user
    self.hiveserver = hiveserver

  def _build_options(self, exec_options):
    """Read the exec_options into self.exec_options

    Args:
      exec_options (str): String formatted as "command1;command2"
    """

    if exec_options:
      # exec_options are seperated by ; on the command line
      options = exec_options.split(';')
      for option in options:
        key, value = option.split(':')
        # The keys in HiveService QueryOptions are lower case.
        self.exec_options[key.lower()] = value

class BeeswaxQueryExecConfig(ImpalaQueryExecConfig):
  """Impala query execution config for beeswax

  Args:
    use_kerberos (boolean)
    exec_options (str): String formatted as "opt1:val1;opt2:val2"
    impalad (str): address of impalad <host>:<port>
    plugin_runner (?): ?

  Attributes:
    use_kerberos (boolean)
    exec_options (dict str -> str): execution options
  """

  def __init__(self, use_kerberos=False, exec_options=None, impalad='localhost:21000',
               plugin_runner=None, user=None, password=None, use_ssl=False):
    super(BeeswaxQueryExecConfig, self).__init__(plugin_runner=plugin_runner,
        impalad=impalad)
    self.use_kerberos = use_kerberos
    self.exec_options = dict()
    self._build_options(exec_options)
    self.user = user
    self.password = password
    self.use_ssl = use_ssl

  def _build_options(self, exec_options):
    """Read the exec_options into self.exec_options

    Args:
      exec_options (str): String formatted as "opt1:val1;opt2:val2"
    """

    if exec_options:
      # exec_options are seperated by ; on the command line
      options = exec_options.split(';')
      for option in options:
        key, value = option.split(':')
        # The keys in ImpalaService QueryOptions are upper case.
        self.exec_options[key.upper()] = value


class QueryExecutor(object):
  """Executes one or more queries.

  Args:
    name (str): eg. "hive"
    query (Query): Container holding 1 or more ;-delimited SQL statements to be executed
    func (function): Function that accepts a QueryExecOption parameter and returns a
      ImpalaQueryResult. Eg. execute_using_impala_beeswax
    config (QueryExecOption)
    exit_on_error (boolean): Exit right after an error encountered.

  Attributes:
    exec_func (function): Function that accepts a QueryExecOption parameter and returns
      an ImpalaQueryResult.
    exec_config (QueryExecOption)
    query (Query): Container holding 1 or more ;-delimited SQL statements to be executed
    exit_on_error (boolean): Exit right after an error encountered.
    executor_name (str): eg. "hive"
    result (ImpalaQueryResult): Contains the result after execute method is called.
  """

  def __init__(self, name, query, func, config, exit_on_error):
    self.exec_func = func
    self.exec_config = config
    self.query = query
    self.exit_on_error = exit_on_error
    self.executor_name = name
    self._result = None

  def prepare(self, impalad):
    """Prepare the query to be run.

    For now, this sets the impalad that the query connects to. If the executor is hive,
    it's a no op.
    """
    if 'hive' not in self.executor_name:
      self.exec_config.impalad = impalad

  def execute(self, plan_first=False):
    """Execute a set of SQL statements using the given execution function,
    and return a result object.  SQL statements can be the familiar SELECT, INSERT,
    DELETE, UPDATE and UPSERT DML commands as well as utilities like SET, SHOW,
    VERSION, etc.

    If plan_first is true, EXPLAIN the "explainable" queries in the set
    first so timing does not include the initial metadata loading
    required for planning.

    This function furnishes a query result object in self._result, for the last
    query in the batch ONLY.
    """
    assert isinstance(self.query, Query)
    orig_query_str = self.query.query_str
    try:
      statements = self.query.query_str.split(';')

      if plan_first:
        # Break out multiple statements in self.query
        for stmt in statements:
          ddl_crud_match = DDL_CRUD_PATTERN.match(stmt)
          if not ddl_crud_match:
            # Don't EXPLAIN this statement
            continue

          self.query.query_str = 'EXPLAIN ' + stmt + ';'
          LOG.debug('Planning %s' % self.query.query_str)
          self.exec_func(self.query, self.exec_config)

      # Now actually execute
      for stmt in statements:
        self.query.query_str = stmt + ';'
        LOG.debug('Executing %s' % self.query.query_str)
        self._result = self.exec_func(self.query, self.exec_config)

        if not self._result.success:
          if self.exit_on_error:
            raise RuntimeError(self._result.query_error)
          else:
            LOG.info("Continuing execution")
    finally:
      self.query.query_str = orig_query_str

    # We do not need to restore the SET options we changed for the next batch
    # (Query object) because the scheduler runs each Query on its own connection
    # (XXX pretty strange for a performance test. :)

  @property
  def result(self):
    """Getter for the result of the query execution.

    A result is a ImpalaQueryResult object that contains the details of a single run of
    the query.
    """
    return self._result
