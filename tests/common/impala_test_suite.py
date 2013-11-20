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
# The base class that should be used for almost all Impala tests

import logging
import os
import pprint
import pytest
from functools import wraps
from random import choice
from tests.beeswax.impala_beeswax import ImpalaBeeswaxClient
from tests.common.impala_service import ImpaladService
from tests.common.test_dimensions import *
from tests.common.test_result_verifier import *
from tests.common.test_vector import *
from tests.common.workload_runner import Query
from tests.util.test_file_parser import *
from tests.util.thrift_util import create_transport
from tests.common.base_test_suite import BaseTestSuite
from tests.common.query_executor import JdbcQueryExecOptions, execute_using_jdbc
from tests.util.hdfs_util import HdfsConfig, get_hdfs_client, get_hdfs_client_from_conf

# Imports required for Hive Metastore Client
from hive_metastore import ThriftHiveMetastore
from thrift.transport import TTransport, TSocket
from thrift.protocol import TBinaryProtocol

logging.basicConfig(level=logging.INFO, format='%(threadName)s: %(message)s')
LOG = logging.getLogger('impala_test_suite')

IMPALAD_HOST_PORT_LIST = pytest.config.option.impalad.split(',')
assert len(IMPALAD_HOST_PORT_LIST) > 0, 'Must specify at least 1 impalad to target'
IMPALAD = IMPALAD_HOST_PORT_LIST[0]
IMPALAD_HS2_HOST_PORT =\
    IMPALAD.split(':')[0] + ":" + pytest.config.option.impalad_hs2_port
HIVE_HS2_HOST_PORT = pytest.config.option.hive_server2
WORKLOAD_DIR = os.environ['IMPALA_WORKLOAD_DIR']
HDFS_CONF = HdfsConfig(pytest.config.option.minicluster_xml_conf)

# Base class for Impala tests. All impala test cases should inherit from this class
class ImpalaTestSuite(BaseTestSuite):
  @classmethod
  def add_test_dimensions(cls):
    """
    A hook for adding additional dimensions.

    By default load the table_info and exec_option dimensions, but if a test wants to
    add more dimensions or different dimensions they can override this function.
    """
    super(ImpalaTestSuite, cls).add_test_dimensions()
    cls.TestMatrix.add_dimension(
        cls.create_table_info_dimension(cls.exploration_strategy()))
    cls.TestMatrix.add_dimension(cls.__create_exec_option_dimension())

  @classmethod
  def setup_class(cls):
    """Setup section that runs before each test suite"""
    cls.hive_client, cls.client = [None, None]
    # Create a Hive Metastore Client (used for executing some test SETUP steps
    metastore_host, metastore_port = pytest.config.option.metastore_server.split(':')
    cls.hive_transport = create_transport(
        use_kerberos=pytest.config.option.use_kerberos,
        host=metastore_host,
        port=metastore_port,
        service=pytest.config.option.hive_service_name)
    protocol = TBinaryProtocol.TBinaryProtocol(cls.hive_transport)
    cls.hive_client = ThriftHiveMetastore.Client(protocol)
    cls.hive_transport.open()

    # The ImpalaBeeswaxClient is used to execute queries in the test suite
    cls.client = cls.create_impala_client(IMPALAD)

    cls.impalad_test_service = ImpaladService(IMPALAD.split(':')[0])
    if pytest.config.option.namenode_http_address is None:
      cls.hdfs_client = get_hdfs_client_from_conf(HDFS_CONF)
    else:
      host, port = pytest.config.option.namenode_http_address.split(":")
      cls.hdfs_client = get_hdfs_client()

  @classmethod
  def teardown_class(cls):
    """Setup section that runs after each test suite"""
    # Cleanup the Impala and Hive Metastore client connections
    if cls.hive_transport:
      cls.hive_transport.close()

    if cls.client:
      cls.client.close_connection()

  @classmethod
  def create_impala_client(cls, host_port=IMPALAD):
    client = ImpalaBeeswaxClient(host_port,
        use_kerberos=pytest.config.option.use_kerberos)
    client.connect()
    return client

  def cleanup_db(self, db_name):
    # To drop a db, we need to first drop all the tables in that db
    self.client.execute("use default")
    self.client.set_query_options({'sync_ddl': 1})
    if db_name in self.client.execute("show databases", ).data:
      for tbl_name in self.client.execute("show tables in " + db_name).data:
        full_tbl_name = '%s.%s' % (db_name, tbl_name)
        result = self.client.execute("describe formatted " + full_tbl_name)
        if 'VIRTUAL_VIEW' in '\n'.join(result.data):
          self.client.execute("drop view " + full_tbl_name)
        else:
          self.client.execute("drop table " + full_tbl_name)
      for fn_name in self.client.execute("show functions in " + db_name).data:
        self.client.execute("drop function %s.%s" % (db_name, fn_name))
      for fn_name in self.client.execute("show aggregate functions in " + db_name).data:
        self.client.execute("drop function %s.%s" % (db_name, fn_name))
      self.client.execute("drop database " + db_name)

  def run_test_case(self, test_file_name, vector, use_db=None, multiple_impalad=False):
    """
    Runs the queries in the specified test based on the vector values

    Runs the query using targeting the file format/compression specified in the test
    vector and the exec options specified in the test vector. If multiple_impalad=True
    a connection to a random impalad will be chosen to execute each test section.
    Otherwise, the default impalad client will be used.
    """
    table_format_info = vector.get_value('table_format')
    exec_options = vector.get_value('exec_option')

    target_impalad_clients = list()
    if multiple_impalad:
      target_impalad_clients =\
          map(ImpalaTestSuite.create_impala_client, IMPALAD_HOST_PORT_LIST)
    else:
      target_impalad_clients = [self.client]

    # Change the database to reflect the file_format, compression codec etc, or the
    # user specified database for all targeted impalad.
    for impalad_client in target_impalad_clients:
      ImpalaTestSuite.change_database(impalad_client, table_format_info, use_db)

    sections = self.load_query_test_file(self.get_workload(), test_file_name)
    for test_section in sections:
      if 'QUERY' not in test_section:
        assert 0, 'Error in test file %s. Test cases require a -- QUERY section.\n%s' %\
            (test_file_name, pprint.pformat(test_section))

      if 'SETUP' in test_section:
        self.execute_test_case_setup(test_section['SETUP'], table_format_info)

      # TODO: support running query tests against different scale factors
      query = QueryTestSectionReader.build_query(test_section['QUERY'])

      if 'QUERY_NAME' in test_section:
        LOG.info('Query Name: \n%s\n' % test_section['QUERY_NAME'])

      # Support running multiple queries within the same test section, only verifying the
      # result of the final query. The main use case is to allow for 'USE database'
      # statements before a query executes, but it is not limited to that.
      # TODO: consider supporting result verification of all queries in the future
      result = None
      target_impalad_client = choice(target_impalad_clients)
      for query in query.split(';'):
        result =\
            self.execute_query_expect_success(target_impalad_client, query, exec_options)
      assert result is not None

      verify_raw_results(test_section, result,
                         vector.get_value('table_format').file_format,
                         pytest.config.option.update_results)
    if pytest.config.option.update_results:
      output_file = os.path.join('/tmp', test_file_name.replace('/','_') + ".test")
      write_test_file(output_file, sections)

  def execute_test_case_setup(self, setup_section, table_format):
    """
    Executes a test case 'SETUP' section

    The test case 'SETUP' section is mainly used for insert tests. These tests need to
    have some actions performed before each test case to ensure the target tables are
    empty. The current supported setup actions:
    RESET <table name> - Drop and recreate the table
    DROP PARTITIONS <table name> - Drop all partitions from the table
    RELOAD - Reload the catalog
    """
    setup_section = QueryTestSectionReader.build_query(setup_section)
    for row in setup_section.split('\n'):
      row = row.lstrip()
      if row.startswith('RESET'):
        db_name, table_name = QueryTestSectionReader.get_table_name_components(\
          table_format, row.split('RESET')[1])
        self.__reset_table(db_name, table_name)
        self.client.execute("invalidate metadata " + db_name + "." + table_name)
      elif row.startswith('DROP PARTITIONS'):
        db_name, table_name = QueryTestSectionReader.get_table_name_components(\
          table_format, row.split('DROP PARTITIONS')[1])
        self.__drop_partitions(db_name, table_name)
        self.client.execute("invalidate metadata " + db_name + "." + table_name)
      elif row.startswith('RELOAD'):
        self.client.refresh()
      else:
        assert False, 'Unsupported setup command: %s' % row

  @classmethod
  def change_database(cls, impala_client, table_format=None, db_name=None):
    if db_name == None:
      assert table_format != None
      db_name =  QueryTestSectionReader.get_db_name(table_format)
    query = 'use %s' % db_name
    # Clear the exec_options before executing a USE statement.
    # The USE statement should not fail for negative exec_option tests.
    impala_client.clear_query_options()
    impala_client.execute(query)

  def execute_wrapper(function):
    """
    Issues a use database query before executing queries.

    Database names are dependent on the input format for table, which the table names
    remaining the same. A use database is issued before query execution. As such,
    dabase names need to be build pre execution, this method wraps around the different
    execute methods and provides a common interface to issue the proper use command.
    """
    @wraps(function)
    def wrapper(*args, **kwargs):
      table_format = None
      if kwargs.get('table_format'):
        table_format = kwargs.get('table_format')
        del kwargs['table_format']
      if kwargs.get('vector'):
        table_format = kwargs.get('vector').get_value('table_format')
        del kwargs['vector']
        # self is the implicit first argument
      if table_format is not None:
        args[0].change_database(args[0].client, table_format)
      return function(*args, **kwargs)
    return wrapper

  @execute_wrapper
  def execute_query_expect_success(self, impalad_client, query, query_exec_options=None):
    """Executes a query and asserts if the query fails"""
    result = self.__execute_query(impalad_client, query, query_exec_options)
    assert result.success
    return result

  @execute_wrapper
  def execute_query(self, query, query_exec_options=None):
    return self.__execute_query(self.client, query, query_exec_options)

  def execute_query_using_client(self, client, query, vector):
    self.change_database(client, vector.get_value('table_format'))
    return client.execute(query)

  @execute_wrapper
  def execute_query_async(self, query, query_exec_options=None):
    self.__set_exec_options(self.client, query_exec_options)
    return self.client.execute_query_async(query)

  @execute_wrapper
  def execute_scalar(self, query, query_exec_options=None):
    result = self.__execute_query(self.client, query, query_exec_options)
    assert len(result.data) <= 1, 'Multiple values returned from scalar'
    return result.data[0] if len(result.data) == 1 else None

  def exec_and_compare_hive_and_impala_hs2(self, stmt):
    """Compare Hive and Impala results when executing the same statment over HS2"""
    # execute_using_jdbc expects a Query object. Convert the query string into a Query
    # object
    query = Query()
    query.query_str = stmt
    # Run the statement targeting Hive
    exec_opts = JdbcQueryExecOptions(iterations=1, impalad=HIVE_HS2_HOST_PORT)
    hive_results = execute_using_jdbc(query, exec_opts).data

    # Run the statement targeting Impala
    exec_opts = JdbcQueryExecOptions(iterations=1, impalad=IMPALAD_HS2_HOST_PORT)
    impala_results = execute_using_jdbc(query, exec_opts).data

    # Compare the results
    assert (impala_results is not None) and (hive_results is not None)
    for impala, hive in zip(impala_results, hive_results):
      assert impala == hive

  def load_query_test_file(self, workload, file_name, valid_section_names=None):
    """
    Loads/Reads the specified query test file. Accepts the given section names as valid.
    Uses a default list of valid section names if valid_section_names is None.
    """
    test_file_path = os.path.join(WORKLOAD_DIR, workload, 'queries', file_name + '.test')
    if not os.path.isfile(test_file_path):
      assert False, 'Test file not found: %s' % file_name
    return parse_query_test_file(test_file_path, valid_section_names)

  def __drop_partitions(self, db_name, table_name):
    """Drops all partitions in the given table"""
    for partition in self.hive_client.get_partition_names(db_name, table_name, 0):
      self.hive_client.drop_partition_by_name(db_name, table_name, partition, True)

  def __execute_query(self, impalad_client, query, query_exec_options=None):
    """Executes the given query against the specified Impalad"""
    LOG.info('Executing Query(%s): \n%s\n' % (impalad_client.impalad, query))
    self.__set_exec_options(impalad_client, query_exec_options)
    return impalad_client.execute(query)

  def __execute_query_new_client(self, query, query_exec_options=None,
      use_kerberos=False):
    """Executes the given query against the specified Impalad"""
    new_client = self.create_impala_client()
    self.__set_exec_options(new_client, query_exec_options)
    return new_client.execute(query)

  def __set_exec_options(self, impalad_client, query_exec_options):
    # Set the specified query exec options, if specified
    impalad_client.clear_query_options()
    if query_exec_options is not None:
      impalad_client.set_query_options(query_exec_options)

  def __reset_table(self, db_name, table_name):
    """Resets a table (drops and recreates the table)"""
    table = self.hive_client.get_table(db_name, table_name)
    assert table is not None
    self.hive_client.drop_table(db_name, table_name, True)
    self.hive_client.create_table(table)

  @classmethod
  def create_table_info_dimension(cls, exploration_strategy):
    # If the user has specified a specific set of table formats to run against, then
    # use those. Otherwise, load from the workload test vectors.
    if pytest.config.option.table_formats:
      table_formats = list()
      for tf in pytest.config.option.table_formats.split(','):
        dataset = get_dataset_from_workload(cls.get_workload())
        table_formats.append(TableFormatInfo.create_from_string(dataset, tf))
      return TestDimension('table_format', *table_formats)
    else:
      return load_table_info_dimension(cls.get_workload(), exploration_strategy)

  @classmethod
  def __create_exec_option_dimension(cls):
    cluster_sizes = ALL_CLUSTER_SIZES
    disable_codegen_options = ALL_DISABLE_CODEGEN_OPTIONS
    batch_sizes = ALL_BATCH_SIZES
    if cls.exploration_strategy() == 'core':
      disable_codegen_options = [False]
      cluster_sizes = ALL_NODES_ONLY
    return create_exec_option_dimension(cluster_sizes, disable_codegen_options,
                                        batch_sizes)

  @classmethod
  def exploration_strategy(cls):
    default_strategy = pytest.config.option.exploration_strategy
    if pytest.config.option.workload_exploration_strategy:
      workload_strategies = pytest.config.option.workload_exploration_strategy.split(',')
      for workload_strategy in workload_strategies:
        workload_strategy = workload_strategy.split(':')
        if len(workload_strategy) != 2:
          raise ValueError, 'Invalid workload:strategy format: %s' % workload_strategy
        if cls.get_workload() == workload_strategy[0]:
          return workload_strategy[1]
    return default_strategy
