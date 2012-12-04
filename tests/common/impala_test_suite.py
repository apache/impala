#!/usr/bin/env python
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
#
# The base class that should be used for almost all Impala tests
import logging
import os
import pprint
import pytest
from tests.beeswax.impala_beeswax import ImpalaBeeswaxClient
from tests.common.test_dimensions import *
from tests.common.test_result_verifier import *
from tests.common.test_vector import *
from tests.util.test_file_parser import *
from tests.common.base_test_suite import BaseTestSuite

# Imports required for Hive Metastore Client
from hive_metastore import ThriftHiveMetastore
from thrift.transport import TTransport, TSocket
from thrift.protocol import TBinaryProtocol

logging.basicConfig(level=logging.INFO, format='%(threadName)s: %(message)s')
LOG = logging.getLogger('impala_test_suite')
IMPALAD = pytest.config.option.impalad
WORKLOAD_DIR = os.environ['IMPALA_WORKLOAD_DIR']

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
    cls.TestMatrix.add_dimension(cls.__create_table_info_dimension())
    cls.TestMatrix.add_dimension(cls.__create_exec_option_dimension())

  @classmethod
  def setup_class(cls):
    """Setup section that runs before each test suite"""
    cls.hive_client, cls.client = [None, None]
    # Create a Hive Metastore Client (used for executing some test SETUP steps
    hive_server_host, hive_server_port = pytest.config.option.hive_server.split(':')
    cls.hive_transport = TTransport.TBufferedTransport(TSocket.TSocket(hive_server_host,
                                                      int(hive_server_port)))
    protocol = TBinaryProtocol.TBinaryProtocol(cls.hive_transport)
    cls.hive_client = ThriftHiveMetastore.Client(protocol)
    cls.hive_transport.open()

    # The ImpalaBeeswaxClient is used to execute all queries in the test suite
    cls.client = ImpalaBeeswaxClient(IMPALAD, use_kerberos=False)
    cls.client.connect()

  @classmethod
  def teardown_class(cls):
    """Setup section that runs after each test suite"""
    # Cleanup the Impala and Hive Metastore client connections
    if cls.hive_transport:
      cls.hive_transport.close()

    if cls.client:
      cls.client.close_connection()

  def run_test_case(self, test_file_name, vector):
    """
    Runs the queries in the specified test based on the vector values

    Runs the query using targeting the file format/compression specified in the test
    vector and the exec options specified in the test vector
    """
    table_format_info = vector.get_value('table_format')
    exec_options = vector.get_value('exec_option')

    sections = self.__load_query_test_file(self.get_dataset(), test_file_name)
    updated_sections = list()
    for test_section in sections:
      if 'QUERY' not in test_section:
        assert 0, 'Error in test file %s. Test cases require a -- QUERY section.\n%s' %\
            (test_file_name, pprint.pformat(test_section))

      if 'SETUP' in test_section:
        self.execute_test_case_setup(test_section['SETUP'], table_format_info)
        self.client.refresh()

      # TODO: support running query tests against different scale factors
      query = QueryTestSectionReader.build_query(
          test_section['QUERY'], table_format_info, scale_factor='')

      if 'QUERY_NAME' in test_section:
        LOG.info('Query Name: \n%s\n' % test_section['QUERY_NAME'])

      # Support running multiple queries within the same test section, only verifying the
      # result of the final query. The main use case is to allow for 'USE database'
      # statements before a query executes, but it is not limited to that.
      # TODO: consider supporting result verification of all queries in the future
      result = None
      for query in query.split(';'):
        result = self.execute_query_expect_success(IMPALAD, query, exec_options)
      assert result is not None

      if pytest.config.option.update_results:
        updated_sections.append(
            self.__update_results(test_file_name, test_section, result))
      else:
        verify_raw_results(test_section, result)

    if pytest.config.option.update_results:
      output_file = os.path.join('/tmp', test_file_name.replace('/','_') + ".test")
      write_test_file(output_file, updated_sections)

  def execute_test_case_setup(self, setup_section, vector):
    """
    Executes a test case 'SETUP' section

    The test case 'SETUP' section is mainly used for insert tests. These tests need to
    have some actions performed before each test case to ensure the target tables are
    empty. The current supported setup actions:
    RESET <table name> - Drop and recreate the table
    DROP PARTITIONS <table name> - Drop all partitions from the table
    RELOAD - Reload the catalog
    """
    setup_section = remove_comments(setup_section)
    for row in setup_section.split('\n'):
      row = row.lstrip()
      if row.startswith('RESET'):
        table_name = QueryTestSectionReader.replace_table_suffix(
            row.split('RESET')[1], vector)
        self.__reset_table(table_name.strip())
      elif row.startswith('DROP PARTITIONS'):
        table_name = QueryTestSectionReader.replace_table_suffix(
            row.split('DROP PARTITIONS')[1], vector)
        self.__drop_partitions(table_name.strip())
      elif row.startswith('RELOAD'):
        self.client.refresh()
      else:
        assert False, 'Unsupported setup command: %s' % row

  def execute_query_expect_success(self, impalad, query, query_exec_options=None,
                                   use_kerberos=False):
    """Executes a query and asserts if the query fails"""
    result = self.__execute_query(impalad, query, query_exec_options, use_kerberos)
    assert result.success
    return result

  def execute_query(self, query, query_exec_options=None, use_kerberos=False):
    return self.__execute_query(IMPALAD, query, query_exec_options, use_kerberos)

  def execute_scalar(self, query, query_exec_options=None, use_kerberos=False):
    result = self.__execute_query(IMPALAD, query, query_exec_options, use_kerberos)
    assert len(result.data) <= 1, 'Multiple values returned from scaler'
    return result.data[0] if len(result.data) == 1 else None

  def __drop_partitions(self, table_name):
    """Drops all partitions in the given table"""
    db_name, table_name = ImpalaTestSuite.__get_database_from_table_name(table_name)
    for partition in self.hive_client.get_partition_names(db_name, table_name, 0):
      self.hive_client.drop_partition_by_name(db_name, table_name, partition, True)

  def __execute_query(self, impalad, query, query_exec_options=None, use_kerberos=False):
    """Executes the given query against the specified Impalad"""

    LOG.info('Executing Query: \n%s\n' % query)
    # Set the specified query exec options, if specified
    if query_exec_options is not None and len(query_exec_options.keys()) > 0:
      for exec_option in query_exec_options.keys():
        self.client.set_query_option(exec_option, query_exec_options[exec_option])
    else:
      self.client.clear_query_options()

    # TODO: Remove this in the future for negative testing
    self.client.set_query_option('allow_unsupported_formats', True)

    return self.client.execute(query)

  def __load_query_test_file(self, workload, file_name):
    """Loads/Reads the specified query test file"""
    test_file_path = os.path.join(WORKLOAD_DIR, workload, 'queries', file_name + '.test')
    if not os.path.isfile(test_file_path):
      assert False, 'Test file not found: %s' % file_name
    return parse_query_test_file(test_file_path)

  def __reset_table(self, table_name):
    """Resets a table (drops and recreates the table)"""
    db_name, table_name = ImpalaTestSuite.__get_database_from_table_name(table_name)
    table = self.hive_client.get_table(db_name, table_name)
    assert table is not None
    self.hive_client.drop_table(db_name, table_name, True)
    self.hive_client.create_table(table)

  def __update_results(self, test_file_name, test_section, exec_result):
    if 'PARTITIONS' in test_section:
      test_section['PARTITIONS'] = '\n'.join(parse_result_rows(exec_result))
    else:
      test_section['RESULTS'] = '\n'.join(parse_result_rows(exec_result))

    if 'TYPES' in test_section:
      col_types = [fs.type.upper() for fs in exec_result.schema.fieldSchemas]
      test_section['TYPES'] = ', '.join(col_types)
    return test_section

  @classmethod
  def __create_table_info_dimension(cls):
    return load_table_info_dimension(cls.get_dataset(),
                                     pytest.config.option.exploration_strategy)

  @classmethod
  def __create_exec_option_dimension(cls):
    cluster_sizes = ALL_CLUSTER_SIZES
    disable_codegen_options = ALL_DISABLE_CODEGEN_OPTIONS
    batch_sizes = ALL_BATCH_SIZES
    if pytest.config.option.exploration_strategy == 'core':
      disable_codegen_options = [False]
      batch_sizes = [0, 1]
      cluster_sizes = ALL_NODES_ONLY
    return create_exec_option_dimension(cluster_sizes, disable_codegen_options,
                                        batch_sizes)

  @staticmethod
  def __get_database_from_table_name(table_name):
    """
    Given a fully qualified table name, returns the database name and table name

    If the table name is not fully qualified, then assume 'default' as the database
    """
    split = table_name.split('.')
    assert len(split) <= 2, 'Unexpected table format: %s' % table_name
    return (split[0], split[1]) if len(split) == 2 else ('default', split[0])
