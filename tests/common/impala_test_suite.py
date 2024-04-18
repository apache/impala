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
# The base class that should be used for almost all Impala tests

from __future__ import absolute_import, division, print_function
from builtins import range, round
import glob
import grp
import json
import logging
import os
import pprint
import pwd
import pytest
import re
import requests
import socket
import subprocess
import time
import string
from functools import wraps
from getpass import getuser
from impala.hiveserver2 import HiveServer2Cursor
from random import choice
from subprocess import check_call
from tests.common.base_test_suite import BaseTestSuite
from tests.common.environ import (
    HIVE_MAJOR_VERSION,
    MANAGED_WAREHOUSE_DIR,
    EXTERNAL_WAREHOUSE_DIR)
from tests.common.errors import Timeout
from tests.common.impala_connection import create_connection
from tests.common.impala_service import ImpaladService
from tests.common.test_dimensions import (
    ALL_BATCH_SIZES,
    ALL_DISABLE_CODEGEN_OPTIONS,
    ALL_NODES_ONLY,
    TableFormatInfo,
    create_exec_option_dimension,
    get_dataset_from_workload,
    load_table_info_dimension)
from tests.common.test_result_verifier import (
    try_compile_regex,
    verify_lineage,
    verify_raw_results,
    verify_runtime_profile)
from tests.common.test_vector import ImpalaTestDimension
from tests.performance.query import Query
from tests.performance.query_exec_functions import execute_using_jdbc
from tests.performance.query_executor import JdbcQueryExecConfig
from tests.util.filesystem_utils import (
    IS_S3,
    IS_OZONE,
    IS_ABFS,
    IS_ADLS,
    IS_GCS,
    IS_COS,
    IS_OSS,
    IS_OBS,
    IS_HDFS,
    S3_BUCKET_NAME,
    S3GUARD_ENABLED,
    ADLS_STORE_NAME,
    FILESYSTEM_PREFIX,
    FILESYSTEM_NAME,
    FILESYSTEM_URI_SCHEME)

from tests.util.hdfs_util import (
  HdfsConfig,
  get_webhdfs_client,
  get_webhdfs_client_from_conf,
  NAMENODE,
  DelegatingHdfsClient,
  HadoopFsCommandLineClient)
from tests.util.test_file_parser import (
  QueryTestSectionReader,
  parse_query_test_file,
  write_test_file)
from tests.util.thrift_util import create_transport

# Imports required for Hive Metastore Client
from hive_metastore import ThriftHiveMetastore
from thrift.protocol import TBinaryProtocol

# Initializing the logger before conditional imports, since we will need it
# for them.
LOG = logging.getLogger('impala_test_suite')

# The ADLS python client isn't downloaded when ADLS isn't the target FS, so do a
# conditional import.
if IS_ADLS:
  try:
    from tests.util.adls_util import ADLSClient
  except ImportError:
    LOG.error("Need the ADLSClient for testing with ADLS")

IMPALAD_HOST_PORT_LIST = pytest.config.option.impalad.split(',')
assert len(IMPALAD_HOST_PORT_LIST) > 0, 'Must specify at least 1 impalad to target'
IMPALAD = IMPALAD_HOST_PORT_LIST[0]
IMPALAD_HOSTNAME = IMPALAD.split(':')[0]
IMPALAD_HOSTNAME_LIST = [s.split(':')[0] for s in IMPALAD_HOST_PORT_LIST]
IMPALAD_BEESWAX_PORT_LIST = [int(s.split(':')[1]) for s in IMPALAD_HOST_PORT_LIST]
IMPALAD_BEESWAX_PORT = IMPALAD_BEESWAX_PORT_LIST[0]
IMPALAD_BEESWAX_HOST_PORT = IMPALAD_HOST_PORT_LIST[0]

IMPALAD_HS2_PORT = int(pytest.config.option.impalad_hs2_port)
IMPALAD_HS2_HOST_PORT = IMPALAD_HOSTNAME + ":" + str(IMPALAD_HS2_PORT)
# Calculate the hs2 ports based on the first hs2 port and the deltas of the beeswax ports
IMPALAD_HS2_HOST_PORT_LIST = [
    IMPALAD_HOSTNAME_LIST[i] + ':' +
    str(IMPALAD_BEESWAX_PORT_LIST[i] - IMPALAD_BEESWAX_PORT + IMPALAD_HS2_PORT)
    for i in range(len(IMPALAD_HOST_PORT_LIST))
]

IMPALAD_HS2_HTTP_PORT = int(pytest.config.option.impalad_hs2_http_port)
IMPALAD_HS2_HTTP_HOST_PORT = IMPALAD_HOSTNAME + ":" + str(IMPALAD_HS2_HTTP_PORT)
# Calculate the hs2-http ports based on the first hs2-http port and the deltas of the
# beeswax ports
IMPALAD_HS2_HTTP_HOST_PORT_LIST = [
    IMPALAD_HOSTNAME_LIST[i] + ':' +
    str(IMPALAD_BEESWAX_PORT_LIST[i] - IMPALAD_BEESWAX_PORT + IMPALAD_HS2_HTTP_PORT)
    for i in range(len(IMPALAD_HOST_PORT_LIST))
]

STRICT_HS2_HOST_PORT =\
    IMPALAD_HOSTNAME + ":" + pytest.config.option.strict_hs2_port
STRICT_HS2_HTTP_HOST_PORT =\
    IMPALAD_HOSTNAME + ":" + pytest.config.option.strict_hs2_http_port
HIVE_HS2_HOST_PORT = pytest.config.option.hive_server2
WORKLOAD_DIR = os.environ['IMPALA_WORKLOAD_DIR']
HDFS_CONF = HdfsConfig(pytest.config.option.minicluster_xml_conf)
TARGET_FILESYSTEM = os.getenv("TARGET_FILESYSTEM") or "hdfs"
IMPALA_HOME = os.getenv("IMPALA_HOME")
INTERNAL_LISTEN_HOST = os.getenv("INTERNAL_LISTEN_HOST")
# Some tests use the IP instead of the host.
INTERNAL_LISTEN_IP = socket.gethostbyname_ex(INTERNAL_LISTEN_HOST)[2][0]
EE_TEST_LOGS_DIR = os.getenv("IMPALA_EE_TEST_LOGS_DIR")
# Match any SET statement. Assume that query options' names
# only contain alphabets, underscores and digits after position 1.
# The statement may include SQL line comments starting with --, which we need to
# strip out. The test file parser already strips out comments starting with #.
COMMENT_LINES_REGEX = r'(?:\s*--.*\n)*'
SET_PATTERN = re.compile(
    COMMENT_LINES_REGEX + r'\s*set\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*=*', re.I)

METRICS_URL = 'http://{0}:25000/metrics?json'.format(IMPALAD_HOSTNAME)
VARZ_URL = 'http://{0}:25000/varz?json'.format(IMPALAD_HOSTNAME)

GROUP_NAME = grp.getgrgid(pwd.getpwnam(getuser()).pw_gid).gr_name

# Base class for Impala tests. All impala test cases should inherit from this class
class ImpalaTestSuite(BaseTestSuite):
  @classmethod
  def add_test_dimensions(cls, cluster_sizes=None):
    """
    A hook for adding additional dimensions.

    By default load the table_info and exec_option dimensions, but if a test wants to
    add more dimensions or different dimensions they can override this function.
    """
    super(ImpalaTestSuite, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(
        cls.create_table_info_dimension(cls.exploration_strategy()))
    if not cluster_sizes:
      # TODO IMPALA-12394: switch to ALL_CLUSTER_SIZES for exhaustive runs
      cluster_sizes = ALL_NODES_ONLY
    cls.ImpalaTestMatrix.add_dimension(cls.__create_exec_option_dimension(cluster_sizes))
    # Execute tests through Beeswax by default. Individual tests that have been converted
    # to work with the HS2 client can add HS2 in addition to or instead of beeswax.
    cls.ImpalaTestMatrix.add_dimension(ImpalaTestDimension('protocol', 'beeswax'))

  @staticmethod
  def create_hive_client(port):
    """
    Creates a HMS client to a external running metastore service at the provided port
    """
    trans_type = 'buffered'
    if pytest.config.option.use_kerberos:
      trans_type = 'kerberos'
    hive_transport = create_transport(
      host=pytest.config.option.metastore_server.split(':')[0],
      port=port,
      service=pytest.config.option.hive_service_name,
      transport_type=trans_type)
    protocol = TBinaryProtocol.TBinaryProtocol(hive_transport)
    hive_client = ThriftHiveMetastore.Client(protocol)
    hive_transport.open()
    return hive_client, hive_transport

  @classmethod
  def setup_class(cls):
    """Setup section that runs before each test suite"""
    cls.client = None
    cls.hive_client = None
    cls.hs2_client = None
    cls.hs2_http_client = None
    # Create a Hive Metastore Client (used for executing some test SETUP steps
    metastore_host, metastore_port = pytest.config.option.metastore_server.split(':')
    trans_type = 'buffered'
    if pytest.config.option.use_kerberos:
      trans_type = 'kerberos'
    cls.hive_transport = create_transport(
        host=metastore_host,
        port=metastore_port,
        service=pytest.config.option.hive_service_name,
        transport_type=trans_type)
    protocol = TBinaryProtocol.TBinaryProtocol(cls.hive_transport)
    cls.hive_client = ThriftHiveMetastore.Client(protocol)
    cls.hive_transport.open()

    cls.create_impala_clients()

    # Default query options are populated on demand.
    cls.default_query_options = {}
    cls.impalad_test_service = cls.create_impala_service()

    # There are multiple clients for interacting with the underlying storage service.
    #
    # There are two main types of clients: filesystem-specific clients and CLI clients.
    # CLI clients all use the 'hdfs dfs' CLI to execute operations against a target
    # filesystem.
    #
    # 'filesystem_client' is a generic interface for doing filesystem operations that
    # works across all the filesystems that Impala supports. 'filesystem_client' uses
    # either the HDFS command line (e.g. 'hdfs dfs'), a filesystem-specific library, or
    # a wrapper around both, to implement common HDFS operations.
    #
    # *Test writers should always use 'filesystem_client' unless they are using filesystem
    # specific functionality (e.g. HDFS ACLs).*
    #
    # The implementation of 'filesystem_client' for each filesystem is:
    #     HDFS: uses a mixture of pywebhdfs (which is faster than the HDFS CLI) and the
    #           HDFS CLI
    #     S3:   uses the HDFS CLI
    #     ABFS: uses the HDFS CLI
    #     ADLS: uses a mixture of azure-data-lake-store-python and the HDFS CLI (TODO:
    #           this should completely switch to the HDFS CLI once we test it)
    #     GCS:  uses the HDFS CLI
    #     COS:  uses the HDFS CLI
    #
    # 'hdfs_client' is a HDFS-specific client library, and it only works when running on
    # HDFS. When using 'hdfs_client', the test must be skipped on everything other than
    # HDFS. This is only really useful for tests that do HDFS ACL operations. The
    # 'hdfs_client' supports all the methods and functionality of the 'filesystem_client',
    # with additional support for ACL operations such as chmod, chown, getacl, and setacl.
    # 'hdfs_client' is set to None on non-HDFS systems.

    if IS_HDFS:
      cls.hdfs_client = cls.create_hdfs_client()
      cls.filesystem_client = cls.hdfs_client
    elif IS_S3:
      # S3Guard needs filesystem operations to go through the s3 connector. Use the
      # HDFS command line client.
      cls.filesystem_client = HadoopFsCommandLineClient("S3")
    elif IS_ABFS:
      # ABFS is implemented via HDFS command line client
      cls.filesystem_client = HadoopFsCommandLineClient("ABFS")
    elif IS_ADLS:
      cls.filesystem_client = ADLSClient(ADLS_STORE_NAME)
    elif IS_GCS:
      # GCS is implemented via HDFS command line client
      cls.filesystem_client = HadoopFsCommandLineClient("GCS")
    elif IS_COS:
      # COS is implemented via HDFS command line client
      cls.filesystem_client = HadoopFsCommandLineClient("COS")
    elif IS_OSS:
      # OSS is implemented via HDFS command line client
      cls.filesystem_client = HadoopFsCommandLineClient("OSS")
    elif IS_OBS:
      # OBS is implemented via HDFS command line client
      cls.filesystem_client = HadoopFsCommandLineClient("OBS")
    elif IS_OZONE:
      cls.filesystem_client = HadoopFsCommandLineClient("Ozone")

    # Override the shell history path so that commands run by any tests
    # don't write any history into the developer's file.
    os.environ['IMPALA_HISTFILE'] = '/dev/null'

  @classmethod
  def teardown_class(cls):
    """Setup section that runs after each test suite"""
    # Cleanup the Impala and Hive Metastore client connections
    if cls.hive_transport:
      cls.hive_transport.close()
    cls.close_impala_clients()

  @classmethod
  def create_impala_client(cls, host_port=None, protocol='beeswax',
      is_hive=False):
    if host_port is None:
      host_port = cls.__get_default_host_port(protocol)
    client = create_connection(host_port=host_port,
        use_kerberos=pytest.config.option.use_kerberos, protocol=protocol,
        is_hive=is_hive)
    client.connect()
    return client

  @classmethod
  def get_impalad_cluster_size(cls):
    return len(cls.__get_cluster_host_ports('beeswax'))

  @classmethod
  def create_client_for_nth_impalad(cls, nth=0, protocol='beeswax'):
    host_port = cls.__get_cluster_host_ports(protocol)[nth]
    return ImpalaTestSuite.create_impala_client(host_port, protocol=protocol)

  @classmethod
  def create_impala_clients(cls):
    """Creates Impala clients for all supported protocols."""
    # The default connection (self.client) is Beeswax so that existing tests, which assume
    # Beeswax do not need modification (yet).
    cls.client = cls.create_impala_client(protocol='beeswax')
    cls.hs2_client = None
    try:
      cls.hs2_client = cls.create_impala_client(protocol='hs2')
    except Exception as e:
      # HS2 connection can fail for benign reasons, e.g. running with unsupported auth.
      LOG.info("HS2 connection setup failed, continuing...: {0}".format(e))
    cls.hs2_http_client = None
    try:
      cls.hs2_http_client = cls.create_impala_client(protocol='hs2-http')
    except Exception as e:
      # HS2 HTTP connection can fail for benign reasons, e.g. running with unsupported
      # auth.
      LOG.info("HS2 HTTP connection setup failed, continuing...: {0}".format(e))

  @classmethod
  def close_impala_clients(cls):
    """Closes Impala clients created by create_impala_clients()."""
    if cls.client:
      cls.client.close()
      cls.client = None
    if cls.hs2_client:
      cls.hs2_client.close()
      cls.hs2_client = None
    if cls.hs2_http_client:
      cls.hs2_http_client.close()
      cls.hs2_http_client = None

  @classmethod
  def __get_default_host_port(cls, protocol):
    if protocol == 'beeswax':
      return IMPALAD
    elif protocol == 'hs2-http':
      return IMPALAD_HS2_HTTP_HOST_PORT
    else:
      assert protocol == 'hs2'
      return IMPALAD_HS2_HOST_PORT

  @classmethod
  def __get_cluster_host_ports(cls, protocol):
    """Return a list of host/port combinations for all impalads in the cluster."""
    if protocol == 'beeswax':
      return IMPALAD_HOST_PORT_LIST
    elif protocol == 'hs2':
      return IMPALAD_HS2_HOST_PORT_LIST
    elif protocol == 'hs2-http':
      return IMPALAD_HS2_HTTP_HOST_PORT_LIST
    else:
      raise NotImplementedError("Not yet implemented: protocol=" + protocol)

  @classmethod
  def create_impala_service(
      cls, host_port=IMPALAD, webserver_interface="", webserver_port=25000):
    host, port = host_port.split(':')
    if webserver_interface == "":
      webserver_interface = host
    return ImpaladService(host, beeswax_port=port,
        webserver_interface=webserver_interface, webserver_port=webserver_port)

  @classmethod
  def create_hdfs_client(cls):
    if pytest.config.option.namenode_http_address is None:
      webhdfs_client = get_webhdfs_client_from_conf(HDFS_CONF)
    else:
      host, port = pytest.config.option.namenode_http_address.split(":")
      webhdfs_client = get_webhdfs_client(host, port)
    return DelegatingHdfsClient(webhdfs_client, HadoopFsCommandLineClient())

  @classmethod
  def all_db_names(cls):
    results = cls.client.execute("show databases").data
    # Extract first column - database name
    return [row.split("\t")[0] for row in results]

  @classmethod
  def cleanup_db(cls, db_name, sync_ddl=1):
    cls.client.execute("use default")
    cls.client.set_configuration({'sync_ddl': sync_ddl})
    cls.client.execute("drop database if exists `" + db_name + "` cascade")

  def __restore_query_options(self, query_options_changed, impalad_client):
    """
    Restore the list of modified query options to their default values.
    """
    # Populate the default query option if it's empty.
    if not self.default_query_options:
      query_options = impalad_client.get_default_configuration()
      for key, value in query_options.items():
        self.default_query_options[key.upper()] = value
    # Restore all the changed query options.
    for query_option in query_options_changed:
      query_option = query_option.upper()
      if not query_option in self.default_query_options:
        continue
      default_val = self.default_query_options[query_option]
      query_str = 'SET ' + query_option + '="' + default_val + '"'
      try:
        impalad_client.execute(query_str)
      except Exception as e:
        LOG.info('Unexpected exception when executing ' + query_str + ' : ' + str(e))

  def get_impala_partition_info(self, table_name, *include_fields):
    """
    Find information about partitions of a table, as returned by a SHOW PARTITION
    statement. Return a list that contains one tuple for each partition.

    If 'include_fields' is not specified, the tuples will contain all the fields returned
    by SHOW PARTITION. Otherwise, return only those fields whose names are listed in
    'include_fields'. Field names are compared case-insensitively.
    """
    exec_result = self.client.execute('show partitions %s' % table_name)
    fieldSchemas = exec_result.schema.fieldSchemas
    fields_dict = {}
    for idx, fs in enumerate(fieldSchemas):
      fields_dict[fs.name.lower()] = idx

    rows = exec_result.get_data().split('\n')
    rows.pop()
    fields_idx = []
    for fn in include_fields:
      fn = fn.lower()
      assert fn in fields_dict, 'Invalid field: %s' % fn
      fields_idx.append(fields_dict[fn])

    result = []
    for row in rows:
      fields = row.split('\t')
      if not fields_idx:
        result_fields = fields
      else:
        result_fields = []
        for i in fields_idx:
          result_fields.append(fields[i])
      result.append(tuple(result_fields))
    return result

  def get_debug_page(self, page_url):
    """Returns the content of the debug page 'page_url' as json."""
    response = requests.get(page_url)
    assert response.status_code == requests.codes.ok
    return json.loads(response.text)

  def get_var_current_val(self, var):
    """Returns the current value of a given Impalad flag variable."""
    # Parse the /varz endpoint to get the flag information.
    varz = self.get_debug_page(VARZ_URL)
    assert 'flags' in varz.keys()
    filtered_varz = [flag for flag in varz['flags'] if flag['name'] == var]
    assert len(filtered_varz) == 1
    assert 'current' in filtered_varz[0].keys()
    return filtered_varz[0]['current'].strip()

  def get_metric(self, name):
    """Finds the metric with name 'name' and returns its value as an int."""
    def iter_metrics(group):
      for m in group['metrics']:
        yield m
      for c in group['child_groups']:
        for m in iter_metrics(c):
          yield m

    metrics = self.get_debug_page(METRICS_URL)['metric_group']
    for m in iter_metrics(metrics):
      if m['name'] == name:
        return int(m['value'])
    assert False, "Could not find metric: %s" % name

  def __do_replacements(self, s, use_db=None, extra=None):
    globs = globals()
    repl = dict(('$' + k, globs[k]) for k in [
        "FILESYSTEM_PREFIX",
        "FILESYSTEM_NAME",
        "FILESYSTEM_URI_SCHEME",
        "GROUP_NAME",
        "NAMENODE",
        "IMPALA_HOME",
        "INTERNAL_LISTEN_HOST",
        "INTERNAL_LISTEN_IP",
        "MANAGED_WAREHOUSE_DIR",
        "EXTERNAL_WAREHOUSE_DIR"])
    repl.update({
        '$ERASURECODE_POLICY': os.getenv("ERASURECODE_POLICY", "NONE"),
        '$SECONDARY_FILESYSTEM': os.getenv("SECONDARY_FILESYSTEM", ""),
        '$WAREHOUSE_LOCATION_PREFIX': os.getenv("WAREHOUSE_LOCATION_PREFIX", ""),
        '$USER': getuser()})

    if use_db:
      repl['$DATABASE'] = use_db
    elif '$DATABASE' in s:
      raise AssertionError("Query contains $DATABASE but no use_db specified")

    if extra:
      for k, v in extra.items():
        if k in repl:
          raise RuntimeError("Key {0} is reserved".format(k))
        repl[k] = v

    for k, v in repl.items():
      s = s.replace(k, v)
    return s


  def __verify_exceptions(self, expected_strs, actual_str, use_db):
    """
    Verifies that at least one of the strings in 'expected_str' is either:
    * A row_regex: line that matches the actual exception string 'actual_str'
    * A substring of the actual exception string 'actual_str'.
    """
    actual_str = actual_str.replace('\n', '')
    for expected_str in expected_strs:
      # In error messages, some paths are always qualified and some are not.
      # So, allow both $NAMENODE and $FILESYSTEM_PREFIX to be used in CATCH.
      expected_str = self.__do_replacements(expected_str.strip(), use_db=use_db)
      # Remove comments
      expected_str = re.sub(COMMENT_LINES_REGEX, '', expected_str)
      # Strip newlines so we can split error message into multiple lines
      expected_str = expected_str.replace('\n', '')
      expected_regex = try_compile_regex(expected_str)
      if expected_regex:
        if expected_regex.match(actual_str): return
      else:
        # Not a regex - check if expected substring is present in actual.
        if expected_str in actual_str: return
    assert False, 'Unexpected exception string. Expected: %s\nNot found in actual: %s' % \
      (expected_str, actual_str)

  def __verify_results_and_errors(self, vector, test_section, result, use_db):
    """Verifies that both results and error sections are as expected. Rewrites both
      by replacing $NAMENODE, $DATABASE and $IMPALA_HOME with their actual values, and
      optionally rewriting filenames with __HDFS_FILENAME__, to ensure that expected and
      actual values are easily compared.
    """
    replace_filenames_with_placeholder = True
    for section_name in ('RESULTS', 'ERRORS'):
      if section_name in test_section:
        if "$NAMENODE" in test_section[section_name]:
          replace_filenames_with_placeholder = False
        test_section[section_name] = test_section[section_name] \
                                     .replace('$NAMENODE', NAMENODE) \
                                     .replace('$IMPALA_HOME', IMPALA_HOME) \
                                     .replace('$USER', getuser()) \
                                     .replace('$FILESYSTEM_NAME', FILESYSTEM_NAME) \
                                     .replace('$INTERNAL_LISTEN_HOST',
                                              INTERNAL_LISTEN_HOST) \
                                     .replace('$INTERNAL_LISTEN_IP', INTERNAL_LISTEN_IP) \
                                     .replace('$MANAGED_WAREHOUSE_DIR',
                                              MANAGED_WAREHOUSE_DIR) \
                                     .replace('$EXTERNAL_WAREHOUSE_DIR',
                                              EXTERNAL_WAREHOUSE_DIR)
        if use_db:
          test_section[section_name] = test_section[section_name].replace('$DATABASE', use_db)
    result_section, type_section = 'RESULTS', 'TYPES'
    if vector.get_value('protocol').startswith('hs2'):
      # hs2 or hs2-http
      if 'HS2_TYPES' in test_section:
        assert 'TYPES' in test_section,\
            "Base TYPES section must always be included alongside HS2_TYPES"
        # In some cases HS2 types are expected differ from Beeswax types (e.g. see
        # IMPALA-914), so use the HS2-specific section if present.
        type_section = 'HS2_TYPES'
    verify_raw_results(test_section, result, vector.get_value('table_format').file_format,
                       result_section, type_section, pytest.config.option.update_results,
                       replace_filenames_with_placeholder)


  def run_test_case(self, test_file_name, vector, use_db=None, multiple_impalad=False,
      encoding=None, test_file_vars=None):
    """
    Runs the queries in the specified test based on the vector values

    Runs the query using targeting the file format/compression specified in the test
    vector and the exec options specified in the test vector. If multiple_impalad=True
    a connection to a random impalad will be chosen to execute each test section.
    Otherwise, the default impalad client will be used. If 'protocol' (either 'hs2' or
    'beeswax') is set in the vector, a client for that protocol is used. Otherwise we
    use the default: beeswax.

    Additionally, the encoding for all test data can be specified using the 'encoding'
    parameter. This is useful when data is ingested in a different encoding (ex.
    latin). If not set, the default system encoding will be used.
    If a dict 'test_file_vars' is provided, then all keys will be replaced with their
    values in queries before they are executed. Callers need to avoid using reserved key
    names, see 'reserved_keywords' below.
    """
    table_format_info = vector.get_value('table_format')
    exec_options = vector.get_value('exec_option')
    protocol = vector.get_value('protocol')

    target_impalad_clients = list()
    if multiple_impalad:
      target_impalad_clients =\
          [ImpalaTestSuite.create_impala_client(host_port, protocol=protocol)
           for host_port in self.__get_cluster_host_ports(protocol)]
    else:
      if protocol == 'beeswax':
        target_impalad_clients = [self.client]
      elif protocol == 'hs2-http':
        target_impalad_clients = [self.hs2_http_client]
      else:
        assert protocol == 'hs2'
        target_impalad_clients = [self.hs2_client]

    # Change the database to reflect the file_format, compression codec etc, or the
    # user specified database for all targeted impalad.
    for impalad_client in target_impalad_clients:
      ImpalaTestSuite.change_database(impalad_client,
          table_format_info, use_db, pytest.config.option.scale_factor)
      impalad_client.set_configuration(exec_options)

    def __exec_in_impala(query, user=None):
      """
      Helper to execute a query block in Impala, restoring any custom
      query options after the completion of the set of queries.
      """
      # Support running multiple queries within the same test section, only verifying the
      # result of the final query. The main use case is to allow for 'USE database'
      # statements before a query executes, but it is not limited to that.
      # TODO: consider supporting result verification of all queries in the future
      result = None
      target_impalad_client = choice(target_impalad_clients)
      if user:
        # Create a new client so the session will use the new username.
        target_impalad_client = self.create_impala_client(protocol=protocol)
      query_options_changed = []
      try:
        for query in query.split(';'):
          set_pattern_match = SET_PATTERN.match(query)
          if set_pattern_match:
            query_options_changed.append(set_pattern_match.groups()[0])
            assert set_pattern_match.groups()[0] not in vector.get_value("exec_option"), \
                "%s cannot be set in  the '.test' file since it is in the test vector. " \
                "Consider deepcopy()-ing the vector and removing this option in the " \
                "python test." % set_pattern_match.groups()[0]
          result = self.__execute_query(target_impalad_client, query, user=user)
      finally:
        if len(query_options_changed) > 0:
          self.__restore_query_options(query_options_changed, target_impalad_client)
      return result

    def __exec_in_hive(query, user=None):
      """
      Helper to execute a query block in Hive. No special handling of query
      options is done, since we use a separate session for each block.
      """
      h = ImpalaTestSuite.create_impala_client(HIVE_HS2_HOST_PORT, protocol='hs2',
              is_hive=True)
      try:
        result = None
        for query in query.split(';'):
          result = h.execute(query, user=user)
        return result
      finally:
        h.close()

    sections = self.load_query_test_file(self.get_workload(), test_file_name,
        encoding=encoding)
    # Assumes that it is same across all the coordinators.
    lineage_log_dir = self.get_var_current_val('lineage_event_log_dir')
    for test_section in sections:
      if 'HIVE_MAJOR_VERSION' in test_section:
        needed_hive_major_version = int(test_section['HIVE_MAJOR_VERSION'])
        assert needed_hive_major_version in [2, 3]
        assert HIVE_MAJOR_VERSION in [2, 3]
        if needed_hive_major_version != HIVE_MAJOR_VERSION:
          continue

      if 'IS_HDFS_ONLY' in test_section and not IS_HDFS:
        continue

      if 'SHELL' in test_section:
        assert len(test_section) == 1, \
            "SHELL test sections can't contain other sections"
        cmd = self.__do_replacements(test_section['SHELL'], use_db=use_db,
            extra=test_file_vars)
        LOG.info("Shell command: " + cmd)
        check_call(cmd, shell=True)
        continue

      if 'QUERY' in test_section:
        query_section = test_section['QUERY']
        exec_fn = __exec_in_impala
      elif 'HIVE_QUERY' in test_section:
        query_section = test_section['HIVE_QUERY']
        exec_fn = __exec_in_hive
      else:
        assert 0, ('Error in test file %s. Test cases require a ' +
            '-- QUERY or HIVE_QUERY section.\n%s') %\
            (test_file_name, pprint.pformat(test_section))

      # TODO: support running query tests against different scale factors
      query = QueryTestSectionReader.build_query(
          self.__do_replacements(query_section, use_db=use_db, extra=test_file_vars))

      if 'QUERY_NAME' in test_section:
        LOG.info('Query Name: \n%s\n' % test_section['QUERY_NAME'])

      result = None
      try:
        result = exec_fn(query, user=test_section.get('USER', '').strip() or None)
        user = None
        if 'USER' in test_section:
          user = test_section['USER'].strip()
      except Exception as e:
        if 'CATCH' in test_section:
          self.__verify_exceptions(test_section['CATCH'], str(e), use_db)
          continue
        raise

      if 'CATCH' in test_section and '__NO_ERROR__' not in test_section['CATCH']:
        expected_str = self.__do_replacements(" or ".join(test_section['CATCH']).strip(),
              use_db=use_db,
              extra=test_file_vars)
        assert False, "Expected exception: {0}\n\nwhen running:\n\n{1}".format(
            expected_str, query)

      assert result is not None
      assert result.success, "Query failed: {0}".format(result.data)

      # Decode the results read back if the data is stored with a specific encoding.
      if encoding: result.data = [row.decode(encoding) for row in result.data]
      # Replace $NAMENODE in the expected results with the actual namenode URI.
      if 'RESULTS' in test_section:
        # Combining 'RESULTS' with 'DML_RESULTS" is currently unsupported because
        # __verify_results_and_errors calls verify_raw_results which always checks
        # ERRORS, TYPES, LABELS, etc. which doesn't make sense if there are two
        # different result sets to consider (IMPALA-4471).
        assert 'DML_RESULTS' not in test_section
        test_section['RESULTS'] = self.__do_replacements(
            test_section['RESULTS'], use_db=use_db, extra=test_file_vars)
        self.__verify_results_and_errors(vector, test_section, result, use_db)
      else:
        # TODO: Can't validate errors without expected results for now.
        assert 'ERRORS' not in test_section,\
          "'ERRORS' sections must have accompanying 'RESULTS' sections"
      # If --update_results, then replace references to the namenode URI with $NAMENODE.
      # TODO(todd) consider running do_replacements in reverse, though that may cause
      # some false replacements for things like username.
      if pytest.config.option.update_results and 'RESULTS' in test_section:
        test_section['RESULTS'] = test_section['RESULTS'] \
            .replace(NAMENODE, '$NAMENODE') \
            .replace(IMPALA_HOME, '$IMPALA_HOME') \
            .replace(INTERNAL_LISTEN_HOST, '$INTERNAL_LISTEN_HOST') \
            .replace(INTERNAL_LISTEN_IP, '$INTERNAL_LISTEN_IP')
      rt_profile_info = None
      if 'RUNTIME_PROFILE_%s' % table_format_info.file_format in test_section:
        # If this table format has a RUNTIME_PROFILE section specifically for it, evaluate
        # that section and ignore any general RUNTIME_PROFILE sections.
        rt_profile_info = 'RUNTIME_PROFILE_%s' % table_format_info.file_format
      elif 'RUNTIME_PROFILE' in test_section:
        rt_profile_info = 'RUNTIME_PROFILE'

      if rt_profile_info is not None:
        if test_file_vars:
          # only do test_file_vars replacement if it exist.
          test_section[rt_profile_info] = self.__do_replacements(
              test_section[rt_profile_info], extra=test_file_vars)
        rt_profile = verify_runtime_profile(test_section[rt_profile_info],
                               result.runtime_profile,
                               update_section=pytest.config.option.update_results)
        if pytest.config.option.update_results:
          test_section[rt_profile_info] = "".join(rt_profile)

      if 'LINEAGE' in test_section:
         # Lineage flusher thread runs every 5s by default and is not configurable. Wait
         # for that period. (TODO) Get rid of this for faster test execution.
         time.sleep(5)
         current_query_lineage = self.get_query_lineage(result.query_id, lineage_log_dir)
         assert current_query_lineage is not "",\
             "No lineage found for query %s in dir %s" %\
             (result.query_id, lineage_log_dir)
         if pytest.config.option.update_results:
           test_section['LINEAGE'] = json.dumps(current_query_lineage, indent=2,
               separators=(',', ': '))
         else:
           verify_lineage(json.loads(test_section['LINEAGE']), current_query_lineage)

      if 'DML_RESULTS' in test_section:
        assert 'ERRORS' not in test_section
        # The limit is specified to ensure the queries aren't unbounded. We shouldn't have
        # test files that are checking the contents of tables larger than that anyways.
        dml_results_query = "select * from %s limit 1000" % \
            test_section['DML_RESULTS_TABLE']
        dml_result = exec_fn(dml_results_query)
        verify_raw_results(test_section, dml_result,
            vector.get_value('table_format').file_format, result_section='DML_RESULTS',
            update_section=pytest.config.option.update_results)
    if pytest.config.option.update_results:
      output_file = os.path.join(EE_TEST_LOGS_DIR,
                                 test_file_name.replace('/','_') + ".test")
      write_test_file(output_file, sections, encoding=encoding)

  def get_query_lineage(self, query_id, lineage_dir):
    """Walks through the lineage files in lineage_dir to look for a given query_id.
    This is an expensive operation is lineage_dir is large, so use carefully."""
    assert lineage_dir and os.path.isdir(lineage_dir),\
        "Invalid lineage dir %s" % (lineage_dir)
    lineage_files = glob.glob(os.path.join(lineage_dir, 'impala_lineage_log_1.0*'))
    assert len(lineage_files) > 0, "Directory %s is empty" % (lineage_dir)
    # Sort by mtime. Optimized for most recently written lineages.
    lineage_files.sort(key=lambda f: os.path.getmtime(f), reverse=True)
    for f in lineage_files:
      with open(f) as fd:
        # A single file can contain a maxmimum of 5000 entries by default.
        lines = fd.readlines()
        for line in reversed(lines):
          line = line.strip()
          if len(line) == 0: continue
          lineage = json.loads(line)
          assert 'queryId' in lineage.keys()
          if lineage['queryId'] == query_id:
            return lineage
    return ""

  @staticmethod
  def get_db_name_from_format(table_format, scale_factor=''):
    return QueryTestSectionReader.get_db_name(table_format, scale_factor)

  @classmethod
  def change_database(cls, impala_client, table_format=None,
      db_name=None, scale_factor=None):
    if db_name == None:
      assert table_format != None
      db_name = QueryTestSectionReader.get_db_name(table_format,
          scale_factor if scale_factor else '')
    query = 'use %s' % db_name
    # Clear the exec_options before executing a USE statement.
    # The USE statement should not fail for negative exec_option tests.
    impala_client.clear_configuration()
    impala_client.execute(query)

  def execute_wrapper(function):
    """
    Issues a use database query before executing queries.

    Database names are dependent on the input format for table, which the table names
    remaining the same. A use database is issued before query execution. As such,
    database names need to be build pre execution, this method wraps around the different
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

  @classmethod
  @execute_wrapper
  def execute_query_expect_success(cls, impalad_client, query, query_options=None,
      user=None):
    """Executes a query and asserts if the query fails"""
    result = cls.__execute_query(impalad_client, query, query_options, user)
    assert result.success
    return result

  @classmethod
  @execute_wrapper
  def execute_query_expect_failure(cls, impalad_client, query, query_options=None,
      user=None):
    """Executes a query and asserts if the query succeeds"""
    result = None
    try:
      result = cls.__execute_query(impalad_client, query, query_options, user)
    except Exception as e:
      return e

    assert not result.success, "No failure encountered for query %s" % query
    return result

  @execute_wrapper
  def execute_query_unchecked(self, impalad_client, query, query_options=None, user=None):
    return self.__execute_query(impalad_client, query, query_options, user)

  @execute_wrapper
  def execute_query(self, query, query_options=None):
    return self.__execute_query(self.client, query, query_options)

  def exec_and_time(self, query, query_options=None, impalad=0):
    """Executes a given query on the given impalad and returns the time taken in
    millisecondsas seen by the client."""
    client = self.create_client_for_nth_impalad(impalad)
    if query_options is not None:
      client.set_configuration(query_options)
    start_time = int(round(time.time() * 1000))
    client.execute(query)
    end_time = int(round(time.time() * 1000))
    return end_time - start_time

  def execute_query_using_client(self, client, query, vector):
    self.change_database(client, vector.get_value('table_format'))
    query_options = vector.get_value('exec_option')
    if query_options is not None: client.set_configuration(query_options)
    return client.execute(query)

  def execute_query_async_using_client(self, client, query, vector):
    self.change_database(client, vector.get_value('table_format'))
    query_options = vector.get_value('exec_option')
    if query_options is not None: client.set_configuration(query_options)
    return client.execute_async(query)

  def close_query_using_client(self, client, query):
    return client.close_query(query)

  @execute_wrapper
  def execute_query_async(self, query, query_options=None):
    if query_options is not None: self.client.set_configuration(query_options)
    return self.client.execute_async(query)

  @execute_wrapper
  def close_query(self, query):
    return self.client.close_query(query)

  @execute_wrapper
  def execute_scalar(self, query, query_options=None):
    result = self.__execute_query(self.client, query, query_options)
    assert len(result.data) <= 1, 'Multiple values returned from scalar'
    return result.data[0] if len(result.data) == 1 else None

  @classmethod
  @execute_wrapper
  def execute_scalar_expect_success(cls, impalad_client, query, query_options=None,
      user=None):
    """Executes a query and asserts if the query fails"""
    result = cls.__execute_query(impalad_client, query, query_options, user)
    assert result.success
    assert len(result.data) <= 1, 'Multiple values returned from scalar'
    return result.data[0] if len(result.data) == 1 else None

  def exec_and_compare_hive_and_impala_hs2(self, stmt, compare = lambda x, y: x == y):
    """Compare Hive and Impala results when executing the same statment over HS2"""
    # execute_using_jdbc expects a Query object. Convert the query string into a Query
    # object
    query = Query()
    query.query_str = stmt
    # Run the statement targeting Hive
    exec_opts = JdbcQueryExecConfig(impalad=HIVE_HS2_HOST_PORT, transport='SASL')
    hive_results = execute_using_jdbc(query, exec_opts).data

    # Run the statement targeting Impala
    exec_opts = JdbcQueryExecConfig(impalad=IMPALAD_HS2_HOST_PORT, transport='NOSASL')
    impala_results = execute_using_jdbc(query, exec_opts).data

    # Compare the results
    assert (impala_results is not None) and (hive_results is not None)
    assert compare(impala_results, hive_results)

  def exec_with_jdbc(self, stmt):
    """Pass 'stmt' to IMPALA via Impala JDBC client and execute it"""
    # execute_using_jdbc expects a Query object. Convert the query string into a Query
    # object
    query = Query()
    query.query_str = stmt
    # Run the statement targeting Impala
    exec_opts = JdbcQueryExecConfig(impalad=IMPALAD_HS2_HOST_PORT, transport='NOSASL')
    return execute_using_jdbc(query, exec_opts).data

  def exec_with_jdbc_and_compare_result(self, stmt, expected):
    """Execute 'stmt' via Impala JDBC client and compare the result with 'expected'"""
    result = self.exec_with_jdbc(stmt)
    # Check the results
    assert (result is not None) and (result == expected)

  def load_query_test_file(self, workload, file_name, valid_section_names=None,
      encoding=None):
    """
    Loads/Reads the specified query test file. Accepts the given section names as valid.
    Uses a default list of valid section names if valid_section_names is None.
    """
    test_file_path = os.path.join(WORKLOAD_DIR, workload, 'queries', file_name + '.test')
    LOG.info("Loading query test file: %s", test_file_path)
    if not os.path.isfile(test_file_path):
      assert False, 'Test file not found: %s' % file_name
    return parse_query_test_file(test_file_path, valid_section_names, encoding=encoding)

  @classmethod
  def __execute_query(cls, impalad_client, query, query_options=None, user=None):
    """Executes the given query against the specified Impalad"""
    if query_options is not None: impalad_client.set_configuration(query_options)
    return impalad_client.execute(query, user=user)

  def clone_table(self, src_tbl, dst_tbl, recover_partitions, vector):
    src_loc = self._get_table_location(src_tbl, vector)
    self.client.execute("create external table {0} like {1} location '{2}'"\
        .format(dst_tbl, src_tbl, src_loc))
    if recover_partitions:
      self.client.execute("alter table {0} recover partitions".format(dst_tbl))

  def appx_equals(self, a, b, diff_perc):
    """Returns True if 'a' and 'b' are within 'diff_perc' percent of each other,
    False otherwise. 'diff_perc' must be a float in [0,1]."""
    if a == b: return True # Avoid division by 0
    assert abs(a - b) / float(max(abs(a), abs(b))) <= diff_perc

  def _get_table_location(self, table_name, vector):
    """ Returns the HDFS location of the table """
    result = self.execute_query_using_client(self.client,
        "describe formatted %s" % table_name, vector)
    for row in result.data:
      if 'Location:' in row:
        return row.split('\t')[1]
    # This should never happen.
    assert 0, 'Unable to get location for table: ' + table_name

  def run_impala_stmt_in_beeline(self, stmt, username=None, default_db='default'):
    """ Run a statement in impala by Beeline. """
    url = 'jdbc:hive2://localhost:' + pytest.config.option.impalad_hs2_port + '/'\
      + default_db + ";auth=noSasl"
    return self.run_stmt_in_beeline(url, username, stmt)

  # TODO(todd) make this use Thrift to connect to HS2 instead of shelling
  # out to beeline for better performance
  @classmethod
  def run_stmt_in_hive(cls, stmt, username=None):
    """Run a statement in Hive by Beeline."""
    LOG.info("-- executing in HiveServer2\n\n" + stmt + "\n")
    url = 'jdbc:hive2://' + pytest.config.option.hive_server2
    return cls.run_stmt_in_beeline(url, username, stmt)

  @classmethod
  def run_stmt_in_beeline(cls, url, username, stmt):
    """
    Run a statement by Beeline, returning stdout if successful and throwing
    RuntimeError(stderr) if not.
    """
    # Remove HADOOP_CLASSPATH from environment. Beeline doesn't need it,
    # and doing so avoids Hadoop 3's classpath de-duplication code from
    # placing $HADOOP_CONF_DIR too late in the classpath to get the right
    # log4j configuration file picked up. Some log4j configuration files
    # in Hadoop's jars send logging to stdout, confusing Impala's test
    # framework.
    env = os.environ.copy()
    env.pop("HADOOP_CLASSPATH", None)
    call = subprocess.Popen(
        ['beeline',
         # TODO IMPALA-2228: Prevents query output from being confused with log.
         '--silent=true',
         '--outputformat=csv2',
         '-u', url,
         '-n', username or getuser(),
         '-e', stmt],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        # Beeline in Hive 2.1 will read from stdin even when "-e"
        # is specified; explicitly make sure there's nothing to
        # read to avoid hanging, especially when running interactively
        # with py.test.
        stdin=open("/dev/null"),
        env=env)
    (stdout, stderr) = call.communicate()
    call.wait()
    if call.returncode != 0:
      raise RuntimeError(stderr)
    return stdout

  def hive_partition_names(self, table_name):
    """Find the names of the partitions of a table, as Hive sees them.

    The return format is a list of strings. Each string represents a partition
    value of a given column in a format like 'column1=7/column2=8'.
    """
    return self.run_stmt_in_hive(
        'show partitions %s' % table_name).split('\n')[1:-1]

  @classmethod
  def create_table_info_dimension(cls, exploration_strategy):
    # If the user has specified a specific set of table formats to run against, then
    # use those. Otherwise, load from the workload test vectors.
    if pytest.config.option.table_formats:
      table_formats = list()
      for tf in pytest.config.option.table_formats.split(','):
        dataset = get_dataset_from_workload(cls.get_workload())
        table_formats.append(TableFormatInfo.create_from_string(dataset, tf))
      tf_dimensions = ImpalaTestDimension('table_format', *table_formats)
    else:
      tf_dimensions = load_table_info_dimension(cls.get_workload(), exploration_strategy)
    # If 'skip_hbase' is specified or the filesystem is isilon, s3, GCS(gs), COS(cosn) or
    # local, we don't need the hbase dimension.
    if pytest.config.option.skip_hbase or TARGET_FILESYSTEM.lower() \
        in ['s3', 'isilon', 'local', 'abfs', 'adls', 'gs', 'cosn', 'ozone', 'obs']:
      for tf_dimension in tf_dimensions:
        if tf_dimension.value.file_format == "hbase":
          tf_dimensions.remove(tf_dimension)
          break
    return tf_dimensions

  @classmethod
  def __create_exec_option_dimension(cls, cluster_sizes):
    disable_codegen_options = ALL_DISABLE_CODEGEN_OPTIONS
    batch_sizes = ALL_BATCH_SIZES
    exec_single_node_option = [0]
    if cls.exploration_strategy() == 'core':
      disable_codegen_options = [False]
    return create_exec_option_dimension(cluster_sizes, disable_codegen_options,
                                        batch_sizes,
                                        exec_single_node_option=exec_single_node_option,
                                        disable_codegen_rows_threshold_options=[0])

  @classmethod
  def exploration_strategy(cls):
    default_strategy = pytest.config.option.exploration_strategy
    if pytest.config.option.workload_exploration_strategy:
      workload_strategies = pytest.config.option.workload_exploration_strategy.split(',')
      for workload_strategy in workload_strategies:
        workload_strategy = workload_strategy.split(':')
        if len(workload_strategy) != 2:
          raise ValueError('Invalid workload:strategy format: %s' % workload_strategy)
        if cls.get_workload() == workload_strategy[0]:
          return workload_strategy[1]
    return default_strategy

  def wait_for_state(self, handle, expected_state, timeout, client=None):
    """Waits for the given 'query_handle' to reach the 'expected_state' using 'client', or
    with the default connection if 'client' is None. If it does not reach the given state
    within 'timeout' seconds, the method throws an AssertionError.
    """
    self.wait_for_any_state(handle, [expected_state], timeout, client)

  def wait_for_any_state(self, handle, expected_states, timeout, client=None):
    """Waits for the given 'query_handle' to reach one of 'expected_states' using 'client'
    or with the default connection if 'client' is None. If it does not reach one of the
    given states within 'timeout' seconds, the method throws an AssertionError. Returns
    the final state.
    """
    if client is None: client = self.client
    start_time = time.time()
    actual_state = client.get_state(handle)
    while actual_state not in expected_states and time.time() - start_time < timeout:
      actual_state = client.get_state(handle)
      time.sleep(0.5)
    if actual_state not in expected_states:
      timeout_msg = "query '{0}' did not reach one of the expected states {1}, last " \
          "known state {2}".format(self.__get_id_or_query_from_handle(handle),
          expected_states, actual_state)
      raise Timeout(timeout_msg)
    return actual_state

  def wait_for_progress(self, handle, expected_progress, timeout, client=None):
    """Waits for the given query handle to reach expected progress rate"""
    if client is None: client = self.client
    start_time = time.time()
    summary = client.get_exec_summary(handle)
    while time.time() - start_time < timeout and \
        self.__get_query_progress_rate(summary.progress) <= expected_progress:
      summary = client.get_exec_summary(handle)
      time.sleep(0.5)
    actual_progress = self.__get_query_progress_rate(summary.progress)
    if actual_progress <= expected_progress:
      timeout_msg = "query '{0}' did not reach the expected progress {1}, current " \
          "progress {2}".format(self.__get_id_or_query_from_handle(handle),
          expected_progress, actual_progress)
      raise Timeout(timeout_msg)
    return actual_progress

  def __get_query_progress_rate(self, progress):
    if progress is None:
      return 0
    return float(progress.num_completed_scan_ranges) / progress.total_scan_ranges

  def __get_id_or_query_from_handle(self, handle):
    """Returns a query identifier, for QueryHandlers it returns the query id. However,
    Impyla handle is a HiveServer2Cursor that does not have query id, returns the query
    string instead."""
    if isinstance(handle.get_handle(), HiveServer2Cursor):
      return handle.get_handle().query_string
    elif hasattr(handle.get_handle(), 'id'):
      return handle.get_handle().id
    else:
      return "UNIDENTIFIED"

  @classmethod
  def has_value(cls, value, lines):
    """Check if lines contain value."""
    return any([line.find(value) != -1 for line in lines])

  def wait_for_db_to_appear(self, db_name, timeout_s):
    """Wait until the database with 'db_name' is present in the impalad's local catalog.
    Fail after timeout_s if the doesn't appear."""
    start_time = time.time()
    while time.time() - start_time < timeout_s:
      try:
        # This will throw an exception if the database is not present.
        self.client.execute("describe database `{db_name}`".format(db_name=db_name))
        return
      except Exception:
        time.sleep(0.2)
        continue
    raise Exception("DB {0} didn't show up after {1}s", db_name, timeout_s)

  def confirm_db_exists(self, db_name):
    """Confirm the database with 'db_name' is present in the impalad's local catalog.
       Fail if the db is not present"""
    # This will throw an exception if the database is not present.
    self.client.execute("describe database `{db_name}`".format(db_name=db_name))
    return

  def confirm_table_exists(self, db_name, tbl_name):
    """Confirms if the table exists. The describe table command will fail if the table
       does not exist."""
    self.client.execute("describe `{0}`.`{1}`".format(db_name, tbl_name))
    return

  def wait_for_table_to_appear(self, db_name, table_name, timeout_s):
    """Wait until the table with 'table_name' in 'db_name' is present in the
    impalad's local catalog. Fail after timeout_s if the doesn't appear."""
    start_time = time.time()
    while time.time() - start_time < timeout_s:
      try:
        # This will throw an exception if the table is not present.
        self.client.execute("describe `{db_name}`.`{table_name}`".format(
                            db_name=db_name, table_name=table_name))
        return
      except Exception as ex:
        print(str(ex))
        time.sleep(0.2)
        continue
    raise Exception("Table {0}.{1} didn't show up after {2}s", db_name, table_name,
                    timeout_s)

  def assert_eventually(self, timeout_s, period_s, condition, error_msg=None):
    """Assert that the condition (a function with no parameters) returns True within the
    given timeout. The condition is executed every period_s seconds. The check assumes
    that once the condition returns True, it continues to return True. Throws a Timeout
    if the condition does not return true within timeout_s seconds. 'error_msg' is an
    optional function that must return a string. If set, the result of the function will
    be included in the Timeout error message."""
    count = 0
    start_time = time.time()
    while not condition() and time.time() - start_time < timeout_s:
      time.sleep(period_s)
      count += 1
    if not condition():
      error_msg_str = " error message: " + error_msg() if error_msg else ""
      raise Timeout(
        "Check failed to return True after {0} tries and {1} seconds{2}".format(
          count, timeout_s, error_msg_str))

  def assert_impalad_log_contains(self, level, line_regex, expected_count=1, timeout_s=6,
      dry_run=False):
    """
    Convenience wrapper around assert_log_contains for impalad logs.
    """
    return self.assert_log_contains(
        "impalad", level, line_regex, expected_count, timeout_s, dry_run)

  def assert_catalogd_log_contains(self, level, line_regex, expected_count=1,
      timeout_s=6, dry_run=False):
    """
    Convenience wrapper around assert_log_contains for catalogd logs.
    """
    return self.assert_log_contains(
        "catalogd", level, line_regex, expected_count, timeout_s, dry_run)

  def assert_log_contains(self, daemon, level, line_regex, expected_count=1, timeout_s=6,
      dry_run=False):
    """
    Assert that the daemon log with specified level (e.g. ERROR, WARNING, INFO) contains
    expected_count lines with a substring matching the regex. When expected_count is -1,
    at least one match is expected.
    Retries until 'timeout_s' has expired. The default timeout is the default minicluster
    log buffering time (5 seconds) with a one second buffer.
    When using this method to check log files of running processes, the caller should
    make sure that log buffering has been disabled, for example by adding
    '-logbuflevel=-1' to the daemon startup options or set timeout_s to a value higher
    than the log flush interval.

    Returns the result of the very last call to line_regex.search or None if
    expected_count is 0 or the line_regex did not match any lines.
    """
    pattern = re.compile(line_regex)
    start_time = time.time()
    while True:
      try:
        found = 0
        if hasattr(self, "impala_log_dir"):
          log_dir = self.impala_log_dir
        else:
          log_dir = EE_TEST_LOGS_DIR
        log_file_path = os.path.join(log_dir, daemon + "." + level)
        # Resolve symlinks to make finding the file easier.
        log_file_path = os.path.realpath(log_file_path)
        last_re_result = None
        with open(log_file_path) as log_file:
          for line in log_file:
            re_result = pattern.search(line)
            if re_result:
              found += 1
              last_re_result = re_result
        if not dry_run:
          if expected_count == -1:
            assert found > 0, "Expected at least one line in file %s matching regex '%s'"\
              ", but found none." % (log_file_path, line_regex)
          else:
            assert found == expected_count, \
              "Expected %d lines in file %s matching regex '%s', but found %d lines. "\
              "Last line was: \n%s" %\
              (expected_count, log_file_path, line_regex, found, line)
        return last_re_result
      except AssertionError as e:
        # Re-throw the exception to the caller only when the timeout is expired. Otherwise
        # sleep before retrying.
        if time.time() - start_time > timeout_s:
          raise
        LOG.info("Expected log lines could not be found, sleeping before retrying: %s",
            str(e))
        time.sleep(1)

  @staticmethod
  def get_random_name(prefix='', length=5):
    """
    Gets a random name used to create unique database or table
    """
    assert length > 0
    return prefix + ''.join(choice(string.ascii_lowercase) for i in range(length))
