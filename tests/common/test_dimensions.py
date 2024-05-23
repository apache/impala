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

# Common test dimensions and associated utility functions.

from __future__ import absolute_import, division, print_function
from builtins import range
import copy
import os
from itertools import product

from tests.common.test_vector import ImpalaTestDimension, ImpalaTestVector
from tests.util.filesystem_utils import (
    IS_HDFS)

WORKLOAD_DIR = os.environ['IMPALA_WORKLOAD_DIR']

# Map from the test dimension file_format string to the SQL "STORED AS" or "STORED BY"
# argument.
FILE_FORMAT_TO_STORED_AS_MAP = {
  'text': 'TEXTFILE',
  'seq': 'SEQUENCEFILE',
  'rc': 'RCFILE',
  'orc': 'ORC',
  'parquet': 'PARQUET',
  'hudiparquet': 'HUDIPARQUET',
  'avro': 'AVRO',
  'hbase': "'org.apache.hadoop.hive.hbase.HBaseStorageHandler'",
  'kudu': "KUDU",
  'iceberg': "ICEBERG",
  'json': "JSONFILE",
}


# Describes the configuration used to execute a single tests. Contains both the details
# of what specific table format to target along with the exec options (num_nodes, etc)
# to use when running the query.
class TableFormatInfo(object):
  KNOWN_FILE_FORMATS = ['text', 'seq', 'rc', 'parquet', 'orc', 'avro', 'hbase',
                        'kudu', 'iceberg', 'json']
  KNOWN_COMPRESSION_CODECS = ['none', 'snap', 'gzip', 'bzip', 'def', 'zstd', 'lz4']
  KNOWN_COMPRESSION_TYPES = ['none', 'block', 'record']

  def __init__(self, **kwargs):
    self.dataset = kwargs.get('dataset', 'UNKNOWN')
    self.file_format = kwargs.get('file_format', 'text')
    self.compression_codec = kwargs.get('compression_codec', 'none')
    self.compression_type = kwargs.get('compression_type', 'none')
    self.__validate()

  def __validate(self):
    if self.file_format not in TableFormatInfo.KNOWN_FILE_FORMATS:
      raise ValueError('Unknown file format: %s' % self.file_format)
    if self.compression_codec not in TableFormatInfo.KNOWN_COMPRESSION_CODECS:
      raise ValueError('Unknown compression codec: %s' % self.compression_codec)
    if self.compression_type not in TableFormatInfo.KNOWN_COMPRESSION_TYPES:
      raise ValueError('Unknown compression type: %s' % self.compression_type)
    if (self.compression_codec == 'none' or self.compression_type == 'none') and\
        self.compression_codec != self.compression_type:
      raise ValueError('Invalid combination of compression codec/type: %s' % str(self))

  @staticmethod
  def create_from_string(dataset, table_format_string):
    """
    Parses a table format string and creates a table format info object from the string

    Expected  input is file_format/compression_codec/[compression_type]. The
    compression_type is optional, defaulting to 'block' if the table is compressed
    or 'none' if the table is uncompressed.
    """
    if table_format_string is None:
      raise ValueError('Table format string cannot be None')

    format_parts = table_format_string.strip().split('/')
    if len(format_parts) not in list(range(2, 4)):
      raise ValueError('Invalid table format %s' % table_format_string)

    file_format, compression_codec = format_parts[:2]
    if len(format_parts) == 3:
      compression_type = format_parts[2]
    else:
      # Assume the default compression type is block (of the table is compressed)
      compression_type = 'none' if compression_codec == 'none' else 'block'

    return TableFormatInfo(dataset=dataset, file_format=file_format,
                           compression_codec=compression_codec,
                           compression_type=compression_type)

  def __str__(self):
    compression_str = '%s/%s' % (self.compression_codec, self.compression_type)
    if self.compression_codec == 'none' and self.compression_type == 'none':
      compression_str = 'none'
    return '%s/%s' % (self.file_format, compression_str)

  def db_suffix(self):
    if self.file_format == 'text' and self.compression_codec == 'none':
      return ''
    elif self.compression_codec == 'none':
      return '_%s' % (self.file_format)
    elif self.compression_type == 'record':
      return '_%s_record_%s' % (self.file_format, self.compression_codec)
    else:
      return '_%s_%s' % (self.file_format, self.compression_codec)


def create_table_format_dimension(workload, table_format_string):
  dataset = get_dataset_from_workload(workload)
  return ImpalaTestDimension('table_format',
      TableFormatInfo.create_from_string(dataset, table_format_string))


def create_uncompressed_text_dimension(workload):
  return create_table_format_dimension(workload, 'text/none')


def create_uncompressed_json_dimension(workload):
  return create_table_format_dimension(workload, 'json/none')


def create_parquet_dimension(workload):
  return create_table_format_dimension(workload, 'parquet/none')


def create_orc_dimension(workload):
  return create_table_format_dimension(workload, 'orc/def')


def create_avro_snappy_dimension(workload):
  return create_table_format_dimension(workload, 'avro/snap/block')


def create_kudu_dimension(workload):
  return create_table_format_dimension(workload, 'kudu/none')


def create_client_protocol_dimension():
  # IMPALA-8864: Older python versions do not support SSLContext object that the thrift
  # http client implementation depends on. Falls back to a dimension without http
  # transport.
  import ssl
  if not hasattr(ssl, "create_default_context"):
    return ImpalaTestDimension('protocol', 'beeswax', 'hs2')
  return ImpalaTestDimension('protocol', 'beeswax', 'hs2', 'hs2-http')


def create_client_protocol_http_transport():
  return ImpalaTestDimension('protocol', 'hs2-http')


def create_client_protocol_strict_dimension():
  # only support strict dimensions if the file system is HDFS, since that is
  # where the hive cluster is run.
  if IS_HDFS:
    return ImpalaTestDimension('strict_hs2_protocol', False, True)
  else:
    return create_client_protocol_no_strict_dimension()


def create_client_protocol_no_strict_dimension():
  return ImpalaTestDimension('strict_hs2_protocol', False)


def hs2_parquet_constraint(v):
  """Constraint function, used to only run HS2 against Parquet format, because file format
  and the client protocol are orthogonal."""
  return (v.get_value('protocol') == 'beeswax' or
          v.get_value('table_format').file_format == 'parquet' and
          v.get_value('table_format').compression_codec == 'none')


def hs2_text_constraint(v):
  """Constraint function, used to only run HS2 against uncompressed text, because file
  format and the client protocol are orthogonal."""
  return (v.get_value('protocol') == 'beeswax' or
          v.get_value('table_format').file_format == 'text' and
          v.get_value('table_format').compression_codec == 'none')


def orc_schema_resolution_constraint(v):
  """ Constraint to use multiple orc_schema_resolution only in case of orc files"""
  file_format = v.get_value('table_format').file_format
  orc_schema_resolution = v.get_value('orc_schema_resolution')
  return file_format == 'orc' or orc_schema_resolution == 0


# Common sets of values for the exec option vectors
ALL_BATCH_SIZES = [0]

# Test SingleNode and Distributed Planners
ALL_CLUSTER_SIZES = [0, 1]

SINGLE_NODE_ONLY = [1]
ALL_NODES_ONLY = [0]
ALL_DISABLE_CODEGEN_OPTIONS = [True, False]


def create_single_exec_option_dimension(num_nodes=0, disable_codegen_rows_threshold=5000):
  """Creates an exec_option dimension that will produce a single test vector"""
  return create_exec_option_dimension(cluster_sizes=[num_nodes],
      disable_codegen_options=[False],
      # Make sure codegen kicks in for functional.alltypes.
      disable_codegen_rows_threshold_options=[disable_codegen_rows_threshold],
      batch_sizes=[0])


# TODO IMPALA-12394: switch to ALL_CLUSTER_SIZES
def create_exec_option_dimension(cluster_sizes=ALL_NODES_ONLY,
                                 disable_codegen_options=ALL_DISABLE_CODEGEN_OPTIONS,
                                 batch_sizes=ALL_BATCH_SIZES,
                                 sync_ddl=None, exec_single_node_option=[0],
                                 # We already run with codegen on and off explicitly -
                                 # don't need automatic toggling.
                                 disable_codegen_rows_threshold_options=[0],
                                 debug_action_options=None):
  exec_option_dimensions = {
      'abort_on_error': [1],
      'exec_single_node_rows_threshold': exec_single_node_option,
      'batch_size': batch_sizes,
      'disable_codegen': disable_codegen_options,
      'disable_codegen_rows_threshold': disable_codegen_rows_threshold_options,
      'num_nodes': cluster_sizes,
      'test_replan': [1]}

  if sync_ddl is not None:
    exec_option_dimensions['sync_ddl'] = sync_ddl
  if debug_action_options is not None:
    exec_option_dimensions['debug_action'] = debug_action_options
  return create_exec_option_dimension_from_dict(exec_option_dimensions)


def create_exec_option_dimension_from_dict(exec_option_dimensions):
  """
  Builds a query exec option test dimension

  Exhaustively goes through all combinations of the given query option values.
  For each combination create an exec option dictionary and add it as a value in the
  exec option test dimension. Each dictionary can then be passed via Beeswax to control
  Impala query execution behavior.

  TODO: In the future we could generate these values using pairwise to reduce total
  execution time.
  """
  # Generate the cross product (all combinations) of the exec options specified. Then
  # store them in exec_option dictionary format.
  keys = sorted(exec_option_dimensions)
  combinations = product(*(exec_option_dimensions[name] for name in keys))
  exec_option_dimension_values = [dict(zip(keys, prod)) for prod in combinations]

  # Build a test vector out of it
  return ImpalaTestDimension('exec_option', *exec_option_dimension_values)


def add_exec_option_dimension(test_suite, key, values):
  """
  Takes an ImpalaTestSuite object 'test_suite' and register new exec option dimension.
  'key' must be a query option known to Impala, and 'values' must be a list of more than
  one element.
  """
  test_suite.ImpalaTestMatrix.add_exec_option_dimension(
    ImpalaTestDimension(key, *values))


def add_mandatory_exec_option(test_suite, key, value):
  """
  Takes an ImpalaTestSuite object 'test_suite' and adds 'key=value' to every exec option
  test dimension, leaving the number of tests that will be run unchanged.
  """
  test_suite.ImpalaTestMatrix.add_mandatory_exec_option(key, value)


def extend_exec_option_dimension(test_suite, key, value):
  """
  Takes an ImpalaTestSuite object 'test_suite' and extends the exec option test dimension
  by creating a copy of each existing exec option value that has 'key' set to 'value',
  doubling the number of tests that will be run.
  """
  dim = test_suite.ImpalaTestMatrix.dimensions["exec_option"]
  new_value = []
  for v in dim:
    new_value.append(ImpalaTestVector.Value(v.name, copy.copy(v.value)))
    new_value[-1].value[key] = value
  dim.extend(new_value)
  test_suite.ImpalaTestMatrix.add_dimension(dim)


def get_dataset_from_workload(workload):
  # TODO: We need a better way to define the workload -> dataset mapping so we can
  # extract it without reading the actual test vector file
  return load_table_info_dimension(workload, 'exhaustive')[0].value.dataset


def load_table_info_dimension(workload_name, exploration_strategy, file_formats=None,
                      compression_codecs=None):
  """Loads test vector corresponding to the given workload and exploration strategy"""
  test_vector_file = os.path.join(
      WORKLOAD_DIR, workload_name, '%s_%s.csv' % (workload_name, exploration_strategy))

  if not os.path.isfile(test_vector_file):
    raise RuntimeError('Vector file not found: ' + test_vector_file)

  vector_values = []

  with open(test_vector_file, 'rb') as vector_file:
    for line in vector_file.readlines():
      if line.strip().startswith('#'):
        continue

      # Extract each test vector and add them to a dictionary
      vals = dict((key.strip(), value.strip()) for key, value in
          (item.split(':') for item in line.split(',')))

      # If only loading specific file formats skip anything that doesn't match
      if file_formats is not None and vals['file_format'] not in file_formats:
        continue
      if compression_codecs is not None and\
         vals['compression_codec'] not in compression_codecs:
        continue
      vector_values.append(TableFormatInfo(**vals))

  return ImpalaTestDimension('table_format', *vector_values)


def is_supported_insert_format(table_format):
  # Returns true if the given table_format is a supported Impala INSERT format
  return table_format.compression_codec == 'none' and\
      table_format.file_format in ['text', 'parquet']
