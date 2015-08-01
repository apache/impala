# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
#
# Common test dimensions and associated utility functions.
import logging
import os
from itertools import product
from tests.common.test_vector import TestDimension
from os.path import isfile

WORKLOAD_DIR = os.environ['IMPALA_WORKLOAD_DIR']

# Describes the configuration used to execute a single tests. Contains both the details
# of what specific table format to target along with the exec options (num_nodes, etc)
# to use when running the query.
class TableFormatInfo(object):
  KNOWN_FILE_FORMATS = ['text', 'seq', 'rc', 'parquet', 'avro', 'hbase']
  KNOWN_COMPRESSION_CODECS = ['none', 'snap', 'gzip', 'bzip', 'def', 'lzo']
  KNOWN_COMPRESSION_TYPES = ['none', 'block', 'record']

  def __init__(self, **kwargs):
    self.dataset = kwargs.get('dataset', 'UNKNOWN')
    self.file_format = kwargs.get('file_format', 'text')
    self.compression_codec = kwargs.get('compression_codec', 'none')
    self.compression_type = kwargs.get('compression_type', 'none')
    self.__validate()

  def __validate(self):
    if self.file_format not in TableFormatInfo.KNOWN_FILE_FORMATS:
      raise ValueError, 'Unknown file format: %s' % self.file_format
    if self.compression_codec not in TableFormatInfo.KNOWN_COMPRESSION_CODECS:
      raise ValueError, 'Unknown compression codec: %s' % self.compression_codec
    if self.compression_type not in TableFormatInfo.KNOWN_COMPRESSION_TYPES:
      raise ValueError, 'Unknown compression type: %s' % self.compression_type
    if (self.compression_codec == 'none' or self.compression_type == 'none') and\
        self.compression_codec != self.compression_type:
      raise ValueError, 'Invalid combination of compression codec/type: %s' % str(self)

  @staticmethod
  def create_from_string(dataset, table_format_string):
    """
    Parses a table format string and creates a table format info object from the string

    Expected  input is file_format/compression_codec/[compression_type]. The
    compression_type is optional, defaulting to 'block' if the table is compressed
    or 'none' if the table is uncompressed.
    """
    if table_format_string is None:
      raise ValueError, 'Table format string cannot be None'

    format_parts = table_format_string.strip().split('/')
    if len(format_parts) not in range(2, 4):
      raise ValueError, 'Invalid table format %s' % table_format_string

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

def create_uncompressed_text_dimension(workload):
  dataset = get_dataset_from_workload(workload)
  return TestDimension('table_format',
      TableFormatInfo.create_from_string(dataset, 'text/none'))

def create_parquet_dimension(workload):
  dataset = get_dataset_from_workload(workload)
  return TestDimension('table_format',
      TableFormatInfo.create_from_string(dataset, 'parquet/none'))

# Available Exec Options:
#01: abort_on_error (bool)
#02 max_errors (i32)
#03: disable_codegen (bool)
#04: batch_size (i32)
#05: return_as_ascii (bool)
#06: num_nodes (i32)
#07: max_scan_range_length (i64)
#08: num_scanner_threads (i32)
#09: max_io_buffers (i32)
#10: allow_unsupported_formats (bool)
#11: partition_agg (bool)

# Common sets of values for the exec option vectors
ALL_BATCH_SIZES = [0]

# Don't run with NUM_NODES=1 due to IMPALA-561
# ALL_CLUSTER_SIZES = [0, 1]
ALL_CLUSTER_SIZES = [0]

SINGLE_NODE_ONLY = [1]
ALL_NODES_ONLY = [0]
ALL_DISABLE_CODEGEN_OPTIONS = [True, False]

def create_single_exec_option_dimension():
  """Creates an exec_option dimension that will produce a single test vector"""
  return create_exec_option_dimension(cluster_sizes=ALL_NODES_ONLY,
                                      disable_codegen_options=[False],
                                      batch_sizes=[0])

def create_exec_option_dimension(cluster_sizes=ALL_CLUSTER_SIZES,
                                 disable_codegen_options=ALL_DISABLE_CODEGEN_OPTIONS,
                                 batch_sizes=ALL_BATCH_SIZES,
                                 sync_ddl=None, exec_single_node_option=[0]):
  """
  Builds a query exec option test dimension

  Exhaustively goes through all combinations of the given query option values.
  For each combination create an exec option dictionary and add it as a value in the
  exec option test dimension. Each dictionary can then be passed via Beeswax to control
  Impala query execution behavior.

  TODO: In the future we could generate these values using pairwise to reduce total
  execution time.
  """
  exec_option_dimensions = {
      'abort_on_error': [1],
      'exec_single_node_rows_threshold': exec_single_node_option,
      'batch_size': batch_sizes,
      'disable_codegen': disable_codegen_options,
      'num_nodes': cluster_sizes}

  if sync_ddl is not None:
    exec_option_dimensions['sync_ddl'] = sync_ddl

  # Generate the cross product (all combinations) of the exec options specified. Then
  # store them in exec_option dictionary format.
  keys = sorted(exec_option_dimensions)
  combinations = product(*(exec_option_dimensions[name] for name in keys))
  exec_option_dimension_values = [dict(zip(keys, prod)) for prod in combinations]

  # Build a test vector out of it
  return TestDimension('exec_option', *exec_option_dimension_values)

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
    raise RuntimeError, 'Vector file not found: ' + test_vector_file

  vector_values = []

  with open(test_vector_file, 'rb') as vector_file:
    for line in vector_file.readlines():
      if line.strip().startswith('#'):
        continue

      # Extract each test vector and add them to a dictionary
      vals = dict((key.strip(), value.strip()) for key, value in\
          (item.split(':') for item in line.split(',')))

      # If only loading specific file formats skip anything that doesn't match
      if file_formats is not None and vals['file_format'] not in file_formats:
        continue
      if compression_codecs is not None and\
         vals['compression_codec'] not in compression_codecs:
        continue
      vector_values.append(TableFormatInfo(**vals))

  return TestDimension('table_format', *vector_values)

def is_supported_insert_format(table_format):
  # Returns true if the given table_format is a supported Impala INSERT format
  return table_format.compression_codec == 'none' and\
      table_format.file_format in ['text', 'parquet']

