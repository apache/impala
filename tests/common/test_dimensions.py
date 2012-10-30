#!/usr/bin/env python
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
  def __init__(self, **kwargs):
    self.dataset = kwargs.get('dataset', 'UNKNOWN')
    self.file_format = kwargs.get('file_format', 'text')
    self.compression_codec = kwargs.get('compression_codec', 'none')
    self.compression_type = kwargs.get('compression_type', 'none')

  def __str__(self):
    compression_str = '%s/%s' % (self.compression_codec, self.compression_type)
    if self.compression_codec == 'none' and self.compression_type == 'none':
      compression_str = 'none'
    return '%s/%s' % (self.file_format, compression_str)

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
ALL_BATCH_SIZES = [0, 16, 1]
SMALL_BATCH_SIZES = [16, 1]

ALL_CLUSTER_SIZES = [1, 2]
SINGLE_NODE_ONLY = [1]
ALL_NODES_ONLY = [2]

ALL_DISABLE_CODEGEN_OPTIONS = [True, False]

def create_exec_option_dimension(cluster_sizes=ALL_CLUSTER_SIZES,
                                 disable_codegen_options=ALL_DISABLE_CODEGEN_OPTIONS,
                                 batch_sizes=ALL_BATCH_SIZES):
  """
  Builds a query exec_option test vector

  Exhaustively goes through all the given values for cluster size, llvm options.
  For each combination create an exec option dictionary and return a list of
  all the dictionaries. Each dictionary can be passed via Beeswax to control Impala
  query execution behavior.
  TODO: This has some problems right now - for example if no batch sizes are
  specified then no values will be generated. We can also be smarted about the
  exploration - instead of exhaustively going through the values we could do a pairwise
  exploration based on the given exploration strategy.
  """
  exec_option_vectors = list()
  for batch_size in batch_sizes:
    for cluster_size in cluster_sizes:
      for disable_codegen in disable_codegen_options:
        exec_option = {'batch_size': batch_size,
                       'num_nodes': cluster_size,
                       'disable_codgen': disable_codegen,
                      }
        exec_option_vectors.append(exec_option)

  return TestDimension('exec_option', *exec_option_vectors)
  return exec_option_vectors

def load_table_info_dimension(workload, exploration_strategy, file_formats=None,
                      compression_codecs=None):
  """Loads test vector corresponding to the given workload and exploration strategy"""
  test_vector_file = os.path.join(
      WORKLOAD_DIR, workload, '%s_%s.csv' % (workload, exploration_strategy))

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
