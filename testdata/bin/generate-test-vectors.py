#!/usr/bin/env impala-python
#
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

# This script is used to generate test "vectors" based on a dimension input file.
# A vector in this context is simply a permutation of the values in the the
# dimension input file.  For example, in this case the script is generating test vectors
# for the Impala / Hive benchmark suite so interesting dimensions are data set,
# file format, and compression algorithm. More can be added later.
# The output of running this script is a list of vectors. Currently two different vector
# outputs are generated - an "exhaustive" vector which contains all permutations and a
# "pairwise" vector that contains a subset of the vectors by chosing all combinations of
# pairs (the pairwise strategy). More information about pairwise can be found at
# http://www.pairwise.org.
#
# The end goal is to have a reduced set of test vectors to provide coverage but don't take
# as long to run as the exhaustive set of vectors along with a set of vectors that provide
# full coverage. This is especially important for benchmarks which work on very large data
# sets.
#
# The output files output can then be read in by other tests by other scripts,tools,tests.
# One major use case is the generate_scehma_statements.py script, which uses the vector
# files to dynamically build schema for running benchmark and functional tests.
#
# The pairwise generation is done using the Python 'AllPairs' module. This module can be
# downloaded from http://pypi.python.org/pypi/AllPairs/2.0.1
#
from __future__ import absolute_import, division, print_function
from builtins import range
import collections
import csv
import math
import os
import sys
from itertools import product
from optparse import OptionParser
from allpairspy import AllPairs as all_pairs

parser = OptionParser()
parser.add_option("-w", "--workload", dest="workload",
                  help="The workload to generate test vectors for")
(options, args) = parser.parse_args()

if options.workload is None:
  print("A workload name must be specified.")
  parser.print_help()
  sys.exit(1)

WORKLOAD_DIR = os.environ['IMPALA_WORKLOAD_DIR']

# This array also defines the order of the dimension values. This ordering
# is important because it is used to apply constraints. Add new items to the
# end of the list.
KNOWN_DIMENSION_NAMES = ['file_format', 'dataset', 'compression_codec',
                         'compression_type']

FILE_FORMAT_IDX = KNOWN_DIMENSION_NAMES.index('file_format')
DATASET_IDX = KNOWN_DIMENSION_NAMES.index('dataset')
COMPRESSION_IDX = KNOWN_DIMENSION_NAMES.index('compression_codec')
COMPRESSION_TYPE_IDX = KNOWN_DIMENSION_NAMES.index('compression_type')

class VectorGenerator:
  def __init__(self, input_vectors):
    self.input_vectors = input_vectors

  def generate_pairwise_matrix(self, filter_func = None):
    if filter_func is None:
      filter_func = lambda vector: True
    return all_pairs(self.input_vectors, filter_func = is_valid_combination)

  def generate_exhaustive_matrix(self, filter_func = None):
    if filter_func is None:
      filter_func = lambda vector: True
    return [list(vec) for vec in product(*self.input_vectors) if filter_func(vec)]

# Add vector value constraints to this function.
def is_valid_combination(vector):
  if len(vector) == 4:
    return not (
        (vector[FILE_FORMAT_IDX] == 'text' and vector[COMPRESSION_IDX] in ['def']) or
        (vector[COMPRESSION_IDX] == 'none' and vector[COMPRESSION_TYPE_IDX] != 'none') or
        (vector[COMPRESSION_IDX] != 'none' and vector[COMPRESSION_TYPE_IDX] == 'none') or
        (vector[FILE_FORMAT_IDX] != 'seq' and vector[COMPRESSION_TYPE_IDX] == 'record') or
        (vector[FILE_FORMAT_IDX] == 'parquet' and vector[COMPRESSION_IDX] != 'none') or
        (vector[FILE_FORMAT_IDX] == 'hbase' and vector[COMPRESSION_IDX] != 'none') or
        (vector[FILE_FORMAT_IDX] == 'kudu' and vector[COMPRESSION_IDX] != 'none') or
        (vector[FILE_FORMAT_IDX] == 'avro' and
         vector[COMPRESSION_IDX] not in ['none', 'snap', 'def']))

  # The pairwise generator may call this with different vector lengths. In that case this
  # should always return true.
  return True

# Vector files have the format: <dimension name>: value1, value2, ... this function
# adds all specified dimensions to a map of dimension name-to-value
def read_dimension_file(file_name):
  dimension_map = collections.defaultdict(list)
  with open(file_name, 'rb') as input_file:
    for line in input_file.readlines():
      if line.strip().startswith('#'):
         continue
      values = line.split(':')
      if len(values) != 2:
        print('Invalid dimension file format. Expected format is <dimension name>: val1,'
              ' val2, ... Found: ' + line)
        sys.exit(1)
      if not values[0] in KNOWN_DIMENSION_NAMES:
        print('Unknown dimension name: ' + values[0])
        print('Valid dimension names: ' + ', '.join(KNOWN_DIMENSION_NAMES))
        sys.exit(1)
      dimension_map[values[0]] = [val.strip() for val in values[1].split(',')]
  return dimension_map

def write_vectors_to_csv(output_dir, output_file, matrix):
  output_text = "# Generated File."
  for row in matrix:
    row = ['%s: %s' % (KNOWN_DIMENSION_NAMES[i], row[i]) for i in range(0, len(row))]
    output_text += '\n' + ', '.join(row)

  output_path = os.path.join(output_dir, output_file)
  print('Writing test vectors to: ' + output_path)
  with open(output_path, 'wb') as output_file:
    output_file.write(output_text)
    output_file.write('\n')

dimension_file = os.path.join(WORKLOAD_DIR, options.workload,
                              '%s_dimensions.csv' % options.workload)
if not os.path.isfile(dimension_file):
  print('Dimension file not found: ' + dimension_file)
  sys.exit(1)

print('Reading dimension file: ' + dimension_file)
vector_map = read_dimension_file(dimension_file)
vectors = []

# This ordering matters! We need to know the order to apply the proper constraints.
for dimension_name in KNOWN_DIMENSION_NAMES:
  vectors.append(vector_map[dimension_name])
vg = VectorGenerator(vectors)

output_dir = os.path.join(WORKLOAD_DIR, options.workload)
write_vectors_to_csv(output_dir, '%s_pairwise.csv' % options.workload,
                     vg.generate_pairwise_matrix(is_valid_combination))
write_vectors_to_csv(output_dir, '%s_exhaustive.csv' % options.workload,
                     vg.generate_exhaustive_matrix(is_valid_combination))
