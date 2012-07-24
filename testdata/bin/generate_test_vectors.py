#!/usr/bin/env python
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
#
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
# In the benchmark case the vector files are used by generate_benchmark_statements.rb to
# dynamically build the schema and data for running benchmarks.
#
# We use the Python 'AllPairs' module which can be download from:
# http://pypi.python.org/pypi/AllPairs/2.0.1
#
import csv
import math
import os
import random
from itertools import product
from optparse import OptionParser
import metacomm.combinatorics.all_pairs2
all_pairs = metacomm.combinatorics.all_pairs2.all_pairs2

parser = OptionParser()
parser.add_option("--dimension_file", dest="dimension_file",
                  default = "benchmark_dimensions.csv",
                  help="The file containing the list of dimensions.")
parser.add_option("--base_output_file_name", dest="base_output_file_name",
                  default = "benchmark",
                  help="The base file name for test vector output")
(options, args) = parser.parse_args()

FILE_FORMAT_IDX = 0
DATA_SET_IDX = 1
COMPRESSION_IDX = 2
COMPRESSION_TYPE_IDX = 3

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

def is_valid_combination(vector):
  if len(vector) == 4:
    return not (
         (vector[FILE_FORMAT_IDX] == 'text' and vector[COMPRESSION_IDX] != 'none') or
         (vector[COMPRESSION_IDX] == 'none' and vector[COMPRESSION_TYPE_IDX] != 'none') or
         (vector[COMPRESSION_IDX] != 'none' and vector[COMPRESSION_TYPE_IDX] == 'none') or
         (vector[FILE_FORMAT_IDX] != 'seq' and vector[COMPRESSION_TYPE_IDX] == 'record') or
         (vector[DATA_SET_IDX] == 'tpch' and vector[FILE_FORMAT_IDX] != 'text'))
  return True

def read_csv_vector_file(file_name):
  results = []
  for row in csv.reader(open(file_name, 'rb'), delimiter=','):
    results.append(row)
  return results

def write_vectors_to_csv(output_csv_file, matrix):
  csv_writer = csv.writer(open(output_csv_file, 'wb'),
                          delimiter=',',
                          quoting=csv.QUOTE_MINIMAL)
  for vector in matrix:
    csv_writer.writerow(vector)

vectors = read_csv_vector_file(options.dimension_file)
vg = VectorGenerator(vectors)
write_vectors_to_csv('%s_pairwise.csv' % options.base_output_file_name,
                     vg.generate_pairwise_matrix(is_valid_combination))
write_vectors_to_csv('%s_exhaustive.csv' % options.base_output_file_name,
                     vg.generate_exhaustive_matrix(is_valid_combination))

