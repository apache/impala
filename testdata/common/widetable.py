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

# Functions for creating wide (i.e. many-column) tables. When run from the command line,
# specify either --get_columns to generate column descriptors, or --create_data to
# generate a CSV data file and prints a SQL load statement to incorporate
# into dataload SQL script generation.

from __future__ import absolute_import, division, print_function
from builtins import range
from datetime import datetime, timedelta
import itertools
import optparse

parser = optparse.OptionParser()
parser.add_option("--get_columns", dest="get_columns", default=False, action="store_true")
parser.add_option("--create_data", dest="create_data", default=False, action="store_true")
parser.add_option("-n", "--num_columns", dest="num_columns", type="int")
parser.add_option("-o", "--output_file", dest="output_file")
parser.add_option("--num_rows", dest="num_rows", default=10)

def get_columns(num_cols):
  """Returns 'num_cols' column declarations, cycling through every column type, as a
  a list of strings."""
  templates = [
    'bool_col%i BOOLEAN',
    'tinyint_col%i TINYINT',
    'smallint_col%i SMALLINT',
    'int_col%i INT',
    'bigint_col%i BIGINT',
    'float_col%i FLOAT',
    'double_col%i DOUBLE',
    'string_col%i STRING',
    ]

  iter = itertools.cycle(templates)
  # Produces [bool_col1, tinyint_col1, ..., bool_col2, tinyint_col2, ...]
  # The final list has 'num_cols' elements.
  return [next(iter) % (i // len(templates) + 1) for i in range(num_cols)]

# Data generators for different types. Each generator yields an infinite number of
# value strings suitable for writing to a CSV file.

def bool_generator():
  """Generates True, False repeating"""
  b = True
  while True:
    yield str(b)
    b = not b

def integer_generator():
  """Generates 0..4 repeating"""
  i = 0
  while True:
    yield str(i % 5)
    i += 1

def floating_point_generator():
  """Generates 0, 1.1, ..., 4.4 repeating"""
  i = 0
  while True:
    yield str((i % 5) * 1.1)
    i += 1

def quote(iter_fn):
  """Returns a generator that returns quoted values of iter_fn."""
  def new_iter_fn():
    iter = iter_fn()
    while True:
      yield "'%s'" % next(iter)
  return new_iter_fn

def get_data(num_cols, num_rows, delimiter=',', quote_strings=False):
  """Returns the data for the given number of rows and columns as a list of strings, each
  of which is a row delimited by 'delimiter'."""
  generators = [
    bool_generator, # boolean
    integer_generator, # tinyint
    integer_generator, # smallint
    integer_generator, # int
    integer_generator, # bigint
    floating_point_generator, # float
    floating_point_generator, # double
    quote(integer_generator) if quote_strings else integer_generator, # string
    ]
  # Create a generator instance for each column, cycling through the different types
  iter = itertools.cycle(generators)
  column_generators = [next(iter)() for i in range(num_cols)]

  # Populate each row using column_generators
  rows = []
  for i in range(num_rows):
    vals = [next(gen) for gen in column_generators]
    rows.append(delimiter.join(vals))
  return rows

if __name__ == "__main__":
  (options, args) = parser.parse_args()

  if options.get_columns == options.create_data:
    parser.error("Must specify either --get_columns or --create_data")

  if not options.num_columns:
    parser.error("--num_columns option must be specified")

  if options.get_columns:
    # Output column descriptors
    print('\n'.join(get_columns(options.num_columns)))

  if options.create_data:
    # Generate data locally, and output the SQL load command for use in dataload
    if not options.output_file:
      parser.error("--output_file option must be specified")

    with open(options.output_file, "w") as f:
      for row in get_data(options.num_columns, options.num_rows):
        f.write(row)
        f.write('\n')

    print ("LOAD DATA LOCAL INPATH '%s' "
           "OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name};"
           % options.output_file)
