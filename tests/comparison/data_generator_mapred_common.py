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

'''This module provides random data generation and database population.

   When this module is run directly for purposes of database population, the default is
   to use a fixed seed for randomization. The result should be that the generated random
   data is the same regardless of when or where the execution is done.

'''

from __future__ import absolute_import, division, print_function
from base import range
import base64
import pickle
from io import BytesIO

from tests.comparison.db_types import Decimal
from tests.comparison.random_val_generator import RandomValGenerator

def serialize(value):
  '''Returns a serialized representation of 'value' suitable for use as a key in an MR
     streaming job.
  '''
  return base64.b64encode(pickle.dumps(value))


def deserialize(value):
  return pickle.loads(base64.b64decode(value))


class TextTableDataGenerator(object):

  def __init__(self):
    self.table = None
    self.randomization_seed = None
    self.row_count = None
    self.output_file = None

  def populate_output_file(self):
    cols = self.table.cols
    col_val_generators = [self._create_val_generator(c.exact_type) for c in cols]
    val_buffer_size = 1024
    col_val_buffers = [[None] * val_buffer_size for c in cols]
    for row_idx in range(self.row_count):
      val_buffer_idx = row_idx % val_buffer_size
      if val_buffer_idx == 0:
        for col_idx, col in enumerate(cols):
          val_buffer = col_val_buffers[col_idx]
          val_generator = col_val_generators[col_idx]
          for idx in range(val_buffer_size):
            val = next(val_generator)
            val_buffer[idx] = r"\N" if val is None else val
      for col_idx, col in enumerate(cols):
        if col_idx > 0:
          # Postgres doesn't seem to have an option to specify that the last column value
          # has a terminator. Impala and Hive accept this format with the option
          # 'ROW FORMAT DELIMITED'.
          self.output_file.write(b"\x01")
        self.output_file.write(str(col_val_buffers[col_idx][val_buffer_idx]))
      self.output_file.write("\n")

  def _create_val_generator(self, val_type):
    val_generator = RandomValGenerator().create_val_generator(val_type)
    if isinstance(val_type, Decimal):
      fmt = '%%0.%sf' % val_type.MAX_FRACTIONAL_DIGITS
      def val():
        while True:
          val = next(val_generator)
          yield None if val is None else fmt % val
      return val
    return val_generator


# MR jobs are hard-coded to try to have each reducer generate this much data.
MB_PER_REDUCER = 120


def estimate_bytes_per_row(table_data_generator, row_count):
  original_row_count = table_data_generator.row_count
  original_output_file = table_data_generator.output_file
  table_data_generator.row_count = row_count
  table_data_generator.output_file = BytesIO()
  table_data_generator.populate_output_file()
  table_data_generator.output_file.flush()
  bytes_per_row = len(table_data_generator.output_file.getvalue()) / float(row_count)
  table_data_generator.output_file.close()
  table_data_generator.output_file = original_output_file
  table_data_generator.row_count = original_row_count
  return max(int(bytes_per_row), 1)


def estimate_rows_per_reducer(table_data_generator, mb_per_reducer):
  bytes_per_reducer = mb_per_reducer * 1024 ** 2
  bytes_per_row = estimate_bytes_per_row(table_data_generator, 1)
  if bytes_per_row >= bytes_per_reducer:
    return 1
  rows_per_reducer = bytes_per_reducer // bytes_per_row
  bytes_per_row = estimate_bytes_per_row(table_data_generator,
      max(int(rows_per_reducer * 0.001), 1))
  return max(bytes_per_reducer // bytes_per_row, 1)
