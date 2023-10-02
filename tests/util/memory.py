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

from __future__ import absolute_import, division, print_function

from re import match


def assert_byte_str(expected_str, actual_bytes, msg, unit_combined=False,
    tolerance=0.005):
  """Asserts a pretty printed memory string matches a specifiec number of bytes."""
  calc = convert_to_bytes(expected_str, unit_combined)

  # Allow a tolerance of +- half a percent to allow for conversion differences between
  # Impala and the Python tests.
  expected_min = calc - (calc * tolerance)
  expected_max = calc + (calc * tolerance)

  assert expected_min <= int(actual_bytes) <= expected_max, "{0} -- expected: {1}, " \
      "actual: {2}, calculated: {3}, tolerance: {4}".format(msg, expected_str,
      actual_bytes, calc, tolerance)


def convert_to_bytes(mem_str, unit_combined=False):
  """Converts a pretty printed memory string into bytes. Since pretty printing removes
     precision, the result may not equal the original number of bytes. Returns an int.

     By default, the format of mem_str is to have a space between the number and the
     units, but setting the unit_combined causes this function to use the last two
     characters of mem_str as the units and all other characters as the number."""
  unit = ""
  calc = 0
  multiplier = 1024
  unit_length = -2

  # Memory can have a unit of 'GB', 'MB', 'KB', or 'B'. If this regular expression
  # matches the provided memory string, then a unit of 'B' was used.
  if match(r'\d+\.?\d*B$', mem_str):
    unit_length = -1

  if unit_combined:
    unit = mem_str[unit_length:]
    calc = float(mem_str[:unit_length])
  else:
    split_str = mem_str.split(' ')
    unit = split_str[1]
    calc = float(split_str[0])

  if unit == 'B' or unit == '':
    calc *= 1
  elif unit == 'KB':
    calc *= multiplier
  elif unit == 'MB':
    calc *= multiplier**2
  elif unit == 'GB':
    calc *= multiplier**3
  else:
    raise ValueError("Invalid unit for byte string: {}".format(mem_str))

  return int(calc)
