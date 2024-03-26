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

def assert_time_str(expected_str, actual_time_ms, msg, tolerance=0.005):
  """Asserts a pretty printed time string matches a specific number of milliseconds."""

  total_ms = convert_to_milliseconds(expected_str)
  actual_time_ms = float(actual_time_ms)

  expected_min = total_ms - (total_ms * tolerance)
  expected_max = total_ms + (total_ms * tolerance)
  assert expected_min <= actual_time_ms <= expected_max, \
      "{0} -- expected: {1}, actual: {2}, calculated: {3}, tolerance: {4}" \
      .format(msg, expected_str, actual_time_ms, total_ms, tolerance)


def convert_to_milliseconds(time_str):
  """Convert a pretty printed time string into a float with up to three digits for the
     decimal places."""
  units = {'h': 3600000, 'm': 60000, 's': 1000, 'ms': 1, 'us': 1e-3, 'ns': 1e-6}

  total_ms = 0.0
  current_number = ''
  current_unit = ''

  for char in time_str:
    if char.isdigit() or char == '.':
      if current_unit != '':
        if current_unit in units:
          total_ms += float(current_number) * units[current_unit]
          current_number = ''
          current_unit = ''
        else:
          raise ValueError("Invalid alphabetic unit '{0}' in time string"
                           .format(current_unit))
      current_number += char
    elif char.isalpha():
      current_unit += char
    else:
      raise ValueError("Invalid character in time string")

  total_ms += float(current_number) * units[current_unit]

  # The differences between round in Python 2 and Python 3 do not matter here.
  # pylint: disable=round-builtin
  return round(total_ms, 3)
