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

def assert_time_str(expected_str, actual_time_ns, msg, tolerance=0.005):
  """Asserts a pretty printed time string matches a specific number of nanoseconds."""

  total_nanoseconds = convert_to_nanos(expected_str)
  actual_time_ns = int(actual_time_ns)

  expected_min = total_nanoseconds - (total_nanoseconds * tolerance)
  expected_max = total_nanoseconds + (total_nanoseconds * tolerance)
  assert expected_min <= actual_time_ns <= expected_max, \
      "{0} -- expected: {1}, actual: {2}, calculated: {3}, tolerance: {4}" \
      .format(msg, expected_str, actual_time_ns, total_nanoseconds, tolerance)


def convert_to_nanos(time_str):
  """Convert a pretty printed time string into integer nanoseconds."""
  units = {'h': 3600 * 1e9, 'm': 60 * 1e9, 's': 1e9, 'ms': 1e6, 'us': 1e3, 'ns': 1}

  total_nanoseconds = 0
  current_number = ''
  current_unit = ''

  for char in time_str:
    if char.isdigit() or char == '.':
      if current_unit != '':
        if current_unit in units:
          total_nanoseconds += int(float(current_number) * units[current_unit])
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

  total_nanoseconds += int(float(current_number) * units[current_unit])

  return total_nanoseconds
