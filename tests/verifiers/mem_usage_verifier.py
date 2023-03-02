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
#
# Verifier for memtracker usage values (Total, Peak, etc).

from __future__ import absolute_import, division, print_function
import re

SIZE_FACTORS = {"b": 1, "kb": 1 << 10, "mb": 1 << 20, "gb": 1 << 30}

def parse_mem_value(value):
  """Parses a memory value with an optional unit like "123", "10 B", or "1.5 KB" into an
  number of bytes.
  """
  elements = value.split()
  result = float(elements[0])
  if len(elements) > 1:
    result *= SIZE_FACTORS[elements[1].lower()]
  return result

class MemUsageVerifier(object):
  """MemUsageVerifier objects can be used to verify values in the debug output of memory
  trackers.
  """

  def __init__(self, impalad_service):
    """Initialize module given an ImpalaService object"""
    self.impalad_service = impalad_service

  def get_mem_usage_values(self, name):
    """Returns a dictionary of all key=value pairs of the memtracker specified by 'name'
    by reading the '/memz' debug webpage. It also parses and converts memory values
    including optional units like "10 B" or "1.5 KB". All strings are converted to
    lowercase. Only the first line starting with 'name' is considered.

    For example, for the line "Data Stream Service: Total=0 Peak=108.00 B" this will
    return "dict(total=0, peak=108.0)".
    """
    memz = self.impalad_service.get_debug_webpage_json("memz")
    details = memz.get("detailed", "")
    for line in details.splitlines():
      line = line.strip()
      prefix = name + ":"
      if line.startswith(prefix):
        line = line[len(prefix):]
        result = {}
        # The value regex matches either '0' or any number including a decimal dot,
        # followed by a required unit.
        for k, v in re.findall(r"(\S+)=(0|[\d\.]+ [KMG]?B)", line):
          result[k.lower()] = parse_mem_value(v)
        return result
    return {}





