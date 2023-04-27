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

# This tool walks the build directory (release by default) and will print the text, data,
# and bss section sizes of the archives.
from __future__ import absolute_import, division, print_function
from builtins import range
import fnmatch
import os
import re
import subprocess
from prettytable import PrettyTable

def get_bin_size_data(file):
  data = ""
  try:
    data = subprocess.check_output(["size", "-B", "-t", file],
        stderr=subprocess.STDOUT, universal_newlines=True)
  except Exception as e:
    data = e.output

  res = re.split(r'\s+', data.split("\n")[-2])
  if len(res[0].strip()) == 0:
    return res[1:-3]
  else:
    return res[:-3]

def find_files(build_type="release"):
  root_path = os.path.join(os.getenv("IMPALA_HOME"), "be", "build", build_type)
  matches = []
  for root, dirs, files in os.walk(root_path):
    for filename in fnmatch.filter(files, '*.a'):
      matches.append(os.path.join(root, filename))

  tab = PrettyTable(["file", "text", "data", "bss"])
  sums = ["Total", 0, 0, 0]
  for m in matches:
    row = [os.path.basename(m)] + get_bin_size_data(m);
    tab.add_row(row)
    for x in range(1, 4):
      sums[x] += int(row[x])
  tab.add_row(sums)
  print(tab)

if __name__ == "__main__":
  find_files()
