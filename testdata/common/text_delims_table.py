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

# Functions for generating test files with specific length, and ended with all
# permutation (with replacement) of items in suffix_list. When run from the
# command line, will generate data files in the specified directory and a
# print a SQL load statement to incorporate into dataload SQL script generation.

from __future__ import absolute_import, division, print_function
from builtins import range
from shutil import rmtree
from optparse import OptionParser
from contextlib import contextmanager
from itertools import product
import os

parser = OptionParser()
parser.add_option("--table_dir", dest="table_dir", default=None)
parser.add_option("--only_newline", dest="only_newline", default=False, action="store_true")
parser.add_option("--file_len", dest="file_len", type="int")

def generate_testescape_files(table_location, only_newline, file_len):
  data = ''.join(["1234567890" for _ in range(1 + file_len // 10)])

  suffix_list = ["\\", ",", "a"]
  if only_newline:
    suffix_list.append("\n")
  else:
    suffix_list.append("\r\n")

  if os.path.exists(table_location):
    rmtree(table_location)

  os.mkdir(table_location)
  for count, p in enumerate(product(suffix_list, repeat=len(suffix_list))):
    ending = ''.join(p)
    content = data[:file_len - len(ending)] + ending
    with open(os.path.join(table_location, str(count)), 'w') as f:
      f.write(content)

if __name__ == "__main__":
  (options, args) = parser.parse_args()
  if not options.table_dir:
    parser.error("--table_dir option must be specified")

  # Generate data locally, and output the SQL load command for use in dataload
  generate_testescape_files(options.table_dir, options.only_newline, options.file_len)

  print ("LOAD DATA LOCAL INPATH '%s' "
         "OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name};"
         % options.table_dir)
