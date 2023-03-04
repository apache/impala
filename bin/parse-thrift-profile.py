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
#
# Parses a base64-encoded profile provided via stdin. It accepts
# three common formats:
#
# 1. Impala profile logs of the format
#    "<ts> <queryid> <base64encoded, compressed thrift profile>"
# 2. Just the base64-encoded compressed thrift profile
# 3. Base-64 encoded uncompressed thrift profile.
#
# In all cases, the script expects one profile per line.
#
# For example:
#
# $ cat logs/cluster_test/custom_cluster_tests/profiles/impala_profile_log \
#      | head -n 1 | awk '{ print $3 }' | parse-profile.py
# TRuntimeProfileTree(nodes=[TRuntimeProfileNode(info_strings_display_order=....
#
# or
#
# $ bin/parse-thrift-profile.py logs/custom_cluster_tests/profiles/impala_profile_log_1.1-1523657191158
# 2018-04-13T15:06:34.144000 e44af7f93edb8cd6:1b1f801600000000 TRuntimeProfileTree(nodes=[TRuntimeProf...


from __future__ import absolute_import, division, print_function
from impala_py_lib import profiles
import sys

if len(sys.argv) == 1 or sys.argv[1] == "-":
  input_data = sys.stdin
elif len(sys.argv) == 2:
  input_data = open(sys.argv[1])
else:
  print("Usage: %s [file]" % (sys.argv[0],), file=sys.stderr)
  sys.exit(1)

for line in input_data:
  tree = profiles.decode_profile_line(line)
  print(tree)
