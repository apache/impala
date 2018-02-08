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


from thrift.protocol import TCompactProtocol
from thrift.TSerialization import deserialize
from RuntimeProfile.ttypes import TRuntimeProfileTree

import base64
import datetime
import sys
import zlib

if len(sys.argv) == 1 or sys.argv[1] == "-":
  input_data = sys.stdin
elif len(sys.argv) == 2:
  input_data = file(sys.argv[1])
else:
  print >> sys.stderr, "Usage: %s [file]" % (sys.argv[0],)
  sys.exit(1)

for line in input_data:
  space_separated = line.split(" ")
  if len(space_separated) == 3:
    ts = int(space_separated[0])
    print datetime.datetime.fromtimestamp(ts/1000.0).isoformat(), space_separated[1],
    base64_encoded = space_separated[2]
  elif len(space_separated) == 1:
    base64_encoded = space_separated[0]
  else:
    raise Exception("Unexpected line: " + line)
  possibly_compressed = base64.b64decode(base64_encoded)
  # Handle both compressed and uncompressed Thrift profiles
  try:
    thrift = zlib.decompress(possibly_compressed)
  except zlib.error:
    thrift = possibly_compressed

  tree = TRuntimeProfileTree()
  deserialize(tree, thrift, protocol_factory=TCompactProtocol.TCompactProtocolFactory())
  tree.validate()
  print tree
