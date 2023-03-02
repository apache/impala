#!/usr/bin/env impala-python
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

# This file contains library functions to decode and access Impala query profiles.

from __future__ import absolute_import, division, print_function
import base64
import datetime
import zlib
from thrift.protocol import TCompactProtocol
from thrift.TSerialization import deserialize
from RuntimeProfile.ttypes import TRuntimeProfileTree


def decode_profile_line(line):
  space_separated = line.split(" ")
  if len(space_separated) == 3:
    ts = int(space_separated[0])
    print(datetime.datetime.fromtimestamp(ts / 1000.0).isoformat(), space_separated[1])
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

  return tree
