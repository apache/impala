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

import os
import struct

from parquet.ttypes import FileMetaData
from thrift.protocol import TCompactProtocol
from thrift.transport import TTransport

PARQUET_VERSION_NUMBER = 'PAR1'

def parse_int(s):
  """Reinterprets the string 's' as a 4-byte integer."""
  return struct.unpack('i', s)[0]

def get_parquet_metadata(filename):
  """Returns a FileMetaData as defined in parquet.thrift. 'filename' must be a local
  file path."""
  file_size = os.path.getsize(filename)
  with open(filename) as f:
    # Check file starts and ends with magic bytes
    start_magic = f.read(len(PARQUET_VERSION_NUMBER))
    assert start_magic == PARQUET_VERSION_NUMBER

    f.seek(file_size - len(PARQUET_VERSION_NUMBER))
    end_magic = f.read(len(PARQUET_VERSION_NUMBER))
    assert end_magic == PARQUET_VERSION_NUMBER

    # Read metadata length
    f.seek(file_size - len(PARQUET_VERSION_NUMBER) - 4)
    metadata_len = parse_int(f.read(4))

    # Read metadata
    f.seek(file_size - len(PARQUET_VERSION_NUMBER) - 4 - metadata_len)
    serialized_metadata = f.read(metadata_len)

    # Deserialize metadata
    transport = TTransport.TMemoryBuffer(serialized_metadata)
    protocol = TCompactProtocol.TCompactProtocol(transport)
    metadata = FileMetaData()
    metadata.read(protocol)
    return metadata
