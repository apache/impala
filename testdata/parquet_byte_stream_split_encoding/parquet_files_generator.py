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

from __future__ import absolute_import

import os

import pyarrow as pa
import pyarrow.parquet as pq

test_file_dir = "testdata/parquet_byte_stream_split_encoding"

nums_to_encode = [1.45, 4.256, 6.3573, 4.235, 7.5198463, 10.57956, 100.68491,
    0.54987623514, 1.0]

floats = pa.array(nums_to_encode, type=pa.float32())
floats_table = pa.table([floats], names=["floats"])
pq.write_table(floats_table, os.path.join(test_file_dir,
    'floats_byte_stream_split.parquet'), use_dictionary=False,
    use_byte_stream_split=True)

doubles = pa.array(nums_to_encode, type=pa.float64())
doubles_table = pa.table([doubles], names=["doubles"])
pq.write_table(doubles_table, os.path.join(test_file_dir,
    'doubles_byte_stream_split.parquet'), use_dictionary=False,
    use_byte_stream_split=True)
