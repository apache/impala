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
# Used to extract minimum memory data for test_mem_usage_scaling.py from stress test
# runtime info json.
#
# Usage
# =====
# Run the stress test binary search on the 3 node minicluster:
#
#   ./tests/stress/concurrent_select.py --tpch-db=tpch_parquet \
#       --runtime-info-path=mem_usage_scaling_runtime_info.json --samples 3 \
#       --mem_limit_eq_threshold_percent=0.01 --mem_limit_eq_threshold_mb=5 \
#       --common-query-options="default_spillable_buffer_size=256k"
#
# Then run this script to extract minimum memory:
#
#   ./tests/stress/extract_min_mem.py mem_usage_scaling_runtime_info.json
#
from __future__ import absolute_import, division, print_function
import json
import sys

results = []
with open(sys.argv[1]) as f:
  data = json.load(f)
  for query_data in data['db_names']['tpch_parquet'].values():
    runtime_info = query_data['[]']
    # Build up list of query numbers and minimum memory.
    results.append((int(runtime_info['name'][1:]),
                   runtime_info['required_mem_mb_with_spilling']))

results.sort()
print(', '.join(["'Q{0}': {1}".format(num, mem) for num, mem in results]))
