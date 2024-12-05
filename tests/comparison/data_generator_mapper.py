#!/usr/bin/env ambari-python-wrap
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

'''This is a mapper for use with hadoop streaming. See data_generator.DatabasePopulator
   for more information on how this file is used.
'''
from __future__ import absolute_import, division, print_function

import os
import random
import sys

# When running locally, the PYTHONPATH needed by impala-shell interferes with python
# through YARN. Specifically, when the data generator needs to import common.py, python
# looks at $IMPALA_HOME/tests/common and errors when it doesn't find what was asked for.
sys.path.insert(1, os.getcwd())

from data_generator_mapred_common import (
    deserialize,
    estimate_rows_per_reducer,
    MB_PER_REDUCER,
    serialize)

for line in sys.stdin:
  table_data_generator = deserialize(line)
  random.seed(table_data_generator.randomization_seed)
  row_count = table_data_generator.row_count
  rows_per_batch = estimate_rows_per_reducer(table_data_generator, MB_PER_REDUCER)
  batch_idx = 0
  while row_count > 0:
    table_data_generator.row_count = min(row_count, rows_per_batch)
    table_data_generator.randomization_seed = int(random.random() * sys.maxsize)

    # Generate input for the reducers.
    print("%s\t%s\t%s" % (table_data_generator.table.name, batch_idx,
        serialize(table_data_generator)))

    batch_idx += 1
    row_count -= rows_per_batch
