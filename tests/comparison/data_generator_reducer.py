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

# The line below is interpreted as an invalid command in bash and a string literal in
# python.
'''' &>/dev/null
set -e
# pypy is preferred since it's about 10x faster than cpython.
if which pypy &>/dev/null; then
  exec pypy $0
else
  exec python $0
fi
'''

'''This is a reducer for use with hadoop streaming. See data_generator.DatabasePopulator
   for more information on how this file is used.
'''

import os
import random
import subprocess
import sys

# When running locally, the PYTHONPATH needed by impala-shell interferes with python
# through YARN. Specifically, when the data generator needs to import common.py, python
# looks at $IMPALA_HOME/tests/common and errors when it doesn't find what was asked for.
sys.path.insert(1, os.getcwd())

from data_generator_mapred_common import deserialize

for line in sys.stdin:
  _, batch_idx, serialized_table_data_generator = line.split("\t")
  table_data_generator = deserialize(serialized_table_data_generator)
  random.seed(table_data_generator.randomization_seed)
  output_file_name = "batch_%s.data" % batch_idx
  with open(output_file_name, "w") as output_file:
    table_data_generator.output_file = output_file
    table_data_generator.populate_output_file()
  put = subprocess.Popen(["hadoop", "fs", "-put", output_file.name,
      table_data_generator.table.storage_location],
      stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
  put.wait()
  if put.returncode != 0:
    raise Exception("Error uploading data to hdfs: %s" % put.communicate()[0])
  os.remove(output_file.name)
