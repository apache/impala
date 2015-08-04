#!/usr/bin/env impala-python
# Copyright (c) 2014 Cloudera, Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from impala.dbapi import connect as impala_connect
from optparse import OptionParser

parser = OptionParser()
parser.add_option('--host')
parser.add_option('--port', default=21050)
parser.add_option('--db_name', default='impala_perf_results')

options, args = parser.parse_args()

CREATE_STATEMENTS = ['''
     CREATE EXTERNAL TABLE IF NOT EXISTS ExecutionResults (
     result_id string,
     run_info_id string,
     query_id string,
     workload_id string,
     file_type_id string,
     num_clients INT,
     num_iterations INT,
     cluster_name string,
     executor_name string,
     exec_time double,
     run_date timestamp,
     version string
) stored as parquet;
''',
'''
CREATE EXTERNAL TABLE IF NOT EXISTS Queries (
     query_id string,
     name string,
     query_string string,
     notes string
) stored as parquet;
''',
'''
CREATE EXTERNAL TABLE IF NOT EXISTS Workloads (
     workload_id string,
     name string,
     scale_factor string
) stored as parquet;
''',
'''
CREATE EXTERNAL TABLE IF NOT EXISTS FileTypes (
     file_type_id string,
     file_format string,
     compression_codec string,
     compression_type string
) stored as parquet;
''',
'''
CREATE EXTERNAL TABLE IF NOT EXISTS RunInfos (
     run_info_id string,
     run_info string,
     user string
) stored as parquet;
''',
'''
CREATE EXTERNAL TABLE IF NOT EXISTS RuntimeProfiles (
     result_id string,
     profile string
) stored as parquet;
''']

def create_database():
  connection = impala_connect(host=options.host, port=int(options.port))
  cursor = connection.cursor()

  cursor.execute('CREATE DATABASE IF NOT EXISTS {0}'.format(options.db_name))
  cursor.execute('USE {0}'.format(options.db_name))
  for create_statement in CREATE_STATEMENTS:
    cursor.execute(create_statement)

if __name__ == '__main__':
  create_database()
