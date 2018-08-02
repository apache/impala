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

# This module contains utility functions for testing Parquet files

import os
from subprocess import check_call

from tests.util.filesystem_utils import get_fs_path


def create_table_from_parquet(impala_client, unique_database, table_name):
  """Utility function to create a database table from a Parquet file. A Parquet file must
  exist in $IMPALA_HOME/testdata/data with the name 'table_name'.parquet"""
  filename = '{0}.parquet'.format(table_name)
  local_file = os.path.join(os.environ['IMPALA_HOME'],
                            'testdata/data/{0}'.format(filename))
  assert os.path.isfile(local_file)
  hdfs_file = get_fs_path('/test-warehouse/{0}.db/{1}'.format(unique_database, filename))
  check_call(['hdfs', 'dfs', '-copyFromLocal', '-f', local_file, hdfs_file])

  qualified_table_name = '{0}.{1}'.format(unique_database, table_name)
  impala_client.execute('create table {0} like parquet "{1}" stored as parquet'.format(
    qualified_table_name, hdfs_file))
  impala_client.execute('load data inpath "{0}" into table {1}'.format(
    hdfs_file, qualified_table_name))


def create_table_and_copy_files(impala_client, create_stmt, unique_database, table_name,
                                files):
  create_stmt = create_stmt.format(db=unique_database, tbl=table_name)
  impala_client.execute(create_stmt)
  for local_file in files:
    # Cut off leading '/' to make os.path.join() happy
    local_file = local_file if local_file[0] != '/' else local_file[1:]
    local_file = os.path.join(os.environ['IMPALA_HOME'], local_file)
    assert os.path.isfile(local_file)
    basename = os.path.basename(local_file)
    hdfs_file = get_fs_path('/test-warehouse/{0}.db/{1}'.format(unique_database,
                                                                basename))
    check_call(['hdfs', 'dfs', '-copyFromLocal', '-f', local_file, hdfs_file])
    qualified_table_name = '{0}.{1}'.format(unique_database, table_name)
    impala_client.execute('load data inpath "{0}" into table {1}'.format(
      hdfs_file, qualified_table_name))
