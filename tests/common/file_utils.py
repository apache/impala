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

# This module contains utility functions for testing Parquet files,
# and other functions used for checking for strings in files and
# directories.

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

  # The table doesn't exist, so create the table's directory
  tbl_dir = get_fs_path('/test-warehouse/{0}.db/{1}'.format(unique_database, table_name))
  check_call(['hdfs', 'dfs', '-mkdir', '-p', tbl_dir])

  # Put the parquet file in the table's directory
  # Note: -d skips a staging copy
  check_call(['hdfs', 'dfs', '-put', '-f', '-d', local_file, tbl_dir])

  # Create the table
  hdfs_file = '{0}/{1}'.format(tbl_dir, filename)
  qualified_table_name = '{0}.{1}'.format(unique_database, table_name)
  impala_client.execute('create table {0} like parquet "{1}" stored as parquet'.format(
    qualified_table_name, hdfs_file))


def create_table_and_copy_files(impala_client, create_stmt, unique_database, table_name,
                                files):
  # Create the directory
  hdfs_dir = get_fs_path('/test-warehouse/{0}.db/{1}'.format(unique_database, table_name))
  check_call(['hdfs', 'dfs', '-mkdir', '-p', hdfs_dir])

  # Copy the files
  #  - build a list of source files
  #  - issue a single put to the hdfs_dir ( -d skips a staging copy)
  source_files = []
  for local_file in files:
    # Cut off leading '/' to make os.path.join() happy
    local_file = local_file if local_file[0] != '/' else local_file[1:]
    local_file = os.path.join(os.environ['IMPALA_HOME'], local_file)
    assert os.path.isfile(local_file)
    source_files.append(local_file)
  check_call(['hdfs', 'dfs', '-put', '-f', '-d'] + source_files + [hdfs_dir])

  # Create the table
  create_stmt = create_stmt.format(db=unique_database, tbl=table_name)
  impala_client.execute(create_stmt)


def grep_dir(dir, search):
  '''Recursively search for files that contain 'search' and return a list of matched
     lines grouped by file.
  '''
  matching_files = dict()
  for dir_name, _, file_names in os.walk(dir):
    for file_name in file_names:
      file_path = os.path.join(dir_name, file_name)
      if os.path.islink(file_path):
        continue
      with open(file_path) as file:
        matching_lines = grep_file(file, search)
        if matching_lines:
          matching_files[file_name] = matching_lines
  return matching_files


def grep_file(file, search):
  '''Return lines in 'file' that contain the 'search' term. 'file' must already be
     opened.
  '''
  matching_lines = list()
  for line in file:
    if search in line:
      matching_lines.append(line)
  return matching_lines


def assert_file_in_dir_contains(dir, search):
  '''Asserts that at least one file in the 'dir' contains the 'search' term.'''
  results = grep_dir(dir, search)
  assert results, "%s should have a file containing '%s' but no file was found" \
      % (dir, search)


def assert_no_files_in_dir_contain(dir, search):
  '''Asserts that no files in the 'dir' contains the 'search' term.'''
  results = grep_dir(dir, search)
  assert not results, \
      "%s should not have any file containing '%s' but a file was found" \
      % (dir, search)
