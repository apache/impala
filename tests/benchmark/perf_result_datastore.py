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

import logging
import uuid
from functools import wraps
from impala.dbapi import connect as impala_connect
from tests.util.calculation_util import get_random_id

LOG = logging.getLogger(__name__)

class PerfResultDataStore(object):
  '''Allows for interaction with the performance Impala database.

  In order to reduce the number of calls to the database we cache query_id, workload_id,
  file_type_id and run_info_id in _cache. If an id cannot be found in the cache, we make a
  request to the database. Objects not in the database will be automatically inserted.

  In order to avoid creating a new file per query, we create a staging table for execution
  results and query profiles. When all results have been inserted, we insert the data from
  the staging tables into ExecutionResults and RuntimeProfiles. This way, we create on the
  order of 1 parquet file per run.'''

  def __init__(self, host, port, database_name, use_secure_connection=False):
    if use_secure_connection:
      self._connection = impala_connect(host, port, use_ssl=True, auth_mechanism='GSSAPI')
    else:
      self._connection = impala_connect(host, port)
    self._database_name = database_name
    self._staging_exec_result_table = None
    self._staging_profile_table = None
    self._cache = {}

  def __enter__(self):
    self._staging_exec_result_table = 'ExecutionResultsStaging_' + get_random_id(5)
    self._staging_profile_table = 'RuntimeProfilesStaging_' + get_random_id(5)
    self._create_new_table_as('ExecutionResults', self._staging_exec_result_table)
    self._create_new_table_as('RuntimeProfiles', self._staging_profile_table)
    return self

  def __exit__(self, type, value, traceback):
    self._copy_to_table(self._staging_exec_result_table, 'ExecutionResults')
    self._copy_to_table(self._staging_profile_table, 'RuntimeProfiles')
    self._drop_table(self._staging_exec_result_table)
    self._drop_table(self._staging_profile_table)

  def insert_execution_result(self,
      query_name,
      query_string,
      workload_name,
      scale_factor,
      file_format,
      compression_codec,
      compression_type,
      num_clients,
      num_iterations,
      cluster_name,
      executor_name,
      exec_time,
      run_date,
      version,
      run_info,
      user_name,
      runtime_profile):

    LOG.debug('About to insert execution result.')

    query_id = self._get_query_id(query_name, query_string)
    workload_id = self._get_workload_id(workload_name, scale_factor)
    file_type_id = self._get_file_type_id(
        file_format, compression_codec, compression_type)
    run_info_id = self._get_run_info_id(run_info, user_name)

    result_id = self._insert_execution_result(
        run_info_id, query_id, workload_id, file_type_id,
        num_clients, num_iterations, cluster_name, executor_name,
        exec_time, run_date, version)

    self._insert_runtime_profile(result_id, runtime_profile)

  def _get_query_id(self, query_name, query_string):
    ''' Gets the query_id for the given query name and query text. If not found in the
    database, this information gets inserted into the database. Results are cached. '''
    if (query_name, query_string) not in self._cache:
      self._cache[(query_name, query_string)] = \
          self._get_query_id_remote(query_name, query_string) or \
          self._insert_query_info(query_name, query_string, '')
          # Empty string above represents notes. No notes will be inserted by default.
    return self._cache[(query_name, query_string)]

  def _get_workload_id(self, workload_name, scale_factor):
    ''' Gets the workload_id for the given workload / scale factor. If not found in the
    database, this information gets inserted into the database. Results are cached.'''
    if (workload_name, scale_factor) not in self._cache:
      self._cache[(workload_name, scale_factor)] = \
          self._get_workload_id_remote(workload_name, scale_factor) or \
          self._insert_workload_info(workload_name, scale_factor)
    return self._cache[(workload_name, scale_factor)]

  def _get_file_type_id(self, file_format, compression_codec, compression_type):
    ''' Gets the file_type_id for the fiven file_format / compression codec / compression
    type combination. If not found in the database, this information gets inserted into
    the database. Results are cached.'''
    if (file_format, compression_codec, compression_type) not in self._cache:
      self._cache[(file_format, compression_codec, compression_type)] = \
          self._get_file_type_id_remote(
              file_format, compression_codec, compression_type) or \
          self._insert_filetype_info(file_format, compression_codec, compression_type)
    return self._cache[(file_format, compression_codec, compression_type)]

  def _get_run_info_id(self, run_info, user):
    '''Gets the run_info id for the given run_info and user. Creates a new entry for each
    (run_info, user) pair. Results are cached.'''
    if (run_info, user) not in self._cache:
      self._cache[(run_info, user)] = self._insert_run_info(run_info, user)
    return self._cache[(run_info, user)]

  def _cursor_wrapper(function):
    ''' Handles the common initialize/close pattern for cursor objects '''
    @wraps(function)
    def wrapper(self, *args, **kwargs):
      cursor = self._connection.cursor()
      cursor.execute('use {0}'.format(self._database_name))
      result = function(self, *args, cursor=cursor)
      cursor.close()
      return result
    return wrapper

  @_cursor_wrapper
  def _get_query_id_remote(self, query_name, query_string, cursor):
    cursor.execute('''
        select query_id
        from Queries where name="{0}"
        and query_string="{1}"'''.format(query_name, query_string))
    query_id = cursor.fetchone()
    return query_id[0] if query_id else None

  @_cursor_wrapper
  def _get_workload_id_remote(self, workload, scale_factor, cursor):
    cursor.execute('''
        select workload_id
        from Workloads where name="{0}"
        and scale_factor="{1}"'''.format(workload, scale_factor))
    workload_id = cursor.fetchone()
    return workload_id[0] if workload_id else None

  @_cursor_wrapper
  def _get_file_type_id_remote(self,
      file_format,
      compression_codec,
      compression_type,
      cursor):
    cursor.execute('''
        select file_type_id
        from FileTypes where file_format="{0}"
        and compression_codec="{1}" and compression_type="{2}"
        '''.format(file_format,compression_codec, compression_type))
    file_type_id = cursor.fetchone()
    return file_type_id[0] if file_type_id else None

  @_cursor_wrapper
  def _get_run_info_id_remote(self, run_info, user, cursor):
    cursor.execute('''
        select run_info_id
        from RunInfos where run_info="{0}" and user="{1}"'''.format(run_info, user))
    run_info_id = cursor.fetchone()
    return run_info_id[0] if run_info_id else None

  @_cursor_wrapper
  def _create_new_table_as(self, old_table, new_table, cursor):
    cursor.execute('create table {0} like {1}'.format(new_table, old_table))

  @_cursor_wrapper
  def _copy_to_table(self, source_table, destination_table, cursor):
    cursor.execute('insert into table {0} select * from {1}'.format(
        destination_table, source_table))

  @_cursor_wrapper
  def _drop_table(self, table, cursor):
    cursor.execute('drop table {0}'.format(table))

  @_cursor_wrapper
  def _insert_execution_result(self,
      run_info_id, query_id, workload_id, file_type_id,
      num_clients, num_iterations, cluster_name, executor_name,
      exec_time, run_date, version, cursor):
    result_id = str(uuid.uuid1())
    insert_query = '''insert into {0} (result_id,
        run_info_id, query_id, workload_id, file_type_id,
        num_clients, num_iterations, cluster_name, executor_name,
        exec_time, run_date, version)
        values("{1}",
        "{2}", "{3}", "{4}", "{5}",
        {6}, {7}, "{8}", "{9}",
        {10}, cast("{11}" as timestamp), "{12}")
        '''.format(self._staging_exec_result_table, result_id,
        run_info_id, query_id, workload_id, file_type_id,
        num_clients, num_iterations, cluster_name, executor_name,
        exec_time, run_date, version)
    cursor.execute(insert_query)
    return result_id

  @_cursor_wrapper
  def _insert_query_info(self, name, query_string, notes, cursor):
    LOG.debug('Inserting Query: {0}'.format(name))
    query_id = str(uuid.uuid1())
    insert_statement = '''
        insert into Queries (query_id, name, query_string, notes)
        values("{0}", "{1}", "{2}", "{3}")'''.format(
        query_id, name, query_string, notes)
    cursor.execute(insert_statement)
    return query_id

  @_cursor_wrapper
  def _insert_workload_info(self, name, scale_factor, cursor):
    LOG.debug('Inserting Workload: {0}, {1}'.format(name, scale_factor))
    workload_id = str(uuid.uuid1())
    cursor.execute('''
        insert into Workloads (workload_id, name, scale_factor)
        values("{0}", "{1}", "{2}")'''.format(workload_id, name, scale_factor))
    return workload_id

  @_cursor_wrapper
  def _insert_filetype_info(
      self, file_format, compression_codec, compression_type, cursor):
    LOG.debug('Inserting File Type: {0}, {1}, {2}'.format(
        file_format, compression_codec, compression_type))
    file_type_id = str(uuid.uuid1())
    cursor.execute('''
        insert into FileTypes
        (file_type_id, file_format, compression_codec, compression_type)
        values("{0}", "{1}", "{2}", "{3}")'''.format(
        file_type_id, file_format, compression_codec, compression_type))
    return file_type_id

  @_cursor_wrapper
  def _insert_run_info(self, run_info, user, cursor):
    LOG.debug('Inserting Run Info: {0}, User: {1}'.format(run_info, user))
    run_info_id = str(uuid.uuid1())
    cursor.execute('''
        insert into RunInfos (run_info_id, run_info, user)
        values ("{0}", "{1}", "{2}")'''.format(run_info_id, run_info, user))
    return run_info_id

  @_cursor_wrapper
  def _insert_runtime_profile(self, result_id, profile, cursor):
    LOG.debug('Inserting Runtime Profile')
    cursor.execute('''
        insert into {0} (result_id, profile)
        values ("{1}", "{2}")'''.format(self._staging_profile_table, result_id, profile))
