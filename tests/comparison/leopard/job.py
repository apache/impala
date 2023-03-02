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

from __future__ import absolute_import, division, print_function
from os.path import join as join_path
from tests.comparison.query_generator import QueryGenerator
from time import time
from tests.comparison.db_connection import (
    DbCursor,
    DbConnection,
    PostgresqlConnection,
    ImpalaConnection,
    POSTGRESQL,
    IMPALA)
from tests.comparison.leopard.controller import (
    PATH_TO_SCHEDULE,
    PATH_TO_REPORTS,
    PATH_TO_FINISHED_JOBS,
    NESTED_TYPES_MODE,
    DATABASE_NAME,
    POSTGRES_DATABASE_NAME)
from tests.comparison.discrepancy_searcher import QueryResultComparator
from tests.comparison.query_profile import DefaultProfile, ImpalaNestedTypesProfile
from threading import Thread
from tests.comparison.leopard.impala_docker_env import ImpalaDockerEnv

import logging
import os
import pickle
import sys

POSTGRES_USER_NAME = 'postgres'
NUM_UNEXPECTED_ERRORS_THRESHOLD = 200
LOG = logging.getLogger('Job')

class Job(object):
  '''Represents a Query Generator Job. One ImpalaDockerEnv is associated with it. Able to
  execute queries by either generaing them based on a provided query profile or by
  extracting queries from an existing report. A report is generated when it finishes
  running.
  '''

  def __init__(self,
      query_profile,
      job_id,
      run_name = 'default',
      time_limit_sec = 24 * 3600,
      git_command = None,
      parent_job = None):
    self.git_hash = ''
    self.job_id = job_id
    self.job_name = run_name
    self.parent_job = parent_job
    self.query_profile = query_profile or (
        ImpalaNestedTypesProfile() if NESTED_TYPES_MODE else DefaultProfile())
    self.ref_connection = None
    self.result_list = []
    self.start_time = time()
    self.stop_time = None
    self.target_stop_time = time() + time_limit_sec
    self.test_connection = None
    self.num_queries_executed = 0
    self.num_queries_returned_correct_data = 0
    self.flatten_dialect = 'POSTGRESQL' if NESTED_TYPES_MODE else None
    self.impala_env = ImpalaDockerEnv(git_command)

  def __getstate__(self):
    '''For pickling'''
    result = {}
    result['job_id'] = self.job_id
    result['job_name'] = self.job_name
    result['parent_job'] = self.parent_job
    result['result_list'] = self.result_list
    result['git_hash'] = self.git_hash
    result['start_time'] = self.start_time
    result['stop_time'] = self.stop_time
    result['num_queries_executed'] = self.num_queries_executed
    result['num_queries_returned_correct_data'] = self.num_queries_returned_correct_data
    return result

  def prepare(self):
    '''Prepares the environment and connects to Postgres and Impala running inside the
    Docker container.
    '''
    LOG.info('Starting Job Preparation')
    self.impala_env.prepare()
    LOG.info('Job Preparation Complete')

    self.ref_connection = PostgresqlConnection(
        user_name=POSTGRES_USER_NAME,
        password=None,
        host_name=self.impala_env.host,
        port=self.impala_env.postgres_port,
        db_name=POSTGRES_DATABASE_NAME)
    LOG.info('Created ref_connection')

    self.start_impala()

    self.git_hash = self.impala_env.get_git_hash()

  def get_stack(self):
    stack_trace = self.impala_env.get_stack()
    LOG.info('Stack Trace: {0}'.format(stack_trace))
    return stack_trace

  def start_impala(self):
    '''Starts impala and creates a connection to it. '''
    self.impala_env.start_impala()

    self.test_connection = ImpalaConnection(
        host_name=self.impala_env.host,
        port=self.impala_env.impala_port,
        user_name=None,
        db_name=DATABASE_NAME)

    self.test_connection.reconnect()
    self.query_result_comparator = QueryResultComparator(
        self.query_profile,
        self.ref_connection,
        self.test_connection,
        query_timeout_seconds=4*60,
        flatten_dialect='POSTGRESQL')
    LOG.info('Created query result comparator')
    LOG.info(str(self.query_result_comparator.__dict__))

  def is_impala_running(self):
    return self.impala_env.is_impala_running()

  def save_pickle(self):
    '''Saves self as pickle. This is normally done when the job finishes running. '''
    with open(join_path(PATH_TO_FINISHED_JOBS, self.job_id), 'w') as f:
      pickle.dump(self, f)
    LOG.info('Saved Completed Job Pickle')

  def queries_to_be_executed(self):
    '''Generator that outputs query models. They are either generated based on the query
    profile, or they are extracted from an existing report.
    '''
    if self.parent_job:
      # If parent job is specified, get the queries from the parent job report
      with open(join_path(PATH_TO_REPORTS, self.parent_job), 'r') as f:
        parent_report = pickle.load(f)
      for error_type in ['stack', 'row_counts', 'mismatch']:
        for query in parent_report.grouped_results[error_type]:
          yield query['model']
    else:
      # If parent job is not specified, generate queries with QueryGenerator
      num_unexpected_errors = 0
      while num_unexpected_errors < NUM_UNEXPECTED_ERRORS_THRESHOLD:
        query = None
        try:
          # TODO: Support DML statements. Possibly this be part of IMPALA-4600.
          self.query_generator = QueryGenerator(self.query_profile)
          query = self.query_generator.generate_statement(self.common_tables)
        except IndexError as e:
          # This is a query generator bug that happens extremely rarely
          LOG.info('Query Generator Choice Problem, {0}'.format(e))
          continue
        except Exception as e:
          LOG.info('Unexpected error in queries_to_be_executed, {0}'.format(e))
          num_unexpected_errors += 1
          if num_unexpected_errors > NUM_UNEXPECTED_ERRORS_THRESHOLD:
            LOG.error('Num Unexpected Errors above threshold')
            raise
          else:
            continue
        yield query

  def generate_report(self):
    '''Generate report and save it into the reports directory. '''
    from report import Report
    rep = Report(self.job_id)
    rep.save_pickle()

  def start(self):
    try:
      self.prepare()
      self.query_generator = QueryGenerator(self.query_profile)
      if NESTED_TYPES_MODE:
        self.common_tables = DbCursor.describe_common_tables(
            [self.test_connection.cursor()])
      else:
        self.common_tables = DbCursor.describe_common_tables(
            [self.test_connection.cursor(), self.ref_connection.cursor()])

      for query_model in self.queries_to_be_executed():
        LOG.info('About to execute query.')
        result_dict = self.run_query(query_model)
        LOG.info('Query Executed successfully.')
        self.num_queries_executed += 1
        if result_dict:
          self.result_list.append(result_dict)
        LOG.info('Time Left: {0}'.format(self.target_stop_time - time()))
        if time() > self.target_stop_time:
          break
      self.stop_time = time()
      self.save_pickle()
      self.generate_report()
      LOG.info('Generated Report')
    except:
      LOG.exception('Unexpected Exception in start')
      raise
    finally:
      self.impala_env.stop_docker()
      LOG.info('Docker Stopped')
      try:
        os.remove(join_path(PATH_TO_SCHEDULE, self.job_id))
        LOG.info('Schedule file removed')
      except OSError:
        LOG.info('Unable to remove schedule file.')

  def reproduce_crash(self, query_model):
    '''Check if the given query_model causes a crash. Returns the number of times the
    query had to be run to cause a crash.
    '''
    NUM_TRIES = 5
    self.start_impala()
    for try_num in range(1, NUM_TRIES + 1):
      self.query_result_comparator.compare_query_results(query_model)
      if not self.is_impala_running():
        return try_num

  def run_query(self, query_model):
    '''Runs a single query. '''
    if not self.is_impala_running():
      LOG.info('Impala is not running, starting Impala.')
      self.start_impala()

    def run_query_internal():
      self.comparison_result = self.query_result_comparator.compare_query_results(
          query_model)

    self.comparison_result = None
    internal_thread = Thread(
      target=run_query_internal,
      name='run_query_internal_{0}'.format(self.job_id))
    internal_thread.daemon = True
    internal_thread.start()
    internal_thread.join(timeout=600)
    if internal_thread.is_alive():
      LOG.info('run_query_internal is alive, restarting Impala Environment')
      self.impala_env.stop_docker()
      self.prepare()
      return None
    else:
      LOG.info('run_query_internal is dead as expected')

    comparison_result = self.comparison_result

    if comparison_result.query_timed_out:
      LOG.info('Query Timeout Exception')
      restart_impala = True
    else:
      restart_impala = False

    result_dict = {}

    if self.is_impala_running():
      if comparison_result.error:
        result_dict = self.comparison_result_analysis(comparison_result)
        result_dict['model'] = query_model
      elif comparison_result.query_resulted_in_data:
        self.num_queries_returned_correct_data += 1
    else:
      LOG.info('CRASH OCCURED')
      result_dict = self.comparison_result_analysis(comparison_result)
      result_dict['model'] = query_model
      result_dict['stack'] = self.get_stack()
      result_dict['num_tries_to_reproduce'] = self.reproduce_crash(query_model)

    if restart_impala:
      self.start_impala()

    return result_dict

  def comparison_result_analysis(self, comparison_result):
    '''Get useful information from the comparison_result. '''
    result_dict = {}
    result_dict['error'] = comparison_result.error
    result_dict['mismatch_col'] = comparison_result.mismatch_at_col_number
    result_dict['mismatch_ref_row'] = comparison_result.ref_row
    result_dict['mismatch_test_row'] = comparison_result.test_row
    result_dict['ref_row_count'] = comparison_result.ref_row_count
    result_dict['ref_sql'] = comparison_result.ref_sql
    result_dict['test_row_count'] = comparison_result.test_row_count
    result_dict['test_sql'] = comparison_result.test_sql
    return result_dict
