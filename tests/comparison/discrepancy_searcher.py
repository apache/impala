#!/usr/bin/env python

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

'''This module will run random queries against existing databases and compare the
   results.

'''

from contextlib import closing
from decimal import Decimal
from itertools import izip, izip_longest
from logging import basicConfig, getLogger
from math import isinf, isnan
from os import getenv, remove
from os.path import exists, join
from shelve import open as open_shelve
from subprocess import call
from threading import current_thread, Thread
from tempfile import gettempdir
from time import time

from tests.comparison.db_connector import (
    DbConnection,
    DbConnector,
    IMPALA,
    MYSQL,
    POSTGRESQL)
from tests.comparison.model import BigInt, TYPES
from tests.comparison.query_generator import QueryGenerator
from tests.comparison.model_translator import SqlWriter

LOG = getLogger(__name__)

class QueryResultComparator(object):

  # If the number of rows * cols is greater than this val, then the comparison will
  # be aborted. Raising this value also raises the risk of python being OOM killed. At
  # 10M python would get OOM killed occasionally even on a physical machine with 32GB
  # ram.
  TOO_MUCH_DATA = 1000 * 1000

  # Used when comparing float vals
  EPSILON = 0.1

  # The decimal vals will be rounded before comparison
  DECIMAL_PLACES = 2

  def __init__(self, impala_connection, reference_connection):
    self.reference_db_type = reference_connection.db_type

    self.impala_cursor = impala_connection.create_cursor()
    self.reference_cursor = reference_connection.create_cursor()

    self.impala_sql_writer = SqlWriter.create(dialect=impala_connection.db_type)
    self.reference_sql_writer = SqlWriter.create(dialect=reference_connection.db_type)

    # At this time the connection will be killed and ther comparison result will be
    # timeout.
    self.query_timeout_seconds = 3 * 60

  def compare_query_results(self, query):
    '''Execute the query, compare the data, and return a summary of the result.'''
    comparison_result = ComparisonResult(query, self.reference_db_type)

    reference_data_set = None
    impala_data_set = None
    # Impala doesn't support getting the row count without getting the rows too. So run
    # the query on the other database first.
    try:
      for sql_writer, cursor in ((self.reference_sql_writer, self.reference_cursor),
                                 (self.impala_sql_writer, self.impala_cursor)):
        self.execute_query(cursor, sql_writer.write_query(query))
        if (cursor.rowcount * len(query.select_clause.select_items)) > self.TOO_MUCH_DATA:
          comparison_result.exception = Exception('Too much data to compare')
          return comparison_result
        if reference_data_set is None:
          # MySQL returns a tuple of rows but a list is needed for sorting
          reference_data_set = list(cursor.fetchall())
          comparison_result.reference_row_count = len(reference_data_set)
        else:
          impala_data_set = cursor.fetchall()
          comparison_result.impala_row_count = len(impala_data_set)
    except Exception as e:
      comparison_result.exception = e
      LOG.debug('Error running query: %s', e, exc_info=True)
      return comparison_result

    comparison_result.query_resulted_in_data = (comparison_result.impala_row_count > 0
        or comparison_result.reference_row_count > 0)

    if comparison_result.impala_row_count != comparison_result.reference_row_count:
      return comparison_result

    for data_set in (reference_data_set, impala_data_set):
      for row_idx, row in enumerate(data_set):
        data_set[row_idx] = [self.standardize_data(data) for data in row]
      data_set.sort(cmp=self.row_sort_cmp)

    for impala_row, reference_row in \
        izip_longest(impala_data_set, reference_data_set):
      for col_idx, (impala_val, reference_val) \
          in enumerate(izip_longest(impala_row, reference_row)):
        if not self.vals_are_equal(impala_val, reference_val):
          if isinstance(impala_val, int) \
              and isinstance(reference_val, (int, float, Decimal)) \
              and abs(reference_val) > BigInt.MAX:
            # Impala will return incorrect results if the val is greater than max bigint
            comparison_result.exception = KnownError(
                'https://issues.cloudera.org/browse/IMPALA-865')
          elif isinstance(impala_val, float) \
              and (isinf(impala_val) or isnan(impala_val)):
            # In some cases, Impala gives NaNs and Infs instead of NULLs
            comparison_result.exception = KnownError(
                'https://issues.cloudera.org/browse/IMPALA-724')
          comparison_result.impala_row = impala_row
          comparison_result.reference_row = reference_row
          comparison_result.mismatch_at_row_number = row_idx + 1
          comparison_result.mismatch_at_col_number = col_idx + 1
          return comparison_result

    if len(impala_data_set) == 1:
      for val in impala_data_set[0]:
        if val:
          break
      else:
        comparison_result.query_resulted_in_data = False

    return comparison_result

  def execute_query(self, cursor, sql):
    '''Execute the query and throw a timeout if needed.'''
    def _execute_query():
      try:
        cursor.execute(sql)
      except Exception as e:
        current_thread().exception = e
    query_thread = Thread(target=_execute_query, name='Query execution thread')
    query_thread.daemon = True
    query_thread.start()
    query_thread.join(self.query_timeout_seconds)
    if query_thread.is_alive():
      if cursor.connection.supports_kill_connection:
        LOG.debug('Attempting to kill connection')
        cursor.connection.kill_connection()
        LOG.debug('Kill connection')
      cursor.close()
      cursor.connection.close()
      cursor = cursor\
          .connection\
          .connector\
          .create_connection(db_name=cursor.connection.db_name)\
          .create_cursor()
      if cursor.connection.db_type == IMPALA:
        self.impala_cursor = cursor
      else:
        self.reference_cursor = cursor
      raise QueryTimeout('Query timed out after %s seconds' % self.query_timeout_seconds)
    if hasattr(query_thread, 'exception'):
      raise query_thread.exception

  def standardize_data(self, data):
    '''Return a val that is suitable for comparison.'''
    # For float data we need to round otherwise differences in precision will cause errors
    if isinstance(data, (float, Decimal)):
      return round(data, self.DECIMAL_PLACES)
    return data

  def row_sort_cmp(self, left_row, right_row):
    for left, right in izip(left_row, right_row):
      if left is None and right is not None:
        return -1
      if left is not None and right is None:
        return 1
      result = cmp(left, right)
      if result:
        return result
    return 0

  def vals_are_equal(self, left, right):
    if left == right:
      return True
    if isinstance(left, (int, float, Decimal)) and \
        isinstance(right, (int, float, Decimal)):
      return self.floats_are_equal(left, right)
    return False

  def floats_are_equal(self, left, right):
    left = round(left, self.DECIMAL_PLACES)
    right = round(right, self.DECIMAL_PLACES)
    diff = abs(left - right)
    if left * right == 0:
      return diff < self.EPSILON
    return diff / (abs(left) + abs(right)) < self.EPSILON


class ComparisonResult(object):

  def __init__(self, query, reference_db_type):
    self.query = query
    self.reference_db_type = reference_db_type
    self.query_resulted_in_data = False
    self.impala_row_count = None
    self.reference_row_count = None
    self.mismatch_at_row_number = None
    self.mismatch_at_col_number = None
    self.impala_row = None
    self.reference_row = None
    self.exception = None
    self._error_message = None

  @property
  def error(self):
    if not self._error_message:
      if self.exception:
        self._error_message = str(self.exception)
      elif self.impala_row_count and \
          self.impala_row_count != self.reference_row_count:
        self._error_message = 'Row counts do not match: %s Impala rows vs %s %s rows' \
            % (self.impala_row_count,
               self.reference_db_type,
               self.reference_row_count)
      elif self.mismatch_at_row_number is not None:
        # Write a row like "[a, b, <<c>>, d]" where c is a bad value
        impala_row = '[' + ', '.join(
            '<<' + str(val) + '>>' if idx == self.mismatch_at_col_number - 1 else str(val)
            for idx, val in enumerate(self.impala_row)
            )  + ']'
        reference_row = '[' + ', '.join(
            '<<' + str(val) + '>>' if idx == self.mismatch_at_col_number - 1 else str(val)
            for idx, val in enumerate(self.reference_row)
            )  + ']'
        self._error_message = \
            'Column %s in row %s does not match: %s Impala row vs %s %s row' \
            % (self.mismatch_at_col_number,
               self.mismatch_at_row_number,
               impala_row,
               reference_row,
               self.reference_db_type)
    return self._error_message

  @property
  def is_known_error(self):
    return isinstance(self.exception, KnownError)

  @property
  def query_timed_out(self):
    return isinstance(self.exception, QueryTimeout)


class QueryTimeout(Exception):
  pass


class KnownError(Exception):

  def __init__(self, jira_url):
    Exception.__init__(self, 'Known issue: ' + jira_url)
    self.jira_url = jira_url


class QueryResultDiffSearcher(object):

  # Sometimes things get into a bad state and the same error loops forever
  ABORT_ON_REPEAT_ERROR_COUNT = 2

  def __init__(self, impala_connection, reference_connection, filter_col_types=[]):
    self.impala_connection = impala_connection
    self.reference_connection = reference_connection
    self.common_tables = DbConnection.describe_common_tables(
        [impala_connection, reference_connection],
        filter_col_types=filter_col_types)

    # A file-backed dict of queries that produced a discrepancy, keyed by query number
    # (in string form, as required by the dict).
    self.query_shelve_path = gettempdir() + '/query.shelve'

    # A list of all queries attempted
    self.query_log_path = gettempdir() + '/impala_query_log.sql'

  def search(self, number_of_test_queries, stop_on_result_mismatch, stop_on_crash):
    if exists(self.query_shelve_path):
      # Ensure a clean shelve will be created
      remove(self.query_shelve_path)

    start_time = time()
    impala_sql_writer = SqlWriter.create(dialect=IMPALA)
    reference_sql_writer = SqlWriter.create(
        dialect=self.reference_connection.db_type)
    query_result_comparator = QueryResultComparator(
        self.impala_connection, self.reference_connection)
    query_generator = QueryGenerator()
    query_count = 0
    queries_resulted_in_data_count = 0
    mismatch_count = 0
    query_timeout_count = 0
    known_error_count = 0
    impala_crash_count = 0
    last_error = None
    repeat_error_count = 0
    with open(self.query_log_path, 'w') as impala_query_log:
      impala_query_log.write(
         '--\n'
         '-- Stating new run\n'
         '--\n')
      while number_of_test_queries > query_count:
        query = query_generator.create_query(self.common_tables)
        impala_sql = impala_sql_writer.write_query(query)
        if 'FULL OUTER JOIN' in impala_sql and self.reference_connection.db_type == MYSQL:
          # Not supported by MySQL
          continue

        query_count += 1
        LOG.info('Running query #%s', query_count)
        impala_query_log.write(impala_sql + ';\n')
        result = query_result_comparator.compare_query_results(query)
        if result.query_resulted_in_data:
          queries_resulted_in_data_count += 1
        if result.error:
          # TODO: These first two come from psycopg2, the postgres driver. Maybe we should
          #       try a different driver? Or maybe the usage of the driver isn't correct.
          #       Anyhow ignore these failures.
          if 'division by zero' in result.error \
              or 'out of range' in result.error \
              or 'Too much data' in result.error:
            LOG.debug('Ignoring error: %s', result.error)
            query_count -= 1
            continue

          if result.is_known_error:
            known_error_count += 1
          elif result.query_timed_out:
            query_timeout_count += 1
          else:
            mismatch_count += 1
            with closing(open_shelve(self.query_shelve_path)) as query_shelve:
              query_shelve[str(query_count)] = query

          print('---Impala Query---\n')
          print(impala_sql_writer.write_query(query, pretty=True) + '\n')
          print('---Reference Query---\n')
          print(reference_sql_writer.write_query(query, pretty=True) + '\n')
          print('---Error---\n')
          print(result.error + '\n')
          print('------\n')

          if 'Could not connect' in result.error \
              or "Couldn't open transport for" in result.error:
            # if stop_on_crash:
            #   break
            # Assume Impala crashed and try restarting
            impala_crash_count += 1
            LOG.info('Restarting Impala')
            call([join(getenv('IMPALA_HOME'), 'bin/start-impala-cluster.py'),
                            '--log_dir=%s' % getenv('LOG_DIR', "/tmp/")])
            self.impala_connection.reconnect()
            query_result_comparator.impala_cursor = self.impala_connection.create_cursor()
            result = query_result_comparator.compare_query_results(query)
            if result.error:
              LOG.info('Restarting Impala')
              call([join(getenv('IMPALA_HOME'), 'bin/start-impala-cluster.py'),
                              '--log_dir=%s' % getenv('LOG_DIR', "/tmp/")])
              self.impala_connection.reconnect()
              query_result_comparator.impala_cursor = self.impala_connection.create_cursor()
            else:
              break

          if stop_on_result_mismatch and \
              not (result.is_known_error or result.query_timed_out):
            break

          if last_error == result.error \
              and not (result.is_known_error or result.query_timed_out):
            repeat_error_count += 1
            if repeat_error_count == self.ABORT_ON_REPEAT_ERROR_COUNT:
              break
          else:
            last_error = result.error
            repeat_error_count = 0
        else:
          if result.query_resulted_in_data:
            LOG.info('Results matched (%s rows)', result.impala_row_count)
          else:
            LOG.info('Query did not produce meaningful data')
          last_error = None
          repeat_error_count = 0

      return SearchResults(
          query_count,
          queries_resulted_in_data_count,
          mismatch_count,
          query_timeout_count,
          known_error_count,
          impala_crash_count,
          time() - start_time)


class SearchResults(object):
  '''This class holds information about the outcome of a search run.'''

  def __init__(self,
      query_count,
      queries_resulted_in_data_count,
      mismatch_count,
      query_timeout_count,
      known_error_count,
      impala_crash_count,
      run_time_in_seconds):
    # Approx number of queries run, some queries may have been ignored
    self.query_count = query_count
    self.queries_resulted_in_data_count = queries_resulted_in_data_count
    # Number of queries that had an error or result mismatch
    self.mismatch_count = mismatch_count
    self.query_timeout_count = query_timeout_count
    self.known_error_count = known_error_count
    self.impala_crash_count = impala_crash_count
    self.run_time_in_seconds = run_time_in_seconds

  def get_summary_text(self):
    mins, secs = divmod(self.run_time_in_seconds, 60)
    hours, mins = divmod(mins, 60)
    hours = int(hours)
    mins = int(mins)
    if hours:
      run_time = '%s hour and %s minutes' % (hours, mins)
    else:
      secs = int(secs)
      run_time = '%s seconds' % secs
      if mins:
        run_time = '%s mins and ' % mins + run_time
    summary_params = self.__dict__
    summary_params['run_time'] = run_time
    return (
        '%(mismatch_count)s mismatches found after running %(query_count)s queries in '
        '%(run_time)s.\n'
        '%(queries_resulted_in_data_count)s of %(query_count)s queries produced results.'
        '\n'
        '%(impala_crash_count)s Impala crashes occurred.\n'
        '%(known_error_count)s queries were excluded from the mismatch count because '
        'they are known errors.\n'
        '%(query_timeout_count)s queries timed out and were excluded from all counts.') \
            % summary_params


if __name__ == '__main__':
  import sys
  from optparse import NO_DEFAULT, OptionGroup, OptionParser

  parser = OptionParser()
  parser.add_option('--log-level', default='INFO',
      help='The log level to use.', choices=('DEBUG', 'INFO', 'WARN', 'ERROR'))
  parser.add_option('--db-name', default='randomness',
      help='The name of the database to use. Ex: funcal.')

  parser.add_option('--reference-db-type', default=MYSQL, choices=(MYSQL, POSTGRESQL),
      help='The type of the reference database to use. Ex: MYSQL.')
  parser.add_option('--stop-on-mismatch', default=False, action='store_true',
      help='Exit immediately upon find a discrepancy in a query result.')
  parser.add_option('--stop-on-crash', default=False, action='store_true',
      help='Exit immediately if Impala crashes.')
  parser.add_option('--query-count', default=1000, type=int,
      help='Exit after running the given number of queries.')
  parser.add_option('--exclude-types', default='Double,Float,TinyInt',
      help='A comma separated list of data types to exclude while generating queries.')

  group = OptionGroup(parser, "Impala Options")
  group.add_option('--impalad-host', default='localhost',
      help="The name of the host running the Impala daemon")
  group.add_option("--impalad-hs2-port", default=21050, type=int,
      help="The hs2 port of the host running the Impala daemon")
  parser.add_option_group(group)

  group = OptionGroup(parser, 'MySQL Options')
  group.add_option('--mysql-host', default='localhost',
      help='The name of the host running the MySQL database.')
  group.add_option('--mysql-port', default=3306, type=int,
      help='The port of the host running the MySQL database.')
  group.add_option('--mysql-user', default='root',
      help='The user name to use when connecting to the MySQL database.')
  group.add_option('--mysql-password',
      help='The password to use when connecting to the MySQL database.')
  parser.add_option_group(group)

  group = OptionGroup(parser, 'Postgresql Options')
  group.add_option('--postgresql-host', default='localhost',
      help='The name of the host running the Postgresql database.')
  group.add_option('--postgresql-port', default=5432, type=int,
      help='The port of the host running the Postgresql database.')
  group.add_option('--postgresql-user', default='postgres',
      help='The user name to use when connecting to the Postgresql database.')
  group.add_option('--postgresql-password',
      help='The password to use when connecting to the Postgresql database.')
  parser.add_option_group(group)

  for group in parser.option_groups + [parser]:
    for option in group.option_list:
      if option.default != NO_DEFAULT:
        option.help += " [default: %default]"

  options, args = parser.parse_args()

  basicConfig(level=options.log_level)

  impala_connection = DbConnector(IMPALA,
      host_name=getattr(options, 'impalad_host'),
      port=getattr(options, 'impalad_hs2_port'))\
      .create_connection(options.db_name)
  db_connector_param_key = options.reference_db_type.lower()
  reference_connection = DbConnector(options.reference_db_type,
      user_name=getattr(options, db_connector_param_key + '_user'),
      password=getattr(options, db_connector_param_key + '_password'),
      host_name=getattr(options, db_connector_param_key + '_host'),
      port=getattr(options, db_connector_param_key + '_port')) \
      .create_connection(options.db_name)
  if options.exclude_types:
    exclude_types = set(type_name.lower() for type_name
                        in options.exclude_types.split(','))
    filter_col_types = [type_ for type_ in TYPES
                        if type_.__name__.lower() in exclude_types]
  else:
    filter_col_types = []
  diff_searcher = QueryResultDiffSearcher(
      impala_connection, reference_connection, filter_col_types=filter_col_types)
  search_results = diff_searcher.search(
      options.query_count, options.stop_on_mismatch, options.stop_on_crash)
  print(search_results.get_summary_text())
  sys.exit(search_results.mismatch_count)
