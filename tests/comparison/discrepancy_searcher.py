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

'''This module will run random queries against existing databases and compare the
   results.

'''
from decimal import Decimal
from itertools import izip
from logging import getLogger
from math import isinf, isnan
from os import getenv, symlink, unlink
from os.path import join as join_path
from random import choice
from string import ascii_lowercase, digits
from subprocess import call
from threading import current_thread, Thread
from tempfile import gettempdir
from time import time

from tests.comparison.types import BigInt
from tests.comparison.db_connector import (
    DbConnection,
    DbConnector,
    IMPALA,
    HIVE,
    MYSQL,
    ORACLE,
    POSTGRESQL)
from tests.comparison.query_generator import QueryGenerator
from tests.comparison.model_translator import SqlWriter

LOG = getLogger(__name__)

class QueryResultComparator(object):
  '''Used for comparing the results of a Query across two databases'''

  # Used when comparing FLOAT values
  EPSILON = 0.1

  # The DECIMAL values will be rounded before comparison
  DECIMAL_PLACES = 2

  def __init__(self, ref_connection, test_connection):
    '''test/ref_connection arguments should be an instance of DbConnection'''
    ref_cursor = ref_connection.create_cursor()
    test_cursor = test_connection.create_cursor()

    self.ref_connection = ref_connection
    self.ref_sql_writer = SqlWriter.create(dialect=ref_connection.db_type)
    self.test_connection = test_connection
    self.test_sql_writer = SqlWriter.create(dialect=test_connection.db_type)

    self.query_executor = QueryExecutor(
        [ref_cursor, test_cursor],
        [self.ref_sql_writer, self.test_sql_writer],
        query_timeout_seconds=(3 * 60))

  @property
  def ref_db_type(self):
    return self.ref_connection.db_type

  def compare_query_results(self, query):
    '''Execute the query, compare the data, and return a ComparisonResult, which
       summarizes the outcome.
    '''
    comparison_result = ComparisonResult(query, self.ref_db_type)
    (ref_sql, ref_exception, ref_data_set), (test_sql, test_exception, test_data_set) = \
        self.query_executor.fetch_query_results(query)

    comparison_result.ref_sql = ref_sql
    comparison_result.test_sql = test_sql

    if ref_exception:
      comparison_result.exception = ref_exception
      error_message = str(ref_exception)
      if 'Year is out of valid range: 1400..10000' in error_message:
        # This comes from Postgresql. Overflow errors will be ignored.
        comparison_result.exception = TypeOverflow(error_message)
      LOG.debug('%s encountered an error running query: %s',
          self.ref_connection.db_type, ref_exception, exc_info=True)
      return comparison_result

    if test_exception:
      # "known errors" will be ignored
      error_message = str(test_exception)
      known_error = None
      if 'Expressions in the ORDER BY clause must not be constant' in error_message \
          or 'Expressions in the PARTITION BY clause must not be consta' in error_message:
        # It's too much work to avoid this bug. Just ignore it if it comes up.
        known_error = KnownError('https://issues.cloudera.org/browse/IMPALA-1354')
      elif 'GROUP BY expression must not contain aggregate functions' in error_message \
          or 'select list expression not produced by aggregation output' in error_message:
        known_error = KnownError('https://issues.cloudera.org/browse/IMPALA-1423')
      elif ('max(' in error_message or 'min(' in error_message) \
          and 'only supported with an UNBOUNDED PRECEDING start bound' in error_message:
        # This analytic isn't supported and ignoring this here is much easier than not
        # generating the query...
        known_error = KnownError('MAX UNBOUNDED PRECISION')
      elif 'IN and/or EXISTS subquery predicates are not supported in binary predicates' \
          in error_message:
        known_error = KnownError('https://issues.cloudera.org/browse/IMPALA-1418')
      elif 'Unsupported predicate with subquery' in error_message:
        known_error = KnownError('https://issues.cloudera.org/browse/IMPALA-1950')
      if known_error:
        comparison_result.exception = known_error
      else:
        comparison_result.exception = test_exception
        LOG.debug('%s encountered an error running query: %s',
            self.test_connection.db_type, test_exception, exc_info=True)
      return comparison_result

    comparison_result.ref_row_count = len(ref_data_set)
    comparison_result.test_row_count = len(test_data_set)
    comparison_result.query_resulted_in_data = (comparison_result.test_row_count > 0
        or comparison_result.ref_row_count > 0)
    if comparison_result.ref_row_count != comparison_result.test_row_count:
      return comparison_result

    # Standardize data (round FLOATs) in each column, and sort the data set
    for data_set in (ref_data_set, test_data_set):
      for row_idx, row in enumerate(data_set):
        data_set[row_idx] = [self.standardize_data(data) for data in row]
      # TODO: If the query has an ORDER BY clause, sorting should only be done within
      #       subsets of rows that have the same order by values.
      data_set.sort(cmp=self.row_sort_cmp)

    found_data = False  # Will be set to True if the result contains non-zero/NULL data
    for ref_row, test_row in izip(ref_data_set, test_data_set):
      for col_idx, (ref_val, test_val) in enumerate(izip(ref_row, test_row)):
        if ref_val or test_val:   # Ignores zeros, ex "SELECT COUNT(*) ... WHERE FALSE"
          found_data = True
        if self.vals_are_equal(ref_val, test_val):
          continue
        if isinstance(test_val, int) \
            and isinstance(ref_val, (int, float, Decimal)) \
            and abs(ref_val) > BigInt.MAX:
          # Impala will return incorrect results if the val is greater than max BigInt
          comparison_result.exception = KnownError(
              'https://issues.cloudera.org/browse/IMPALA-865')
        elif isinstance(test_val, float) \
            and (isinf(test_val) or isnan(test_val)):
          # In some cases, Impala gives NaNs and Infs instead of NULLs
          comparison_result.exception = KnownError(
              'https://issues.cloudera.org/browse/IMPALA-724')
        comparison_result.ref_row = ref_row
        comparison_result.test_row = test_row
        comparison_result.mismatch_at_row_number = row_idx + 1
        comparison_result.mismatch_at_col_number = col_idx + 1
        return comparison_result
    comparison_result.query_resulted_in_data = found_data

    return comparison_result

  def standardize_data(self, data):
    '''Return a val that is suitable for comparison.'''
    # For float data we need to round otherwise differences in precision will cause errors
    if isinstance(data, float):
      return round(data, self.DECIMAL_PLACES)
    return data

  def row_sort_cmp(self, ref_row, test_row):
    '''Comparison used for sorting. '''
    for ref_val, test_val in izip(ref_row, test_row):
      if ref_val is None and test_val is not None:
        return -1
      if ref_val is not None and test_val is None:
        return 1
      result = cmp(ref_val, test_val)
      if result:
        return result
    return 0

  def vals_are_equal(self, ref, test):
    '''Compares if two values are equal in two cells. Floats are considered equal if the
    difference between them is very small.'''
    if ref == test:
      return True
    # For some reason Postgresql will return Decimals when using some aggregate
    # functions such as AVG().
    if isinstance(ref, (float, Decimal)) and isinstance(test, float):
      return self.floats_are_equal(ref, test)
    LOG.debug("Values differ, reference: %s (%s), test: %s (%s)",
        ref, type(ref),
        test, type(test))
    return False

  def floats_are_equal(self, ref, test):
    '''Compare two floats.'''
    ref = round(ref, self.DECIMAL_PLACES)
    test = round(test, self.DECIMAL_PLACES)
    diff = abs(ref - test)
    if ref * test == 0:
      return diff < self.EPSILON
    result = diff / (abs(ref) + abs(test)) < self.EPSILON
    if not result:
      LOG.debug("Floats differ, diff: %s, |reference|: %s, |test|: %s",
          diff, abs(ref), abs(test))
    return result


class QueryExecutor(object):
  '''Concurrently executes queries'''

  # If the number of rows * cols is greater than this val, then the comparison will
  # be aborted. Raising this value also raises the risk of python being OOM killed. At
  # 10M python would get OOM killed occasionally even on a physical machine with 32GB
  # ram.
  TOO_MUCH_DATA = 1000 * 1000

  def __init__(self, cursors, sql_writers, query_timeout_seconds):
    '''cursors should be a list of db_connector.Cursors.

       sql_writers should be a list of model_translator.SqlWriters, with translators in
       the same order as cursors in "cursors".
    '''
    self.query_timeout_seconds = query_timeout_seconds
    self.cursors = cursors
    self.sql_writers = sql_writers
    self.query_logs = list()


    for cursor in cursors:
      # A list of all queries attempted
      query_log_path = gettempdir() + '/test_query_log_%s_%s.sql' \
          % (cursor.connection.db_type.lower(), time())
      self.query_logs.append(open(query_log_path, 'w'))
      link = gettempdir() + '/test_query_log_%s.sql' % cursor.connection.db_type.lower()
      try:
        unlink(link)
      except OSError as e:
        if not 'No such file' in str(e):
          raise e
      try:
        symlink(query_log_path, link)
      except OSError as e:
        # TODO: Figure out what the error message is where there is a race condition
        #       and ignore it.
        raise e

    # In case the query will be executed as a "CREATE TABLE <name> AS ..." or
    # "CREATE VIEW <name> AS ...", this will be the value of "<name>".
    self._table_or_view_name = None

  def fetch_query_results(self, query):
    '''Concurrently execute the query using each cursor and return a list of tuples
       containing the result information for each cursor. The tuple format is
       (<exception or None>, <data set or None>).

       If query_timeout_seconds is reached and the connection is killable then the
       query will be cancelled and the connection reset. Otherwise the query will
       continue to run in the background.

       "query" should be an instance of query.Query.
    '''
    if query.execution != 'RAW':
      self._table_or_view_name = self._create_random_table_name()

    query_threads = list()
    for sql_writer, cursor, log_file \
        in izip(self.sql_writers, self.cursors, self.query_logs):
      query_thread = Thread(
          target=self._fetch_sql_results,
          args=[query, cursor, sql_writer, log_file],
          name='Query execution thread {0}'.format(current_thread().name))
      query_thread.daemon = True
      query_thread.sql = ''
      query_thread.data_set = None
      query_thread.exception = None
      query_thread.start()
      query_threads.append(query_thread)

    end_time = time() + self.query_timeout_seconds
    for query_thread, cursor in izip(query_threads, self.cursors):
      join_time = end_time - time()
      if join_time > 0:
        query_thread.join(join_time)
      if query_thread.is_alive():
        # Kill connection and reconnect to return cursor to initial state.
        if cursor.connection.supports_kill_connection:
          LOG.debug('Attempting to kill connection')
          cursor.connection.kill_connection()
          LOG.debug('Kill connection')
        try:
          # XXX: Sometimes this takes a very long time causing the program to appear to
          #      hang. Maybe this should be done in another thread so a timeout can be
          #      applied?
          cursor.close()
        except Exception as e:
          LOG.info('Error closing cursor: %s', e)
        cursor.connection.reconnect()
        cursor.cursor = cursor.connection.create_cursor().cursor
        query_thread.exception = QueryTimeout(
            'Query timed out after %s seconds' % self.query_timeout_seconds)

    return [(query_thread.sql, query_thread.exception, query_thread.data_set)
            for query_thread in query_threads]

  def _fetch_sql_results(self, query, cursor, sql_writer, log_file):
    '''Execute the query using the cursor and set the result or exception on the local
       thread.
    '''
    try:
      log_file.write('/***** Start Query *****/\n')
      if query.execution == 'CREATE_TABLE_AS':
        setup_sql = sql_writer.write_create_table_as(query, self._table_or_view_name)
        query_sql = 'SELECT * FROM ' + self._table_or_view_name
      elif query.execution == 'VIEW':
        setup_sql = sql_writer.write_create_view(query, self._table_or_view_name)
        query_sql = 'SELECT * FROM ' + self._table_or_view_name
      else:
        setup_sql = None
        query_sql = sql_writer.write_query(query)
      if setup_sql:
        LOG.debug("Executing on %s:\n%s", cursor.connection.db_type, setup_sql)
        current_thread().sql = setup_sql + ';\n'
        log_file.write(setup_sql + ';\n')
        log_file.flush()
        cursor.execute(setup_sql)
      LOG.debug("Executing on %s:\n%s", cursor.connection.db_type, query_sql)
      current_thread().sql += query_sql
      log_file.write(query_sql + ';\n')
      log_file.write('/***** End Query *****/\n')
      log_file.flush()
      cursor.execute(query_sql)
      col_count = len(cursor.description)
      batch_size = min(10000 / col_count, 1)
      row_limit = self.TOO_MUCH_DATA / col_count
      data_set = list()
      current_thread().data_set = data_set
      LOG.debug("Fetching results from %s", cursor.connection.db_type)
      while True:
        batch = cursor.fetchmany(batch_size)
        data_set.extend(batch)
        if len(batch) < batch_size:
          if cursor.connection.db_type == IMPALA:
            impala_log = cursor.get_log()
            if 'Expression overflowed, returning NULL' in impala_log:
              raise TypeOverflow('Numeric overflow; data may not match')
          break
        if len(data_set) > row_limit:
          raise DataLimitExceeded('Too much data')
    except Exception as e:
      current_thread().exception = e
    finally:
      if query.execution == 'CREATE_TABLE_AS':
        cursor.connection.drop_table(self._table_or_view_name)
      elif query.execution == 'VIEW':
        cursor.connection.drop_view(self._table_or_view_name)

  def _create_random_table_name(self):
    char_choices = ascii_lowercase
    chars = list()
    for idx in xrange(4):   # will result in ~1M combinations
      if idx == 1:
        char_choices += '_' + digits
      chars.append(choice(char_choices))
    return 'qgen_' + ''.join(chars)


class ComparisonResult(object):
  '''Represents a result.'''

  def __init__(self, query, ref_db_type):
    self.query = query
    self.ref_db_type = ref_db_type
    self.ref_sql = None
    self.test_sql = None
    self.query_resulted_in_data = False
    self.ref_row_count = None
    self.test_row_count = None
    self.mismatch_at_row_number = None
    self.mismatch_at_col_number = None
    self.ref_row = None   # The test row where mismatch happened
    self.test_row = None   # The reference row where mismatch happened
    self.exception = None
    self._error_message = None

  @property
  def error(self):
    if not self._error_message:
      if self.exception:
        self._error_message = str(self.exception)
      elif (self.ref_row_count or self.test_row_count) and \
          self.ref_row_count != self.test_row_count:
        self._error_message = 'Row counts do not match: %s Impala rows vs %s %s rows' \
            % (self.test_row_count,
               self.ref_db_type,
               self.ref_row_count)
      elif self.mismatch_at_row_number is not None:
        # Write a row like "[a, b, <<c>>, d]" where c is a bad value
        test_row = '[' + ', '.join(
            '<<' + str(val) + '>>' if idx == self.mismatch_at_col_number - 1 else str(val)
            for idx, val in enumerate(self.test_row)
            )  + ']'
        ref_row = '[' + ', '.join(
            '<<' + str(val) + '>>' if idx == self.mismatch_at_col_number - 1 else str(val)
            for idx, val in enumerate(self.ref_row)
            )  + ']'
        self._error_message = \
            'Column %s in row %s does not match: %s Impala row vs %s %s row' \
            % (self.mismatch_at_col_number,
               self.mismatch_at_row_number,
               test_row,
               ref_row,
               self.ref_db_type)
    return self._error_message

  @property
  def is_known_error(self):
    return isinstance(self.exception, KnownError)

  @property
  def query_timed_out(self):
    return isinstance(self.exception, QueryTimeout)


QueryTimeout = type('QueryTimeout', (Exception, ), {})
TypeOverflow = type('TypeOverflow', (Exception, ), {})
DataLimitExceeded = type('DataLimitExceeded', (Exception, ), {})

class KnownError(Exception):

  def __init__(self, jira_url):
    Exception.__init__(self, 'Known issue: ' + jira_url)
    self.jira_url = jira_url


class QueryResultDiffSearcher(object):
  '''This class uses the query generator (query_generator.py) along with the
     query profile (query_profile.py) to randomly generate queries then executes the
     queries on the reference and test databases, then compares the results.
  '''

  # Sometimes things get into a bad state and the same error loops forever
  ABORT_ON_REPEAT_ERROR_COUNT = 2

  def __init__(self, query_profile, ref_connection, test_connection):
    '''query_profile should be an instance of one of the profiles in query_profile.py'''
    self.query_profile = query_profile
    self.ref_connection = ref_connection
    self.test_connection = test_connection
    self.common_tables = DbConnection.describe_common_tables(
        [ref_connection, test_connection])

  def search(self, number_of_test_queries, stop_on_result_mismatch, stop_on_crash):
    '''Returns an instance of SearchResults, which is a summary report. This method
       oversees the generation, execution, and comparison of queries.

      number_of_test_queries should an integer indicating the maximum number of queries
      to generate and execute.
    '''
    start_time = time()
    query_result_comparator = QueryResultComparator(
        self.ref_connection, self.test_connection)
    query_generator = QueryGenerator(self.query_profile)
    query_count = 0
    queries_resulted_in_data_count = 0
    mismatch_count = 0
    query_timeout_count = 0
    known_error_count = 0
    test_crash_count = 0
    last_error = None
    repeat_error_count = 0
    while number_of_test_queries > query_count:
      query = query_generator.create_query(self.common_tables)
      query.execution = self.query_profile.get_query_execution()
      query_count += 1
      LOG.info('Running query #%s', query_count)
      result = query_result_comparator.compare_query_results(query)
      if result.query_resulted_in_data:
        queries_resulted_in_data_count += 1
      if isinstance(result.exception, DataLimitExceeded) \
          or isinstance(result.exception, TypeOverflow):
        continue
      if result.error:
        # TODO: These first two come from psycopg2, the postgres driver. Maybe we should
        #       try a different driver? Or maybe the usage of the driver isn't correct.
        #       Anyhow ignore these failures.
        if 'division by zero' in result.error \
            or 'out of range' in result.error:
          LOG.debug('Ignoring error: %s', result.error)
          query_count -= 1
          continue

        if result.is_known_error:
          known_error_count += 1
        elif result.query_timed_out:
          query_timeout_count += 1
        else:
          mismatch_count += 1

        print('---Test Query---\n')
        print(result.test_sql + '\n')
        print('---Reference Query---\n')
        print(result.ref_sql + '\n')
        print('---Error---\n')
        print(result.error + '\n')
        print('------\n')

        if 'Could not connect' in result.error \
            or "Couldn't open transport for" in result.error:
          if stop_on_crash:
            break
          # Assume Impala crashed and try restarting
          test_crash_count += 1
          LOG.info('Restarting Impala')
          call([join_path(getenv('IMPALA_HOME'), 'bin/start-impala-cluster.py'),
                          '--log_dir=%s' % getenv('LOG_DIR', "/tmp/")])
          self.test_connection.reconnect()
          query_result_comparator.test_cursor = self.test_connection.create_cursor()
          result = query_result_comparator.compare_query_results(query)
          if result.error:
            LOG.info('Restarting Impala')
            call([join_path(getenv('IMPALA_HOME'), 'bin/start-impala-cluster.py'),
                            '--log_dir=%s' % getenv('LOG_DIR', "/tmp/")])
            self.test_connection.reconnect()
            query_result_comparator.test_cursor = self.test_connection.create_cursor()
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
          LOG.info('Results matched (%s rows)', result.test_row_count)
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
        test_crash_count,
        time() - start_time)


class SearchResults(object):
  '''This class holds information about the outcome of a search run.'''

  def __init__(self,
      query_count,
      queries_resulted_in_data_count,
      mismatch_count,
      query_timeout_count,
      known_error_count,
      test_crash_count,
      run_time_in_seconds):
    # Approx number of queries run, some queries may have been ignored
    self.query_count = query_count
    self.queries_resulted_in_data_count = queries_resulted_in_data_count
    # Number of queries that had an error or result mismatch
    self.mismatch_count = mismatch_count
    self.query_timeout_count = query_timeout_count
    self.known_error_count = known_error_count
    self.test_crash_count = test_crash_count
    self.run_time_in_seconds = run_time_in_seconds

  def __str__(self):
    '''Returns the string representation of the results.'''
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
        '%(test_crash_count)s crashes occurred.\n'
        '%(known_error_count)s queries were excluded from the mismatch count because '
        'they are known errors.\n'
        '%(query_timeout_count)s queries timed out and were excluded from all counts.') \
            % summary_params


if __name__ == '__main__':
  import sys
  from optparse import OptionParser

  import tests.comparison.cli_options as cli_options
  from tests.comparison.query_profile import PROFILES

  parser = OptionParser()
  cli_options.add_logging_options(parser)
  cli_options.add_db_name_option(parser)
  cli_options.add_connection_option_groups(parser)

  parser.add_option('--test-db-type', default=IMPALA,
      choices=(HIVE, IMPALA, MYSQL, ORACLE, POSTGRESQL),
      help='The type of the test database to use. Ex: IMPALA.')
  parser.add_option('--ref-db-type', default=POSTGRESQL,
      choices=(MYSQL, ORACLE, POSTGRESQL),
      help='The type of the ref database to use. Ex: POSTGRESQL.')
  parser.add_option('--stop-on-mismatch', default=False, action='store_true',
      help='Exit immediately upon find a discrepancy in a query result.')
  parser.add_option('--stop-on-crash', default=False, action='store_true',
      help='Exit immediately if Impala crashes.')
  parser.add_option('--query-count', default=1000000, type=int,
      help='Exit after running the given number of queries.')
  parser.add_option('--exclude-types', default='',
      help='A comma separated list of data types to exclude while generating queries.')
  profiles = dict()
  for profile in PROFILES:
    profile_name = profile.__name__
    if profile_name.endswith('Profile'):
      profile_name = profile_name[:-1 * len('Profile')]
    profiles[profile_name.lower()] = profile
  parser.add_option('--profile', default='default',
      choices=(sorted(profiles.keys())),
      help='Determines the mix of SQL features to use during query generation.')
  # TODO: Seed the random query generator for repeatable queries?

  cli_options.add_default_values_to_help(parser)

  options, args = parser.parse_args()
  cli_options.configure_logging(options.log_level)

  db_connector_param_key = options.ref_db_type.lower()
  ref_connection = DbConnector(options.ref_db_type,
      user_name=getattr(options, db_connector_param_key + '_user'),
      password=getattr(options, db_connector_param_key + '_password'),
      host_name=getattr(options, db_connector_param_key + '_host'),
      port=getattr(options, db_connector_param_key + '_port')) \
      .create_connection(options.db_name)
  db_connector_param_key = options.test_db_type.lower()
  test_connection = DbConnector(options.test_db_type,
      user_name=getattr(options, db_connector_param_key + '_user', None),
      password=getattr(options, db_connector_param_key + '_password', None),
      host_name=getattr(options, db_connector_param_key + '_host', None),
      port=getattr(options, db_connector_param_key + '_port', None)) \
      .create_connection(options.db_name)
  # Create an instance of profile class (e.g. DefaultProfile)
  query_profile = profiles[options.profile]()
  diff_searcher = QueryResultDiffSearcher(query_profile, ref_connection, test_connection)
  search_results = diff_searcher.search(
      options.query_count, options.stop_on_mismatch, options.stop_on_crash)
  print(search_results)
  sys.exit(search_results.mismatch_count)
