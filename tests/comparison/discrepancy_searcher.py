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

'''This module will run random queries against existing databases and compare the
   results.

'''

# TODO: IMPALA-4600: refactor this module
from __future__ import absolute_import, division, print_function
from builtins import range, round, zip
from copy import deepcopy
from decimal import Decimal
from logging import getLogger
from math import isinf, isnan
from os import getenv, symlink, unlink
from os.path import join as join_path
from random import choice, randint
from string import ascii_lowercase, digits
from subprocess import call
from tempfile import gettempdir
from threading import current_thread, Thread
from time import time

from tests.comparison.db_types import BigInt
from tests.comparison.db_connection import (
    DbCursor,
    IMPALA,
    HIVE,
    MYSQL,
    ORACLE,
    POSTGRESQL)
from tests.comparison.model_translator import SqlWriter
from tests.comparison.query import (
    FromClause,
    InsertClause,
    InsertStatement,
    Query,
    StatementExecutionMode,
    SelectClause,
    SelectItem)
from tests.comparison.query_flattener import QueryFlattener
from tests.comparison.statement_generator import get_generator
from tests.comparison import db_connection, compat

LOG = getLogger(__name__)


class QueryResultComparator(object):
  '''Used for comparing the results of a Query across two databases'''

  # Used when comparing FLOAT values
  EPSILON = 0.1

  # The DECIMAL values will be rounded before comparison
  DECIMAL_PLACES = 2

  def __init__(self, query_profile, ref_conn,
      test_conn, query_timeout_seconds, flatten_dialect=None):
    '''test/ref_conn arguments should be an instance of DbConnection'''
    self.ref_conn = ref_conn
    self.ref_sql_writer = SqlWriter.create(
        dialect=ref_conn.db_type, nulls_order_asc=query_profile.nulls_order_asc())
    self.test_conn = test_conn
    self.test_sql_writer = SqlWriter.create(dialect=test_conn.db_type)

    compat.setup_ref_database(self.ref_conn)

    ref_cursor = ref_conn.cursor()
    test_cursor = test_conn.cursor()

    self.query_executor = QueryExecutor(
        [ref_cursor, test_cursor],
        [self.ref_sql_writer, self.test_sql_writer],
        query_timeout_seconds=query_timeout_seconds,
        flatten_dialect=flatten_dialect)

  @property
  def test_db_type(self):
    return self.test_conn.db_type

  @property
  def ref_db_type(self):
    return self.ref_conn.db_type

  def compare_query_results(self, query):
    '''Execute the query, compare the data, and return a ComparisonResult, which
       summarizes the outcome.
    '''
    comparison_result = ComparisonResult(query, self.test_db_type, self.ref_db_type)
    (ref_sql, ref_exception, ref_data_set, ref_cursor_description), (test_sql,
        test_exception, test_data_set, test_cursor_description) = \
            self.query_executor.fetch_query_results(query)

    comparison_result.ref_sql = ref_sql
    comparison_result.test_sql = test_sql

    if ref_exception:
      comparison_result.exception = ref_exception
      error_message = str(ref_exception)
      if 'Year is out of valid range: 1400..9999' in error_message:
        # This comes from Postgresql. Overflow errors will be ignored.
        comparison_result.exception = TypeOverflow(error_message)
      LOG.debug('%s encountered an error running query: %s',
          self.ref_conn.db_type, ref_exception, exc_info=True)
      return comparison_result

    if test_exception:
      # "known errors" will be ignored
      error_message = str(test_exception)
      known_error = None

      if self.test_db_type is db_connection.IMPALA:
        if 'Expressions in the ORDER BY clause must not be constant' in error_message \
            or 'Expressions in the PARTITION BY clause must not be consta' in error_message:
          # It's too much work to avoid this bug. Just ignore it if it comes up.
          known_error = KnownError('IMPALA-1354')
        elif 'GROUP BY expression must not contain aggregate functions' in error_message \
            or 'select list expression not produced by aggregation output' in error_message:
          known_error = KnownError('IMPALA-1423')
        elif ('max(' in error_message or 'min(' in error_message) \
            and 'only supported with an UNBOUNDED PRECEDING start bound' in error_message:
          # This analytic isn't supported and ignoring this here is much easier than not
          # generating the query...
          known_error = KnownError('MAX UNBOUNDED PRECISION')
        elif 'IN and/or EXISTS subquery predicates are not supported in binary predicates' \
            in error_message:
          known_error = KnownError('IMPALA-1418')
        elif 'Unsupported predicate with subquery' in error_message:
          known_error = KnownError('IMPALA-1950')
        elif 'RIGHT OUTER JOIN type with no equi-join' in error_message:
          known_error = KnownError('IMPALA-3063')
        elif 'Operation is in ERROR_STATE' in error_message:
          known_error = KnownError('Mem limit exceeded')
      elif self.test_db_type is db_connection.HIVE:
        if 'ParseException line' in error_message and 'missing ) at' in \
              error_message and query.select_clause and \
              query.select_clause.analytic_items:
          known_error = KnownError("HIVE-14871")

      if known_error:
        comparison_result.exception = known_error
      else:
        comparison_result.exception = test_exception
        LOG.debug('%s encountered an error running query: %s',
            self.test_conn.db_type, test_exception, exc_info=True)
      return comparison_result

    comparison_result.ref_row_count = len(ref_data_set)
    comparison_result.test_row_count = len(test_data_set)
    comparison_result.query_resulted_in_data = (comparison_result.test_row_count > 0 or
                                                comparison_result.ref_row_count > 0)
    if comparison_result.ref_row_count != comparison_result.test_row_count:
      return comparison_result

    # Standardize data (round FLOATs) in each column, and sort the data set
    for data_set in (ref_data_set, test_data_set):
      for row_idx, row in enumerate(data_set):
        data_set[row_idx] = []
        for col_idx, col in enumerate(row):
          data_set[row_idx].append(self.standardize_data(col,
              ref_cursor_description[col_idx], test_cursor_description[col_idx]))
      # TODO: If the query has an ORDER BY clause, sorting should only be done within
      #       subsets of rows that have the same order by values.
      data_set.sort(cmp=self.row_sort_cmp)

    found_data = False  # Will be set to True if the result contains non-zero/NULL data
    for ref_row, test_row in zip(ref_data_set, test_data_set):
      for col_idx, (ref_val, test_val) in enumerate(zip(ref_row, test_row)):
        if ref_val or test_val:   # Ignores zeros, ex "SELECT COUNT(*) ... WHERE FALSE"
          found_data = True
        if self.vals_are_equal(ref_val, test_val):
          continue
        if isinstance(test_val, int) \
            and isinstance(ref_val, (int, float, Decimal)) \
            and abs(ref_val) > BigInt.MAX:
          # Impala will return incorrect results if the val is greater than max BigInt
          comparison_result.exception = KnownError('IMPALA-865')
        elif isinstance(test_val, float) \
            and (isinf(test_val) or isnan(test_val)):
          # In some cases, Impala gives NaNs and Infs instead of NULLs
          comparison_result.exception = KnownError('IMPALA-724')
        comparison_result.ref_row = ref_row
        comparison_result.test_row = test_row
        comparison_result.mismatch_at_row_number = row_idx + 1
        comparison_result.mismatch_at_col_number = col_idx + 1
        return comparison_result
    comparison_result.query_resulted_in_data = found_data

    # If we're here, it means ref and test data sets are equal for a DML statement.
    if isinstance(query, (InsertStatement,)):
      comparison_result.modified_rows_count = test_data_set[0][0]

    return comparison_result

  def standardize_data(self, data, ref_col_description, test_col_description):
    '''Return a val that is suitable for comparison.'''
    # For float data we need to round otherwise differences in precision will cause errors
    if isinstance(data, float):
      return round(data, self.DECIMAL_PLACES)
    if isinstance(data, Decimal):
      if ref_col_description[5] is not None and test_col_description[5] is not None:
        return round(data, min(ref_col_description[5], test_col_description[5]))
    return data

  def row_sort_cmp(self, ref_row, test_row):
    '''Comparison used for sorting. '''
    for ref_val, test_val in zip(ref_row, test_row):
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

  # TODO: Set to false while IMPALA-3336 is a problem. Disabling random query options
  # seems to reduce IMPALA-3336 occurances.
  ENABLE_RANDOM_QUERY_OPTIONS = False

  # If the number of rows * cols is greater than this val, then the comparison will
  # be aborted. Raising this value also raises the risk of python being OOM killed. At
  # 10M python would get OOM killed occasionally even on a physical machine with 32GB
  # ram.
  TOO_MUCH_DATA = 1000 * 1000

  def __init__(self, cursors, sql_writers, query_timeout_seconds, flatten_dialect=None):
    '''cursors should be a list of db_connector.Cursors.

       sql_writers should be a list of model_translator.SqlWriters, with translators in
       the same order as cursors in "cursors".
    '''
    self.query_timeout_seconds = query_timeout_seconds
    self.cursors = cursors
    self.sql_writers = sql_writers
    self.query_logs = list()
    # SQL dialect for which the queries should be flattened
    self.flatten_dialect = flatten_dialect

    for cursor in cursors:
      # A list of all queries attempted
      query_log_path = gettempdir() + '/test_query_log_%s_%s.sql' \
          % (cursor.db_type.lower(), time())
      self.query_logs.append(open(query_log_path, 'w'))
      link = gettempdir() + '/test_query_log_%s.sql' % cursor.db_type.lower()
      try:
        unlink(link)
      except OSError as e:
        if 'No such file' not in str(e):
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

  def set_impala_query_options(self, cursor):
    opts = """
        SET MEM_LIMIT={mem_limit};
        SET BATCH_SIZE={batch_size};
        SET DISABLE_CODEGEN={disable_codegen};
        SET DISABLE_OUTERMOST_TOPN={disable_outermost_topn};
        SET DISABLE_ROW_RUNTIME_FILTERING={disable_row_runtime_filtering};
        SET DISABLE_STREAMING_PREAGGREGATIONS={disable_streaming_preaggregations};
        SET DISABLE_UNSAFE_SPILLS={disable_unsafe_spills};
        SET EXEC_SINGLE_NODE_ROWS_THRESHOLD={exec_single_node_rows_threshold};
        SET BUFFER_POOL_LIMIT={buffer_pool_limit};
        SET MAX_SCAN_RANGE_LENGTH={max_scan_range_length};
        SET NUM_NODES={num_nodes};
        SET NUM_SCANNER_THREADS={num_scanner_threads};
        SET OPTIMIZE_PARTITION_KEY_SCANS={optimize_partition_key_scans};
        SET RUNTIME_BLOOM_FILTER_SIZE={runtime_bloom_filter_size};
        SET RUNTIME_FILTER_MODE={runtime_filter_mode};
        SET RUNTIME_FILTER_WAIT_TIME_MS={runtime_filter_wait_time_ms};
        SET SCAN_NODE_CODEGEN_THRESHOLD={scan_node_codegen_threshold}""".format(
            mem_limit=randint(1024 ** 3, 10 * 1024 ** 3),
            batch_size=randint(1, 4096),
            disable_codegen=choice((0, 1)),
            disable_outermost_topn=choice((0, 1)),
            disable_row_runtime_filtering=choice((0, 1)),
            disable_streaming_preaggregations=choice((0, 1)),
            disable_unsafe_spills=choice((0, 1)),
            exec_single_node_rows_threshold=randint(1, 100000000),
            buffer_pool_limit=randint(1, 100000000),
            max_scan_range_length=randint(1, 100000000),
            num_nodes=randint(3, 3),
            num_scanner_threads=randint(1, 100),
            optimize_partition_key_scans=choice((0, 1)),
            random_replica=choice((0, 1)),
            replica_preference=choice(("CACHE_LOCAL", "DISK_LOCAL", "REMOTE")),
            runtime_bloom_filter_size=randint(4096, 16777216),
            runtime_filter_mode=choice(("OFF", "LOCAL", "GLOBAL")),
            runtime_filter_wait_time_ms=randint(1, 100000000),
            scan_node_codegen_threshold=randint(1, 100000000))
    LOG.debug(opts)
    for opt in opts.strip().split(";"):
      cursor.execute(opt)

  def fetch_query_results(self, query):
    '''Concurrently execute the query using each cursor and return a list of tuples
       containing the result information for each cursor. The tuple format is
       (<exception or None>, <data set or None>).

       If query_timeout_seconds is reached and the connection is killable then the
       query will be cancelled and the connection reset. Otherwise the query will
       continue to run in the background.

       "query" should be an instance of query.Query.
    '''
    if query.execution in (StatementExecutionMode.CREATE_TABLE_AS,
                           StatementExecutionMode.CREATE_VIEW_AS):
      self._table_or_view_name = self._create_random_table_name()
    elif isinstance(query, (InsertStatement,)):
      self._table_or_view_name = query.dml_table.name

    query_threads = list()
    for sql_writer, cursor, log_file in zip(
        self.sql_writers, self.cursors, self.query_logs
    ):
      if self.ENABLE_RANDOM_QUERY_OPTIONS and cursor.db_type == IMPALA:
        self.set_impala_query_options(cursor)
      query_thread = Thread(
          target=self._fetch_sql_results,
          args=[query, cursor, sql_writer, log_file],
          name='{db_type}-exec-{id_}'.format(
              db_type=cursor.db_type, id_=id(query)))
      query_thread.daemon = True
      query_thread.sql = ''
      query_thread.data_set = None
      query_thread.cursor_description = None
      query_thread.exception = None
      query_thread.start()
      query_threads.append(query_thread)

    end_time = time() + self.query_timeout_seconds
    for query_thread, cursor in zip(query_threads, self.cursors):
      join_time = end_time - time()
      if join_time > 0:
        query_thread.join(join_time)
      if query_thread.is_alive():
        # Kill connection and reconnect to return cursor to initial state.
        if cursor.conn.supports_kill:
          LOG.debug('Attempting to kill connection')
          cursor.conn.kill()
          LOG.debug('Kill connection')
        try:
          # TODO: Sometimes this takes a very long time causing the program to appear to
          #       hang. Maybe this should be done in another thread so a timeout can be
          #       applied?
          cursor.close()
        except Exception as e:
          LOG.info('Error closing cursor: %s', e)
        cursor.reconnect()
        query_thread.exception = QueryTimeout(
            'Query timed out after %s seconds' % self.query_timeout_seconds)

      if (query.execution in (StatementExecutionMode.CREATE_TABLE_AS,
                              StatementExecutionMode.DML_TEST)):
        cursor.drop_table(self._table_or_view_name)
      elif query.execution == StatementExecutionMode.CREATE_VIEW_AS:
        cursor.drop_view(self._table_or_view_name)

    return [(query_thread.sql, query_thread.exception, query_thread.data_set,
             query_thread.cursor_description) for query_thread in query_threads]

  def _fetch_sql_results(self, query, cursor, sql_writer, log_file):
    '''Execute the query using the cursor and set the result or exception on the local
       thread.
    '''
    try:
      log_file.write('/***** Start Query *****/\n')
      if sql_writer.DIALECT == self.flatten_dialect:
        # Converts the query model for the flattened version of the data. This is for
        # testing of Impala nested types support.
        query = deepcopy(query)
        QueryFlattener().flatten(query)
      if query.execution == StatementExecutionMode.CREATE_TABLE_AS:
        setup_sql = sql_writer.write_create_table_as(query, self._table_or_view_name)
        query_sql = 'SELECT * FROM ' + self._table_or_view_name
      elif query.execution == StatementExecutionMode.CREATE_VIEW_AS:
        setup_sql = sql_writer.write_create_view(query, self._table_or_view_name)
        query_sql = 'SELECT * FROM ' + self._table_or_view_name
      elif isinstance(query, (InsertStatement,)):
        setup_sql = sql_writer.write_query(query)
        # TODO: improve validation (IMPALA-4599). This is good enough for looking for
        # crashes on DML statements
        query_sql = 'SELECT COUNT(*) FROM ' + self._table_or_view_name
      else:
        setup_sql = None
        query_sql = sql_writer.write_query(query)
      if setup_sql:
        LOG.debug("Executing on %s:\n%s", cursor.db_type, setup_sql)
        current_thread().sql = setup_sql + ';\n'
        log_file.write(setup_sql + ';\n')
        log_file.flush()
        cursor.execute(setup_sql)
      LOG.debug("Executing on %s:\n%s", cursor.db_type, query_sql)
      current_thread().sql += query_sql
      log_file.write(query_sql + ';\n')
      log_file.write('/***** End Query *****/\n')
      log_file.flush()
      cursor.execute(query_sql)
      col_count = len(cursor.description)
      batch_size = max(10000 // col_count, 1)
      row_limit = self.TOO_MUCH_DATA // col_count
      data_set = list()
      current_thread().data_set = data_set
      current_thread().cursor_description = cursor.description
      LOG.debug("Fetching results from %s", cursor.db_type)
      while True:
        batch = cursor.fetchmany(batch_size)
        data_set.extend(batch)
        if len(batch) < batch_size:
          if cursor.db_type == IMPALA:
            impala_log = cursor.get_log()
            if 'Expression overflowed, returning NULL' in impala_log:
              raise TypeOverflow('Numeric overflow; data may not match')
          break
        if len(data_set) > row_limit:
          raise DataLimitExceeded('Too much data')
      if isinstance(query, (InsertStatement,)):
        LOG.debug('Total row count for {0}: {1}'.format(
          cursor.db_type, str(data_set)))
    except Exception as e:
      current_thread().exception = e

  def _create_random_table_name(self):
    char_choices = ascii_lowercase
    chars = list()
    for idx in range(4):   # will result in ~1M combinations
      if idx == 1:
        char_choices += '_' + digits
      chars.append(choice(char_choices))
    return 'qgen_' + ''.join(chars)


class ComparisonResult(object):
  '''Represents a result.'''

  def __init__(self, query, test_db_type, ref_db_type):
    self.query = query
    self.test_db_type = test_db_type
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
    self.modified_rows_count = None
    self._error_message = None

  @property
  def error(self):
    if not self._error_message:
      if self.exception:
        self._error_message = str(self.exception)
      elif (self.ref_row_count or self.test_row_count) and \
          self.ref_row_count != self.test_row_count:
        self._error_message = 'Row counts do not match: %s %s rows vs %s %s rows' \
            % (self.test_row_count,
               self.test_db_type,
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
            'Column %s in row %s does not match: %s %s row vs %s %s row' \
            % (self.mismatch_at_col_number,
               self.mismatch_at_row_number,
               test_row,
               self.test_db_type,
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


class FrontendExceptionSearcher(object):

  def __init__(self, query_profile, ref_conn, test_conn):
    '''query_profile should be an instance of one of the profiles in query_profile.py'''
    self.query_profile = query_profile
    self.ref_conn = ref_conn
    self.test_conn = test_conn
    self.ref_sql_writer = SqlWriter.create(dialect=ref_conn.db_type)
    self.test_sql_writer = SqlWriter.create(dialect=test_conn.db_type)
    with ref_conn.cursor() as ref_cursor:
      with test_conn.cursor() as test_cursor:
        self.common_tables = DbCursor.describe_common_tables([ref_cursor, test_cursor])
        if not self.common_tables:
          raise Exception("Unable to find a common set of tables in both databases")

  def search(self, number_of_test_queries):

    def on_ref_db_error(e, sql):
      LOG.warn("Error generating explain plan for reference db:\n%s\n%s" % (e, sql))

    def on_test_db_error(e, sql):
      LOG.error("Error generating explain plan for test db:\n%s" % sql)
      raise e

    for idx in range(number_of_test_queries):
      LOG.info("Explaining query #%s" % (idx + 1))
      statement_type = self.query_profile.choose_statement()
      statement_generator = get_generator(statement_type)(self.query_profile)
      if issubclass(statement_type, (InsertStatement,)):
        dml_table = self.query_profile.choose_table(self.common_tables)
      else:
        dml_table = None
      query = statement_generator.generate_statement(
          self.common_tables, dml_table=dml_table)
      if not self._explain_query(self.ref_conn, self.ref_sql_writer, query,
          on_ref_db_error):
        continue
      self._explain_query(self.test_conn, self.test_sql_writer, query,
          on_test_db_error)

  def _explain_query(self, conn, writer, query, exception_handler):
    sql = writer.write_query(query)
    try:
      with conn.cursor() as cursor:
        cursor.execute("EXPLAIN %s" % sql)
        return True
    except Exception as e:
      exception_handler(e, sql)
      return False


class QueryResultDiffSearcher(object):
  '''This class uses the query generator (query_generator.py) along with the
     query profile (query_profile.py) to randomly generate queries then executes the
     queries on the reference and test databases, then compares the results.
  '''

  # Sometimes things get into a bad state and the same error loops forever
  ABORT_ON_REPEAT_ERROR_COUNT = 2

  COPY_TABLE_SUFFIX = '__qgen_copy'

  def __init__(self, query_profile, ref_conn, test_conn):
    '''query_profile should be an instance of one of the profiles in query_profile.py'''
    self.query_profile = query_profile
    self.ref_conn = ref_conn
    self.test_conn = test_conn
    with ref_conn.cursor() as ref_cursor:
      with test_conn.cursor() as test_cursor:
        self.common_tables = DbCursor.describe_common_tables([ref_cursor, test_cursor])
        if not self.common_tables:
          raise Exception("Unable to find a common set of tables in both databases")

  def _concurrently_copy_table(self, src_table):
    """
    Given a Table object, create another Table with the same schema and return the new
    Table object.  The schema will be created in both the test and reference databases.

    The data is then copied in both the ref and test databases using threads.
    """
    with test_conn.cursor() as test_cursor:
      test_cursor.execute('SHOW CREATE TABLE {0}'.format(src_table.name))
      (create_table_sql,) = test_cursor.fetchall()[0]
      new_table_name = src_table.name + self.COPY_TABLE_SUFFIX
      create_table_sql = create_table_sql.replace(src_table.name, new_table_name, 1)
      test_cursor.drop_table(new_table_name)
      test_cursor.execute(create_table_sql)
      new_table = test_cursor.describe_table(new_table_name)
    with ref_conn.cursor() as ref_cursor:
      ref_cursor.drop_table(new_table_name)
      ref_cursor.create_table(new_table)

    copy_select_query = Query()
    copy_select_query.select_clause = SelectClause(
        [SelectItem(col) for col in src_table.cols])
    copy_select_query.from_clause = FromClause(src_table)

    if new_table.primary_keys:
      conflict_action = InsertClause.CONFLICT_ACTION_IGNORE
    else:
      conflict_action = InsertClause.CONFLICT_ACTION_DEFAULT

    table_copy_statement = InsertStatement(
        insert_clause=InsertClause(new_table, conflict_action=conflict_action),
        select_query=copy_select_query, execution=StatementExecutionMode.DML_SETUP)

    result = self.query_result_comparator.compare_query_results(table_copy_statement)
    if result.error:
      raise Exception('setup SQL to copy table failed: {0}'.format(result.error))
    self._dml_table_size = result.modified_rows_count

    return new_table

  def search(self, number_of_test_queries, stop_on_result_mismatch, stop_on_crash,
             query_timeout_seconds):
    '''Returns an instance of SearchResults, which is a summary report. This method
       oversees the generation, execution, and comparison of queries.

      number_of_test_queries should an integer indicating the maximum number of queries
      to generate and execute.
    '''
    start_time = time()
    self.query_result_comparator = QueryResultComparator(
        self.query_profile, self.ref_conn, self.test_conn, query_timeout_seconds)
    query_count = 0
    queries_resulted_in_data_count = 0
    mismatch_count = 0
    query_timeout_count = 0
    known_error_count = 0
    test_crash_count = 0
    last_error = None
    repeat_error_count = 0
    count_effective_dml_statements = 0
    count_rows_affected_by_dml = 0

    while number_of_test_queries > query_count:
      statement_type = self.query_profile.choose_statement()
      statement_generator = get_generator(statement_type)(self.query_profile)
      dml_table = None
      if issubclass(statement_type, (InsertStatement,)):
        dml_choice_src_table = self.query_profile.choose_table(self.common_tables)
        # Copy the table we want to INSERT/UPSERT INTO. Do this for the following reasons:
        #
        # 1. If we don't copy, the tables will get larger and larger
        # 2. If we want to avoid tables getting larger and larger, we have to come up
        # with some threshold about when to cut and start over.
        # 3. If we keep INSERT/UPSERTing into tables and finally find a crash, we have to
        # replay all previous INSERT/UPSERTs again. Those INSERTs may not produce the
        # same rows as before. To maximize the chance of bug reproduction, run every
        # INSERT/UPSERT on a pristine table.
        dml_table = self._concurrently_copy_table(dml_choice_src_table)
      statement = statement_generator.generate_statement(
          self.common_tables, dml_table=dml_table)
      if isinstance(statement, Query):
        # we can re-write statement execution here to possibly be a CREATE TABLE AS SELECT
        # or CREATE VIEW AS SELECT
        statement.execution = self.query_profile.get_query_execution()
      query_count += 1
      LOG.info('Running query #%s', query_count)
      result = self.query_result_comparator.compare_query_results(statement)
      if result.query_resulted_in_data:
        queries_resulted_in_data_count += 1
      if result.modified_rows_count:
        count_effective_dml_statements += 1
        count_rows_affected_by_dml += abs(
            result.modified_rows_count - self._dml_table_size)
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
          impalad_args = [
              '-convert_legacy_hive_parquet_utc_timestamps=true',
          ]
          impala_restart_cmd = [
              join_path(getenv('IMPALA_HOME'), 'bin/start-impala-cluster.py'),
              '--log_dir={0}'.format(getenv('LOG_DIR', "/tmp/")),
              '--impalad_args="{0}"'.format(' '.join(impalad_args)),
          ]
          call(impala_restart_cmd)
          self.test_conn.reconnect()
          self.query_result_comparator.test_cursor = self.test_conn.cursor()
          result = self.query_result_comparator.compare_query_results(statement)
          if result.error:
            LOG.info('Restarting Impala')
            call(impala_restart_cmd)
            self.test_conn.reconnect()
            self.query_result_comparator.test_cursor = self.test_conn.cursor()
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
        time() - start_time,
        count_effective_dml_statements,
        count_rows_affected_by_dml)


class SearchResults(object):
  '''This class holds information about the outcome of a search run.'''

  def __init__(self,
      query_count,
      queries_resulted_in_data_count,
      mismatch_count,
      query_timeout_count,
      known_error_count,
      test_crash_count,
      run_time_in_seconds,
      count_effective_dml_statements,
      count_rows_affected_by_dml
    ):
    # Approx number of queries run, some queries may have been ignored
    self.query_count = query_count
    self.queries_resulted_in_data_count = queries_resulted_in_data_count
    # Number of queries that had an error or result mismatch
    self.mismatch_count = mismatch_count
    self.query_timeout_count = query_timeout_count
    self.known_error_count = known_error_count
    self.test_crash_count = test_crash_count
    self.run_time_in_seconds = run_time_in_seconds
    # number of DML statements that actually modified tables
    self.count_effective_dml_statements = count_effective_dml_statements
    # total number of rows modified by DML statemnts
    self.count_rows_affected_by_dml = count_rows_affected_by_dml

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
        '%(count_effective_dml_statements)s of %(query_count)s statements modified a '
        'total of %(count_rows_affected_by_dml)s rows\n'
        '%(test_crash_count)s crashes occurred.\n'
        '%(known_error_count)s queries were excluded from the mismatch count because '
        'they are known errors.\n'
        '%(query_timeout_count)s queries timed out and were excluded from all counts.') \
            % summary_params


if __name__ == '__main__':
  import sys
  from argparse import ArgumentDefaultsHelpFormatter, ArgumentParser

  from tests.comparison import cli_options
  from tests.comparison.query_profile import PROFILES

  parser = ArgumentParser(formatter_class=ArgumentDefaultsHelpFormatter)
  cli_options.add_logging_options(parser)
  cli_options.add_db_name_option(parser)
  cli_options.add_cluster_options(parser)
  cli_options.add_connection_option_groups(parser)
  cli_options.add_timeout_option(parser)

  parser.add_argument('--test-db-type', default=IMPALA,
      choices=(HIVE, IMPALA, MYSQL, ORACLE, POSTGRESQL),
      help='The type of the test database to use. Ex: IMPALA.')
  parser.add_argument('--ref-db-type', default=POSTGRESQL,
      choices=(MYSQL, ORACLE, POSTGRESQL),
      help='The type of the ref database to use. Ex: POSTGRESQL.')
  parser.add_argument('--stop-on-mismatch', default=False, action='store_true',
      help='Exit immediately upon find a discrepancy in a query result.')
  parser.add_argument('--stop-on-crash', default=False, action='store_true',
      help='Exit immediately if Impala crashes.')
  parser.add_argument('--query-count', default=1000000, type=int,
      help='Exit after running the given number of queries.')
  parser.add_argument('--exclude-types', default='',
      help='A comma separated list of data types to exclude while generating queries.')
  parser.add_argument('--explain-only', action='store_true',
      help="Don't run the queries only explain them to see if there was an error in "
      "planning.")
  profiles = dict()
  for profile in PROFILES:
    profile_name = profile.__name__
    if profile_name.endswith('Profile'):
      profile_name = profile_name[:-1 * len('Profile')]
    profiles[profile_name.lower()] = profile
  parser.add_argument('--profile', default='default',
      choices=(sorted(profiles.keys())),
      help='Determines the mix of SQL features to use during query generation.')
  # TODO: Seed the random query generator for repeatable queries?

  args = parser.parse_args()
  cli_options.configure_logging(
      args.log_level, debug_log_file=args.debug_log_file, log_thread_name=True)
  cluster = cli_options.create_cluster(args)

  ref_conn = cli_options.create_connection(args, args.ref_db_type, db_name=args.db_name)
  if args.test_db_type == IMPALA:
    test_conn = cluster.impala.connect(db_name=args.db_name)
  elif args.test_db_type == HIVE:
    test_conn = cluster.hive.connect(db_name=args.db_name)
  else:
    test_conn = cli_options.create_connection(
        args, args.test_db_type, db_name=args.db_name)
  # Create an instance of profile class (e.g. DefaultProfile)
  query_profile = profiles[args.profile]()
  if args.explain_only:
    searcher = FrontendExceptionSearcher(query_profile, ref_conn, test_conn)
    searcher.search(args.query_count)
  else:
    diff_searcher = QueryResultDiffSearcher(query_profile, ref_conn, test_conn)
    query_timeout_seconds = args.timeout
    search_results = diff_searcher.search(
        args.query_count, args.stop_on_mismatch, args.stop_on_crash,
        query_timeout_seconds)
    print(search_results)
    sys.exit(search_results.mismatch_count)
