# encoding=utf-8
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

import os
import pytest
import re
import signal
import socket
import tempfile

from shell.impala_shell import ImpalaShell as ImpalaShellClass

from subprocess import call, Popen
from tests.common.environ import ImpalaTestClusterProperties
from tests.common.impala_service import ImpaladService
from tests.common.impala_test_suite import ImpalaTestSuite, IMPALAD_HS2_HOST_PORT
from tests.common.skip import SkipIf
from tests.common.test_dimensions import create_beeswax_hs2_dimension
from time import sleep, time
from util import (get_impalad_host_port, assert_var_substitution, run_impala_shell_cmd,
                  ImpalaShell, IMPALA_SHELL_EXECUTABLE)
from contextlib import closing


DEFAULT_QUERY = 'select 1'
QUERY_FILE_PATH = os.path.join(os.environ['IMPALA_HOME'], 'tests', 'shell')

RUSSIAN_CHARS = (u"А, Б, В, Г, Д, Е, Ё, Ж, З, И, Й, К, Л, М, Н, О, П, Р,"
                 u"С, Т, У, Ф, Х, Ц,Ч, Ш, Щ, Ъ, Ы, Ь, Э, Ю, Я")


def find_query_option(key, string, strip_brackets=True):
  """
  Parses 'string' for 'key': value pairs, and returns value. It is assumed
  that the 'string' contains the pair exactly once.

  If 'strip_brackets' is true, enclosing [] are stripped (this is used to mark
  query options that have their default value).
  """
  pattern = r'^\s*%s: (.*)\s*$' % key
  values = re.findall(pattern, string, re.MULTILINE)
  assert len(values) == 1
  return values[0].strip("[]") if strip_brackets else values[0]

@pytest.fixture
def empty_table(unique_database, request):
  """Create an empty table within the test database before executing test.

  The table will have the same name as the test_function itself. Setup and teardown
  of the database is handled by the unique_database fixture.

  Args:
    unique_database: pytest fixture defined in conftest.py
    request: standard pytest request fixture

  Returns:
    fq_table_name (str): the fully qualified name of the table: : dbname.table_name
  """
  table_name = request.node.function.func_name
  fq_table_name = '.'.join([unique_database, table_name])
  stmt = "CREATE TABLE %s (i integer, s string)" % fq_table_name
  request.instance.execute_query_expect_success(request.instance.client, stmt,
                                                query_options={'sync_ddl': 1})
  return fq_table_name


@pytest.fixture
def populated_table(empty_table, request):
  """
  Populate a table within the test database before executing test.

  The table will have the same name as the test_function itself. Setup and teardown
  of the database is handled by the unique_database fixture.

  Args:
    empty_table: pytest fixture that creates an empty table
    request: standard pytest request fixture

  Returns:
    fq_table_name (str): the fully qualified name of the table: : dbname.table_name
  """
  fq_table_name = empty_table
  stmt = "insert into %s values (1, 'a'),(1, 'b'),(3, 'b')" % fq_table_name
  request.instance.execute_query_expect_success(request.instance.client, stmt)
  return fq_table_name


class TestImpalaShell(ImpalaTestSuite):
  """A set of sanity tests for the Impala shell commandline parameters.

  The tests need to maintain Python 2.4 compatibility as a sub-goal of having
  shell tests is to ensure that it's not broken in systems running Python 2.4.
  The tests need a running impalad instance in order to execute queries.

  TODO:
     * Test individual modules.
     * Add a test for a kerberized impala.
  """

  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    # Run with both beeswax and HS2 to ensure that behaviour is the same.
    cls.ImpalaTestMatrix.add_dimension(create_beeswax_hs2_dimension())

  def test_no_args(self, vector):
    args = ['-q', DEFAULT_QUERY]
    run_impala_shell_cmd(vector, args)

  def test_multiple_queries(self, vector):
    queries = ';'.join([DEFAULT_QUERY] * 3)
    args = ['-q', queries, '-B']
    run_impala_shell_cmd(vector, args)

  def test_multiple_queries_with_escaped_backslash(self, vector):
    """Regression test for string containing an escaped backslash.

    This relies on the patch at thirdparty/patches/sqlparse/0001-....patch.
    """
    run_impala_shell_cmd(vector, ['-q', r'''select '\\'; select '\'';''', '-B'])

  def test_default_db(self, vector, empty_table):
    db_name, table_name = empty_table.split('.')
    args = ['-d', db_name, '-q', 'describe {0}'.format(table_name), '--quiet']
    run_impala_shell_cmd(vector, args)
    args = ['-q', 'describe {0}'.format(table_name)]
    run_impala_shell_cmd(vector, args, expect_success=False)
    # test keyword parquet is interpreted as an identifier
    # when passed as an argument to -d
    args = ['-d', 'parquet']
    result = run_impala_shell_cmd(vector, args)
    assert "Query: use `parquet`" in result.stderr, result.stderr
    # test if backticking is idempotent
    args = ['-d', '```parquet```']
    result = run_impala_shell_cmd(vector, args)
    assert "Query: use `parquet`" in result.stderr, result.stderr

  def test_unsecure_message(self, vector):
    results = run_impala_shell_cmd(vector, [], wait_until_connected=False)
    assert "Starting Impala Shell without Kerberos authentication" in results.stderr

  def test_print_header(self, vector, populated_table):
    args = ['--print_header', '-B', '--output_delim=,', '-q',
            'select * from {0}'.format(populated_table)]
    result = run_impala_shell_cmd(vector, args)
    result_rows = result.stdout.strip().split('\n')
    assert len(result_rows) == 4
    assert result_rows[0].split(',') == ['i', 's']

    args = ['-B', '--output_delim=,', '-q', 'select * from {0}'.format(populated_table)]
    result = run_impala_shell_cmd(vector, args)
    result_rows = result.stdout.strip().split('\n')
    assert len(result_rows) == 3

  @pytest.mark.execute_serially
  def test_kerberos_option(self, vector):
    args = ['-k']

    # If you have a valid kerberos ticket in your cache, this test fails - so
    # here we set a bogus KRB5CCNAME in the environment so that klist (and other
    # kerberos commands) won't find the normal ticket cache.
    # KERBEROS TODO: add kerberized cluster test case
    os.environ["KRB5CCNAME"] = "/tmp/this/file/hopefully/does/not/exist"

    # The command will fail because we're trying to connect to a kerberized impalad.
    results = run_impala_shell_cmd(vector, args, expect_success=False)
    # Check that impala is using the right service name.
    assert "Using service name 'impala'" in results.stderr
    assert "Starting Impala Shell using Kerberos authentication" in results.stderr
    # Check that Impala warns the user if klist does not exist on the system, or if
    # no kerberos tickets are initialized.
    try:
      call(["klist"])
      expected_error_msg = ("-k requires a valid kerberos ticket but no valid kerberos "
                            "ticket found.")
      assert expected_error_msg in results.stderr
    except OSError:
      assert 'klist not found on the system' in results.stderr
    # Make sure we don't try to re-connect
    assert "retrying the connection with a secure transport" not in results.stderr
    # Change the service name
    args += ['-s', 'foobar']
    results = run_impala_shell_cmd(vector, args, expect_success=False)
    assert "Using service name 'foobar'" in results.stderr

  def test_continue_on_error(self, vector):
    args = ['-c', '-q', 'select foo; select bar;']
    run_impala_shell_cmd(vector, args)
    # Should fail
    args = ['-q', 'select foo; select bar;']
    run_impala_shell_cmd(vector, args, expect_success=False)

  def test_execute_queries_from_file(self, vector):
    args = ['-f', '{0}/test_file_comments.sql'.format(QUERY_FILE_PATH), '--quiet', '-B']
    result = run_impala_shell_cmd(vector, args)
    output = result.stdout
    args = ['-f', '{0}/test_file_no_comments.sql'.format(QUERY_FILE_PATH), '--quiet',
            '-B']
    result = run_impala_shell_cmd(vector, args)
    assert output == result.stdout, "Queries with comments not parsed correctly"

  def test_completed_query_errors(self, vector):
    args = ['-q', 'set abort_on_error=false;'
            'select count(*) from functional_seq_snap.bad_seq_snap']
    result = run_impala_shell_cmd(vector, args)
    assert 'WARNINGS:' in result.stderr
    assert 'Bad synchronization marker' in result.stderr
    assert 'Expected: ' in result.stderr
    assert 'Actual: ' in result.stderr
    assert 'Problem parsing file' in result.stderr

  def test_completed_query_errors_1(self, vector):
    args = ['-q', 'set abort_on_error=true;'
            'select id from functional_parquet.bad_column_metadata t']
    result = run_impala_shell_cmd(vector, args, expect_success=False)
    assert 'ERROR: Column metadata states there are 11 values, ' in result.stderr
    assert 'but read 10 values from column id.' in result.stderr

  def test_completed_query_errors_2(self, vector):
    args = ['-q', 'set abort_on_error=true; '
            'select id, cnt from functional_parquet.bad_column_metadata t, '
            '(select 1 cnt) u']
    result = run_impala_shell_cmd(vector, args, expect_success=False)
    assert 'ERROR: Column metadata states there are 11 values, ' in result.stderr,\
        result.stderr
    assert 'but read 10 values from column id.' in result.stderr, result.stderr

  def test_no_warnings_in_log_with_quiet_mode(self, vector):
    """Regression test for IMPALA-4222."""
    args = ['--quiet', '-q', 'set abort_on_error=false;'
            'select count(*) from functional_seq_snap.bad_seq_snap']
    result = run_impala_shell_cmd(vector, args)
    assert 'WARNINGS:' not in result.stderr, result.stderr

  def test_removed_query_option(self, vector):
    """Test that removed query options produce warning."""
    result = run_impala_shell_cmd(vector, ['-q', 'set disable_cached_reads=true'],
        expect_success=True)
    assert "Ignoring removed query option: 'disable_cached_reads'" in result.stderr,\
        result.stderr

  def test_output_format(self, vector):
    expected_output = ['1'] * 3
    args = ['-q', 'select 1,1,1', '-B', '--quiet']
    result = run_impala_shell_cmd(vector, args)
    actual_output = [r.strip() for r in result.stdout.split('\t')]
    assert actual_output == expected_output
    result = run_impala_shell_cmd(vector, args + ['--output_delim=|'])
    actual_output = [r.strip() for r in result.stdout.split('|')]
    assert actual_output == expected_output
    result = run_impala_shell_cmd(vector, args + ['--output_delim=||'],
                                  expect_success=False)
    assert "Illegal delimiter" in result.stderr

  def test_do_methods(self, vector, empty_table):
    """Ensure that the do_ methods in the shell work.

    Some of the do_ methods are implicitly tested in other tests, and as part of the
    test setup.
    """
    # explain
    args = ['-q', 'explain select 1']
    run_impala_shell_cmd(vector, args)
    # show
    args = ['-q', 'show tables']
    run_impala_shell_cmd(vector, args)
    # with
    args = ['-q', 'with t1 as (select 1) select * from t1']
    run_impala_shell_cmd(vector, args)
    # set
    # spaces around the = sign
    args = ['-q', 'set batch_size  =   10']
    run_impala_shell_cmd(vector, args)
    # no spaces around the = sign
    args = ['-q', 'set batch_size=10']
    run_impala_shell_cmd(vector, args)
    # test query options displayed
    args = ['-q', 'set']
    result_set = run_impala_shell_cmd(vector, args)
    assert 'MEM_LIMIT: [0]' in result_set.stdout
    # test to check that explain_level is STANDARD
    assert 'EXPLAIN_LEVEL: [STANDARD]' in result_set.stdout
    # test to check that configs without defaults show up as []
    assert 'COMPRESSION_CODEC: []' in result_set.stdout
    # test values displayed after setting value
    args = ['-q', 'set mem_limit=1g;set']
    result_set = run_impala_shell_cmd(vector, args)
    # single list means one instance of mem_limit in displayed output
    assert 'MEM_LIMIT: 1g' in result_set.stdout
    assert 'MEM_LIMIT: [0]' not in result_set.stdout
    # Negative tests for set
    # use : instead of =
    args = ['-q', 'set batch_size:10']
    run_impala_shell_cmd(vector, args, expect_success=False)
    # use 2 = signs
    args = ['-q', 'set batch_size=10=50']
    run_impala_shell_cmd(vector, args, expect_success=False)
    # describe and desc should return the same result.
    args = ['-q', 'describe {0}'.format(empty_table), '-B']
    result_describe = run_impala_shell_cmd(vector, args)
    args = ['-q', 'desc {0}'.format(empty_table), '-B']
    result_desc = run_impala_shell_cmd(vector, args)
    assert result_describe.stdout == result_desc.stdout

  def test_runtime_profile(self, vector):
    # test summary is in both the profile printed by the
    # -p option and the one printed by the profile command
    args = ['-p', '-q', 'select 1; profile;']
    result_set = run_impala_shell_cmd(vector, args)
    # This regex helps us uniquely identify a profile.
    regex = re.compile("Operator\s+#Hosts\s+Avg\s+Time")
    # We expect two query profiles.
    assert len(re.findall(regex, result_set.stdout)) == 2, \
        "Could not detect two profiles, stdout: %s" % result_set.stdout

  def test_summary(self, vector):
    args = ['-q', 'select count(*) from functional.alltypes; summary;']
    result_set = run_impala_shell_cmd(vector, args)
    assert "03:AGGREGATE" in result_set.stdout

    args = ['-q', 'summary;']
    result_set = run_impala_shell_cmd(vector, args, expect_success=False)
    assert "Could not retrieve summary: no previous query" in result_set.stderr

    args = ['-q', 'show tables; summary;']
    result_set = run_impala_shell_cmd(vector, args)
    assert "Summary not available" in result_set.stderr

    # Test queries without an exchange
    args = ['-q', 'select 1; summary;']
    result_set = run_impala_shell_cmd(vector, args)
    assert "00:UNION" in result_set.stdout

  @pytest.mark.execute_serially
  def test_queries_closed(self, vector):
    """Regression test for IMPALA-897."""
    args = ['-f', '{0}/test_close_queries.sql'.format(QUERY_FILE_PATH), '--quiet', '-B']
    # Execute the shell command async
    p = ImpalaShell(vector, args)

    impalad_service = ImpaladService(get_impalad_host_port(vector).split(':')[0])
    # The last query in the test SQL script will sleep for 10 seconds, so sleep
    # here for 5 seconds and verify the number of in-flight queries is 1.
    sleep(5)
    assert 1 == impalad_service.get_num_in_flight_queries()
    assert p.get_result().rc == 0
    assert 0 == impalad_service.get_num_in_flight_queries()

  def test_cancellation(self, vector):
    """Test cancellation (Ctrl+C event). Run a query that sleeps 10ms per row so will run
    for 110s if not cancelled, but will detect cancellation quickly because of the small
    batch size."""
    query = "set num_nodes=1; set mt_dop=1; set batch_size=1; \
             select sleep(10) from functional_parquet.alltypesagg"
    p = ImpalaShell(vector, ['-q', query])
    p.wait_for_query_start()
    os.kill(p.pid(), signal.SIGINT)
    result = p.get_result()
    assert "Cancelling Query" in result.stderr, result.stderr

  @pytest.mark.execute_serially
  def test_query_cancellation_during_fetch(self, vector):
    """IMPALA-1144: Test cancellation (CTRL+C) while results are being
    fetched"""
    pytest.skip("""Skipping as it occasionally gets stuck in Jenkins builds
                resulting the build to timeout.""")
    # A select query where fetch takes several seconds
    stmt = "with v as (values (1 as x), (2), (3), (4)) " + \
        "select * from v, v v2, v v3, v v4, v v5, v v6, v v7, v v8, " + \
        "v v9, v v10, v v11"
    # Kill happens when the results are being fetched
    self.run_and_verify_query_cancellation_test(vector, stmt, "FINISHED")

  @pytest.mark.execute_serially
  def test_query_cancellation_during_wait_to_finish(self, vector):
    """IMPALA-1144: Test cancellation (CTRL+C) while the query is in the
    wait_to_finish state"""
    # A select where wait_to_finish takes several seconds
    stmt = "select * from tpch.customer c1, tpch.customer c2, " + \
           "tpch.customer c3 order by c1.c_name"
    # Kill happens in wait_to_finish state
    self.run_and_verify_query_cancellation_test(vector, stmt, "RUNNING")

  def wait_for_query_state(self, vector, stmt, state, max_retry=15):
    """Checks the in flight queries on Impala debug page. Polls the state of
    the query statement from parameter every second until the query gets to
    a state given via parameter or a maximum retry count is reached.
    Restriction: Only works if there is only one in flight query."""
    impalad_service = ImpaladService(get_impalad_host_port(vector).split(':')[0])
    if not impalad_service.wait_for_num_in_flight_queries(1):
      raise Exception("No in flight query found")

    retry_count = 0
    while retry_count <= max_retry:
      query_info = impalad_service.get_in_flight_queries()[0]
      if query_info['stmt'] != stmt:
        exc_text = "The found in flight query is not the one under test: " + \
            query_info['stmt']
        raise Exception(exc_text)
      if query_info['state'] == state:
        return
      retry_count += 1
      sleep(1.0)
    raise Exception("Query didn't reach desired state: " + state)

  def run_and_verify_query_cancellation_test(self, vector, stmt, cancel_at_state):
    """Starts the execution of the received query, waits until the query
    execution in fact starts and then cancels it. Expects the query
    cancellation to succeed."""
    p = ImpalaShell(vector, ['-q', stmt])

    self.wait_for_query_state(vector, stmt, cancel_at_state)

    os.kill(p.pid(), signal.SIGINT)
    result = p.get_result()
    assert "Cancelling Query" in result.stderr
    assert "Invalid query handle" not in result.stderr

  def test_get_log_once(self, vector, empty_table):
    """Test that get_log() is always called exactly once."""
    # Query with fetch
    args = ['-q', 'select * from functional.alltypeserror']
    result = run_impala_shell_cmd(vector, args)
    assert result.stderr.count('WARNINGS') == 1

    # Insert query (doesn't fetch)
    drop_args = ['-q', 'drop table if exists {0}'.format(empty_table)]
    run_impala_shell_cmd(vector, drop_args)

    args = ['-q', 'create table {0} like functional.alltypeserror'.format(empty_table)]
    run_impala_shell_cmd(vector, args)

    args = ['-q', 'insert overwrite {0} partition(year, month)'
           'select * from functional.alltypeserror'.format(empty_table)]
    result = run_impala_shell_cmd(vector, args)

    assert result.stderr.count('WARNINGS') == 1

  def test_international_characters(self, vector):
    """Sanity test to ensure that the shell can read international characters."""
    args = ['-B', '-q', "select '{0}'".format(RUSSIAN_CHARS.encode('utf-8'))]
    result = run_impala_shell_cmd(vector, args)
    assert 'UnicodeDecodeError' not in result.stderr
    assert RUSSIAN_CHARS.encode('utf-8') in result.stdout

  def test_international_characters_prettyprint(self, vector):
    """IMPALA-2717: ensure we can handle international characters in pretty-printed
    output"""
    args = ['-q', "select '{0}'".format(RUSSIAN_CHARS.encode('utf-8'))]
    result = run_impala_shell_cmd(vector, args)
    assert 'UnicodeDecodeError' not in result.stderr
    assert RUSSIAN_CHARS.encode('utf-8') in result.stdout

  def test_international_characters_prettyprint_tabs(self, vector):
    """IMPALA-2717: ensure we can handle international characters in pretty-printed
    output when pretty-printing falls back to delimited output."""
    args = ['-q', "select '{0}\\t'".format(RUSSIAN_CHARS.encode('utf-8'))]
    result = run_impala_shell_cmd(vector, args)
    protocol = vector.get_value('protocol')
    if protocol == 'beeswax':
      assert 'Reverting to tab delimited text' in result.stderr
    else:
      # HS2 does not need to fall back, but should behave appropriately.
      assert protocol == 'hs2', protocol
      assert 'Reverting to tab delimited text' not in result.stderr
    assert 'UnicodeDecodeError' not in result.stderr
    assert RUSSIAN_CHARS.encode('utf-8') in result.stdout

  def test_global_config_file(self, vector):
    """Test global and user configuration files."""
    args = []
    # shell uses shell options in global config
    env = {
      ImpalaShellClass.GLOBAL_CONFIG_FILE: '{0}/good_impalarc2'.format(QUERY_FILE_PATH)}
    result = run_impala_shell_cmd(vector, args, env=env)
    assert 'WARNING:' not in result.stderr, \
      "A valid config file should not trigger any warning: {0}".format(result.stderr)
    assert 'Query: select 2' in result.stderr

    # shell uses query options in global config
    args = ['-q', 'set;']
    result = run_impala_shell_cmd(vector, args, env=env)
    assert 'DEFAULT_FILE_FORMAT: avro' in result.stdout

    # shell options and query options in global config get overriden
    # by options in user config
    args = ['--config_file={0}/good_impalarc'.format(QUERY_FILE_PATH),
            """--query=select '${VAR:msg1}'; set"""]
    result = run_impala_shell_cmd(vector, args, env=env)
    assert 'Query: select \'hello\'' in result.stderr
    assert 'DEFAULT_FILE_FORMAT: parquet' in result.stdout

    # command line options override options in global config
    args = ['--query_option=DEFAULT_FILE_FORMAT=text',
            """--query=select '${VAR:msg1}'; set"""]
    result = run_impala_shell_cmd(vector, args, env=env)
    assert 'Query: select \'test\'' in result.stderr
    assert 'DEFAULT_FILE_FORMAT: text' in result.stdout

    # specified global config file does not exist
    env = {ImpalaShellClass.GLOBAL_CONFIG_FILE: '/does_not_exist'}
    run_impala_shell_cmd(vector, args, env=env, expect_success=False)

  def test_config_file(self, vector):
    """Test the optional configuration file."""
    # Positive tests
    args = ['--config_file=%s/good_impalarc' % QUERY_FILE_PATH]
    result = run_impala_shell_cmd(vector, args)
    assert 'WARNING:' not in result.stderr, \
      "A valid config file should not trigger any warning: {0}".format(result.stderr)
    assert 'Query: select 1' in result.stderr
    # override option in config file through command line
    args = ['--config_file={0}/good_impalarc'.format(QUERY_FILE_PATH), '--query=select 2']
    result = run_impala_shell_cmd(vector, args)
    assert 'Query: select 2' in result.stderr

    # IMPALA-8317: Add support for list-type, i.e. action=append in config file.
    args = ['--config_file={0}/good_impalarc'.format(QUERY_FILE_PATH),
            """--query=select '${VAR:msg1}'; select '${VAR:msg2}';
            select '${VAR:msg3}'; select '${VAR:msg4}'; set"""]
    result = run_impala_shell_cmd(vector, args)
    assert 'Query: select \'hello\'' in result.stderr
    assert 'Query: select \'world\'' in result.stderr
    assert 'Query: select \'foo\'' in result.stderr
    assert 'Query: select \'bar\'' in result.stderr
    assert 'DEFAULT_FILE_FORMAT: parquet' in result.stdout

    # Override the variables in the config file with the ones passed via --var.
    args = ['--config_file={0}/good_impalarc'.format(QUERY_FILE_PATH), '--var=msg1=foo',
            '--var=msg2=bar', """--query=select '${VAR:msg1}'; select '${VAR:msg2}'"""]
    result = run_impala_shell_cmd(vector, args)
    assert 'Query: select \'foo\'' in result.stderr
    assert 'Query: select \'bar\'' in result.stderr

    # Negative Tests
    # specified config file does not exist
    args = ['--config_file=%s/does_not_exist' % QUERY_FILE_PATH]
    run_impala_shell_cmd(vector, args, expect_success=False)
    # bad formatting of config file
    args = ['--config_file=%s/bad_impalarc' % QUERY_FILE_PATH]
    run_impala_shell_cmd(vector, args, expect_success=False)

    # Testing config file related warning and error messages
    args = ['--config_file=%s/impalarc_with_warnings' % QUERY_FILE_PATH]
    result = run_impala_shell_cmd(
        vector, args, expect_success=True, wait_until_connected=False)
    assert "WARNING: Option 'config_file' can be only set from shell." in result.stderr
    err_msg = ("WARNING: Unable to read configuration file correctly. "
               "Ignoring unrecognized config option: 'invalid_option'\n")
    assert  err_msg in result.stderr

    args = ['--config_file=%s/impalarc_with_error' % QUERY_FILE_PATH]
    result = run_impala_shell_cmd(vector, args, expect_success=False)
    err_msg = ("Unexpected value in configuration file. "
               "'maybe' is not a valid value for a boolean option.")
    assert  err_msg in result.stderr

  def test_execute_queries_from_stdin(self, vector):
    """Test that queries get executed correctly when STDIN is given as the sql file."""
    args = ['-f', '-', '--quiet', '-B']
    query_file = "{0}/test_file_comments.sql".format(QUERY_FILE_PATH)
    query_file_handle = None
    try:
      query_file_handle = open(query_file, 'r')
      query = query_file_handle.read()
      query_file_handle.close()
    except Exception, e:
      assert query_file_handle is not None, "Exception %s: Could not find query file" % e
    result = run_impala_shell_cmd(vector, args, expect_success=True, stdin_input=query)
    output = result.stdout

    args = ['-f', '{0}/test_file_no_comments.sql'.format(QUERY_FILE_PATH), '--quiet',
            '-B']
    result = run_impala_shell_cmd(vector, args)
    assert output == result.stdout, "Queries from STDIN not parsed correctly."

  def test_allow_creds_in_clear(self, vector):
    args = ['-l']
    result = run_impala_shell_cmd(vector, args, expect_success=False)
    err_msg = ("LDAP credentials may not be sent over insecure connections. "
               "Enable SSL or set --auth_creds_ok_in_clear")

    assert err_msg in result.stderr

    # TODO: Without an Impala daemon running LDAP authentication, we can't test if
    # --auth_creds_ok_in_clear works when correctly set.

  def test_ldap_password_from_shell(self, vector):
    args = ['-l', '--auth_creds_ok_in_clear']
    result = run_impala_shell_cmd(vector, args + ['--ldap_password_cmd=cmddoesntexist'],
                                  expect_success=False)
    assert ("Error retrieving LDAP password (command was: 'cmddoesntexist', exception "
            "was: '[Errno 2] No such file or directory')") in result.stderr
    result = run_impala_shell_cmd(
        vector, args + ['--ldap_password_cmd=cat filedoesntexist'], expect_success=False)
    assert ("Error retrieving LDAP password (command was 'cat filedoesntexist', error "
            "was: 'cat: filedoesntexist: No such file or directory')") in result.stderr

    # TODO: Without an Impala daemon with LDAP authentication enabled, we can't test the
    # positive case where the password is correct.

  def test_var_substitution(self, vector):
    args = ['--var=foo=123', '--var=BAR=456', '--delimited', '--output_delimiter= ',
            '-c', '-f', os.path.join(QUERY_FILE_PATH, 'test_var_substitution.sql')]
    result = run_impala_shell_cmd(vector, args, expect_success=True)
    assert_var_substitution(result)
    args = ['--var=foo']
    result = run_impala_shell_cmd(vector, args, expect_success=False)
    assert ("Error: Could not parse key-value \"foo\". It must follow the pattern "
             "\"KEY=VALUE\".") in result.stderr

    # IMPALA-7673: Test that variable substitution in command line can accept values
    # from other variables just like the one in interactive shell.
    result = run_impala_shell_cmd(vector, [
      '--var=msg1=1', '--var=msg2=${var:msg1}2', '--var=msg3=${var:msg1}${var:msg2}',
      '--query=select ${var:msg3}'])
    self._validate_shell_messages(result.stderr, ['112', 'Fetched 1 row(s)'],
                                  should_exist=True)

    # Test with an escaped variable.
    result = run_impala_shell_cmd(vector, ['--var=msg1=1', '--var=msg2=${var:msg1}2',
                                           '--var=msg3=\${var:msg1}${var:msg2}',
                                           "--query=select '${var:msg3}'"])
    self._validate_shell_messages(result.stderr, ['${var:msg1}12', 'Fetched 1 row(s)'],
                                  should_exist=True)

    # Referencing a non-existent variable will result in an error.
    result = run_impala_shell_cmd(vector, [
        '--var=msg1=1', '--var=msg2=${var:doesnotexist}2',
        '--var=msg3=\${var:msg1}${var:msg2}', "--query=select '${var:msg3}'"],
        expect_success=False)
    self._validate_shell_messages(result.stderr,
                                  ['Error: Unknown variable DOESNOTEXIST',
                                   'Could not execute command: select \'${var:msg3}\''],
                                  should_exist=True)

  # Checks if 'messages' exists/does not exist in 'result_stderr' based on the value of
  # 'should_exist'
  def _validate_shell_messages(self, result_stderr, messages, should_exist=True):
    for msg in messages:
      if should_exist:
        assert msg in result_stderr, result_stderr
      else:
        assert msg not in result_stderr, result_stderr

  def test_query_time_and_link_message(self, vector, unique_database):
    shell_messages = ["Query submitted at: ", "(Coordinator: ",
        "Query progress can be monitored at: "]
    # CREATE statements should not print query time and webserver address.
    results = run_impala_shell_cmd(
        vector, ['--query=create table %s.shell_msg_test (id int)' % unique_database])
    self._validate_shell_messages(results.stderr, shell_messages, should_exist=False)

    # SELECT, INSERT, CTAS, CTE queries should print the query time message and webserver
    # address.
    results = run_impala_shell_cmd(
        vector, ['--query=insert into %s.shell_msg_test values (1)' % unique_database])
    self._validate_shell_messages(results.stderr, shell_messages, should_exist=True)
    results = run_impala_shell_cmd(
        vector, ['--query=select * from %s.shell_msg_test' % unique_database])
    self._validate_shell_messages(results.stderr, shell_messages, should_exist=True)
    results = run_impala_shell_cmd(vector, [
        '--query=create table %s.shell_msg_ctas_test as \
        select * from %s.shell_msg_test' % (unique_database, unique_database)])
    self._validate_shell_messages(results.stderr, shell_messages, should_exist=True)
    results = run_impala_shell_cmd(vector, [
        '--query=create table {db}.shell_msg_cte_test(i int); '
        'with abc as (select 1) '
        'insert overwrite {db}.shell_msg_cte_test '
        'select * from {db}.shell_msg_cte_test;'.format(db=unique_database)])
    self._validate_shell_messages(results.stderr, shell_messages, should_exist=True)

    # DROP statements should not print query time and webserver address.
    results = run_impala_shell_cmd(
        vector, ['--query=drop table %s.shell_msg_test' % unique_database])
    self._validate_shell_messages(results.stderr, shell_messages, should_exist=False)
    run_impala_shell_cmd(
        vector, ['--query=drop table %s.shell_msg_ctas_test' % unique_database])

    # Simple queries should not print query time and webserver address.
    results = run_impala_shell_cmd(vector, ['--query=use default'])
    self._validate_shell_messages(results.stderr, shell_messages, should_exist=False)
    results = run_impala_shell_cmd(vector, ['--query=show tables'])
    self._validate_shell_messages(results.stderr, shell_messages, should_exist=False)

  def test_insert_status(self, vector, unique_database):
    run_impala_shell_cmd(
        vector, ['--query=create table %s.insert_test (id int)' % unique_database])
    results = run_impala_shell_cmd(
        vector, ['--query=insert into %s.insert_test values (1)' % unique_database])
    assert "Modified 1 row(s)" in results.stderr

  def _validate_dml_stmt(self, vector, stmt, expected_rows_modified, expected_row_errors):
    results = run_impala_shell_cmd(vector, ['--query=%s' % stmt])
    expected_output = "Modified %d row(s), %d row error(s)" %\
        (expected_rows_modified, expected_row_errors)
    assert expected_output in results.stderr, results.stderr

  @SkipIf.kudu_not_supported
  def test_kudu_dml_reporting(self, vector, unique_database):
    db = unique_database
    run_impala_shell_cmd(vector, [
        '--query=create table %s.dml_test (id int primary key, '
        'age int null) partition by hash(id) partitions 2 stored as kudu' % db])

    self._validate_dml_stmt(
        vector, "insert into %s.dml_test (id) values (7), (7)" % db, 1, 1)
    self._validate_dml_stmt(vector, "insert into %s.dml_test (id) values (7)" % db, 0, 1)
    self._validate_dml_stmt(
        vector, "upsert into %s.dml_test (id) values (7), (7)" % db, 2, 0)
    self._validate_dml_stmt(
        vector, "update %s.dml_test set age = 1 where id = 7" % db, 1, 0)
    self._validate_dml_stmt(vector, "delete from %s.dml_test where id = 7" % db, 1, 0)

    # UPDATE/DELETE where there are no matching rows; there are no errors because the
    # scan produced no rows.
    self._validate_dml_stmt(
        vector, "update %s.dml_test set age = 1 where id = 8" % db, 0, 0)
    self._validate_dml_stmt(vector, "delete from %s.dml_test where id = 7" % db, 0, 0)

    # WITH clauses, only apply to INSERT and UPSERT
    self._validate_dml_stmt(vector,
        "with y as (values(7)) insert into %s.dml_test (id) select * from y" % db, 1, 0)
    self._validate_dml_stmt(vector,
        "with y as (values(7)) insert into %s.dml_test (id) select * from y" % db, 0, 1)
    self._validate_dml_stmt(vector,
        "with y as (values(7)) upsert into %s.dml_test (id) select * from y" % db, 1, 0)

  def test_missing_query_file(self, vector):
    result = run_impala_shell_cmd(vector, ['-f', 'nonexistent.sql'], expect_success=False)
    assert "Could not open file 'nonexistent.sql'" in result.stderr

  def _validate_expected_socket_connected(self, vector, args, sock):
    # Building an one-off shell command instead of using Util::ImpalaShell since we need
    # to customize the impala daemon socket.
    protocol = vector.get_value("protocol")
    shell_cmd = [IMPALA_SHELL_EXECUTABLE, "--protocol={0}".format(protocol)]
    if protocol == 'beeswax':
      expected_output = "get_default_configuration"
    else:
      expected_output = "OpenSession"
    with open(os.devnull, 'w') as devnull:
      try:
        connection = None
        impala_shell = Popen(shell_cmd + args, stdout=devnull, stderr=devnull)
        connection, client_address = sock.accept()
        data = connection.recv(1024)
        assert expected_output in data
      finally:
        if impala_shell.poll() is None:
          impala_shell.kill()
        if connection is not None:
          connection.close()

  def test_socket_opening(self, vector):
    ''' Tests that impala-shell will always open a socket against
    the host[:port] specified by the -i option with or without the
    -b option '''
    try:
      socket.setdefaulttimeout(10)
      s = socket.socket()
      s.bind(("",0))
      s.listen(1)
      test_impalad_port = s.getsockname()[1]
      load_balancer_fqdn = "my-load-balancer.local"
      args1 = ["-i", "localhost:{0}".format(test_impalad_port)]
      args2 = args1 + ["-b", load_balancer_fqdn]

      # Verify that impala-shell tries to create a socket against the host:port
      # combination specified by -i when -b is not used
      self._validate_expected_socket_connected(vector, args1, s)
      # Verify that impala-shell tries to create a socket against the host:port
      # combination specified by -i when -b is used
      self._validate_expected_socket_connected(vector, args2, s)
    finally:
      s.close()

  def test_malformed_query(self, vector):
    """Test that malformed queries are handled by the commandline shell"""
    args = ['-q', 'with foo as (select bar from temp where temp.a=\'"']
    result = run_impala_shell_cmd(vector, args, expect_success=False)
    assert "Encountered: EOF" in result.stderr
    args = ['-q', 'with v as (select 1) \;"']
    result = run_impala_shell_cmd(vector, args, expect_success=False)
    assert "Encountered: Unexpected character" in result.stderr

  def test_large_sql(self, vector, unique_database):
    # In this test, we are only interested in the performance of Impala shell and not
    # the performance of Impala in general. So, this test will execute a large query
    # from a non-existent table since this will make the query execution time negligible.
    sql_file, sql_path = tempfile.mkstemp()
    num_cols = 10000
    os.write(sql_file, "select \n")
    for i in xrange(num_cols):
      if i < num_cols:
        os.write(sql_file, "col_{0} as a{1},\n".format(i, i))
        os.write(sql_file, "col_{0} as b{1},\n".format(i, i))
        os.write(sql_file, "col_{0} as c{1}{2}\n".format(
            i, i, "," if i < num_cols - 1 else ""))
    os.write(sql_file, "from non_existence_large_table;")
    os.close(sql_file)

    try:
      args = ['-q', '-f', sql_path, '-d', unique_database]
      start_time = time()
      run_impala_shell_cmd(vector, args, expect_success=False)
      end_time = time()
      time_limit_s = 10
      actual_time_s = end_time - start_time
      assert actual_time_s <= time_limit_s, (
          "It took {0} seconds to execute the query. Time limit is {1} seconds.".format(
              actual_time_s, time_limit_s))
    finally:
      os.remove(sql_path)

  @pytest.mark.skipif(ImpalaTestClusterProperties.get_instance().is_remote_cluster(),
                      reason='Test assumes a minicluster.')
  def test_default_timezone(self, vector):
    """Test that the default TIMEZONE query option is a valid timezone.

       It would be nice to check that the default timezone is the system's timezone,
       but doing this reliably on different Linux distributions is quite hard.

       We skip this test on non-local clusters because the result_set from the
       cluster is platform specific, but there's no guarantee the local machine
       is the same OS, and the assert fails if there's a mismatch.
    """
    result_set = run_impala_shell_cmd(vector, ['-q', 'set;'])
    tzname = find_query_option("TIMEZONE", result_set.stdout)
    assert os.path.isfile("/usr/share/zoneinfo/" + tzname)

  def test_find_query_option(self, vector):
    """Test utility function find_query_option()."""
    test_input = """
        not_an_option
        default: [default]
        non_default: non_default
        has_space: has space
        duplicate: d
        duplicate: d
        empty: """
    assert find_query_option("default", test_input) == "default"
    assert find_query_option("non_default", test_input) == "non_default"
    assert find_query_option("has_space", test_input) == "has space"
    assert find_query_option("empty", test_input) == ""
    with pytest.raises(AssertionError):
      find_query_option("duplicate", test_input)
    with pytest.raises(AssertionError):
      find_query_option("not_an_option", test_input)

  def test_impala_shell_timeout(self, vector):
    """Tests that impala shell times out during connect.
       This creates a random listening socket and we try to connect to this
       socket through the impala-shell. The impala-shell should timeout and not hang
       indefinitely while connecting
    """
    with closing(socket.socket()) as s:
      s.bind(("", 0))
      # maximum number of queued connections on this socket is 1.
      s.listen(1)
      test_port = s.getsockname()[1]
      args = ['-q', 'select foo; select bar;', '--ssl', '-t', '2000', '-i',
              'localhost:%d' % (test_port)]
      run_impala_shell_cmd(vector, args, expect_success=False)

  def test_client_identifier(self, vector):
    """Confirms that a version string is passed along."""
    args = ['--query', 'select 0; profile']
    result = run_impala_shell_cmd(vector, args)
    assert 'client_identifier=impala shell' in result.stdout.lower()

  def test_default_port(self, vector):
    """Ensure that we can run when -i does not specify a port."""
    args = ['-q', DEFAULT_QUERY, '-i', IMPALAD_HS2_HOST_PORT.split(":")[0]]
    run_impala_shell_cmd(vector, args)

  def test_type_formatting(self, vector):
    """Test formatting of data types that should be identical between HS2 and beeswax,
    i.e. excluding floating point values which are tested in test_float_formatting."""
    # Query covers bool, all int types, string, timestamp, date, various decimal types,
    # CHAR (including a value with padding) and VARCHAR.
    query = """select bool_col, tinyint_col, smallint_col, int_col, bigint_col,
                      string_col, timestamp_col, date_col, dt.c1, dt.c2, dt.c3,
                      ct.cs, ct.vc
               from functional.alltypestiny att, functional.date_tbl dtbl,
                    functional.decimal_tiny dt, functional.chars_tiny ct
               where att.id = 1 and dtbl.id_col = 1 and dt.c1 = 2.2220 and ct.vc = '3ccc'
           """
    result = run_impala_shell_cmd(vector, ['-q', query, '-B'])
    assert("false\t1\t1\t1\t10\t1\t2009-01-01 00:01:00\t0001-12-31\t2.2220\t124.44440"
           "\t0.0\t3aaa \t3ccc" in result.stdout), result.stdout

  def test_float_formatting(self, vector):
    """Test that rounding of float types is as expected. Includes some floating point
    values that can be printed in multiple valid ways. Includes additional expressions
    to confirm that the output of ROUND() is a DOUBLE and that a plain FLOAT is
    formatted correctly."""
    query = """select typeof(round(cast(8.072 as double), 3)), cast(0.5 as float),
                      round(cast(8.072 as double), 3), round(cast(8 as double), 3),
                      round(cast(8.0719999999 as double),3),
                      round(cast(8.072 as double), 0), round(8.072, 4)"""
    result = run_impala_shell_cmd(vector, ['-q', query, '-B'])
    # Note that Beeswax formats results differently (i.e. the "fix" for IMPALA-266 stopped
    # working for Beeswax at some point.
    protocol = vector.get_value("protocol")
    if protocol == 'hs2':
      assert "DOUBLE\t0.5\t8.072\t8.0\t8.072\t8.0\t8.072" in result.stdout
    else:
      assert protocol == 'beeswax'
      assert ("DOUBLE\t0.5\t8.071999999999999\t8\t8.071999999999999\t8\t8.072"
              in result.stdout)

  def test_bool_display(self, vector):
    """Test that boolean values are displayed correctly."""
    query = "select true a, false b"
    result = run_impala_shell_cmd(vector, ['-q', query])
    assert "| a    | b     |" in result.stdout, result.stdout
    assert "| true | false |" in result.stdout, result.stdout

  def test_null_values(self, vector):
    """Test that null values are displayed correctly."""
    query = """select id, int_col, double_col
               from functional_parquet.alltypesagg where id < 5 order by id"""
    result = run_impala_shell_cmd(vector, ['-q', query, '-B'])
    assert "0\tNULL\tNULL" in result.stdout, result.stdout
    assert "0\tNULL\tNULL" in result.stdout, result.stdout
    assert "1\t1\t10.1" in result.stdout, result.stdout
    assert "2\t2\t20.2" in result.stdout, result.stdout
    assert "3\t3\t30.3" in result.stdout, result.stdout
    assert "4\t4\t40.4" in result.stdout, result.stdout
