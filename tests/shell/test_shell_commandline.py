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

from subprocess import call
from tests.common.impala_service import ImpaladService
from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.skip import SkipIf
from time import sleep
from util import IMPALAD, SHELL_CMD
from util import assert_var_substitution, run_impala_shell_cmd, ImpalaShell

DEFAULT_QUERY = 'select 1'
QUERY_FILE_PATH = os.path.join(os.environ['IMPALA_HOME'], 'tests', 'shell')


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
  table_name = request.node.name
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

  def test_no_args(self):
    args = '-q "%s"' % DEFAULT_QUERY
    run_impala_shell_cmd(args)

  def test_multiple_queries(self):
    queries = ';'.join([DEFAULT_QUERY] * 3)
    args = '-q "%s" -B' % queries
    run_impala_shell_cmd(args)

  def test_multiple_queries_with_escaped_backslash(self):
    """Regression test for string containing an escaped backslash.

    This relies on the patch at thirdparty/patches/sqlparse/0001-....patch.
    """
    run_impala_shell_cmd(r'''-q "select '\\\\'; select '\\'';" -B''')

  def test_default_db(self, empty_table):
    db_name, table_name = empty_table.split('.')
    args = '-d %s -q "describe %s" --quiet' % (db_name, table_name)
    run_impala_shell_cmd(args)
    args = '-q "describe %s"' % table_name
    run_impala_shell_cmd(args, expect_success=False)
    # test keyword parquet is interpreted as an identifier
    # when passed as an argument to -d
    args = '-d parquet'
    result = run_impala_shell_cmd(args)
    assert "Query: use `parquet`" in result.stderr, result.stderr
    # test if backticking is idempotent
    args = "-d '```parquet```'"
    result = run_impala_shell_cmd(args)
    assert "Query: use `parquet`" in result.stderr, result.stderr

  def test_unsecure_message(self):
    results = run_impala_shell_cmd("")
    assert "Starting Impala Shell without Kerberos authentication" in results.stderr

  def test_print_header(self, populated_table):
    args = '--print_header -B --output_delim="," -q "select * from %s"' % populated_table
    result = run_impala_shell_cmd(args)
    result_rows = result.stdout.strip().split('\n')
    assert len(result_rows) == 4
    assert result_rows[0].split(',') == ['i', 's']

    args = '-B --output_delim="," -q "select * from %s"' % populated_table
    result = run_impala_shell_cmd(args)
    result_rows = result.stdout.strip().split('\n')
    assert len(result_rows) == 3

  @pytest.mark.execute_serially
  def test_kerberos_option(self):
    args = "-k"

    # If you have a valid kerberos ticket in your cache, this test fails - so
    # here we set a bogus KRB5CCNAME in the environment so that klist (and other
    # kerberos commands) won't find the normal ticket cache.
    # KERBEROS TODO: add kerberized cluster test case
    os.environ["KRB5CCNAME"] = "/tmp/this/file/hopefully/does/not/exist"

    # The command will fail because we're trying to connect to a kerberized impalad.
    results = run_impala_shell_cmd(args, expect_success=False)
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
    args += " -s foobar"
    results = run_impala_shell_cmd(args, expect_success=False)
    assert "Using service name 'foobar'" in results.stderr

  def test_continue_on_error(self):
    args = '-c -q "select foo; select bar;"'
    run_impala_shell_cmd(args)
    # Should fail
    args = '-q "select foo; select bar;"'
    run_impala_shell_cmd(args, expect_success=False)

  def test_execute_queries_from_file(self):
    args = '-f %s/test_file_comments.sql --quiet -B' % QUERY_FILE_PATH
    result = run_impala_shell_cmd(args)
    output = result.stdout
    args = '-f %s/test_file_no_comments.sql --quiet -B' % QUERY_FILE_PATH
    result = run_impala_shell_cmd(args)
    assert output == result.stdout, "Queries with comments not parsed correctly"

  def test_completed_query_errors(self):
    args = ('-q "set abort_on_error=false;'
            ' select count(*) from functional_seq_snap.bad_seq_snap"')
    result = run_impala_shell_cmd(args)
    assert 'WARNINGS:' in result.stderr
    assert 'Bad synchronization marker' in result.stderr
    assert 'Expected: ' in result.stderr
    assert 'Actual: ' in result.stderr
    assert 'Problem parsing file' in result.stderr

  def test_no_warnings_in_log_with_quiet_mode(self):
    """Regression test for IMPALA-4222."""
    args = ('-q "set abort_on_error=false;'
            ' select count(*) from functional_seq_snap.bad_seq_snap" --quiet')
    result = run_impala_shell_cmd(args)
    assert 'WARNINGS:' not in result.stderr

  def test_removed_query_option(self):
    """Test that removed query options produce warning."""
    result = run_impala_shell_cmd("-q 'set disable_cached_reads=true'",
        expect_success=True)
    assert "Ignoring removed query option: 'disable_cached_reads'" in result.stderr

  def test_output_format(self):
    expected_output = ['1'] * 3
    args = '-q "select 1,1,1" -B --quiet'
    result = run_impala_shell_cmd(args)
    actual_output = [r.strip() for r in result.stdout.split('\t')]
    assert actual_output == expected_output
    result = run_impala_shell_cmd(args + ' --output_delim="|"')
    actual_output = [r.strip() for r in result.stdout.split('|')]
    assert actual_output == expected_output
    result = run_impala_shell_cmd(args + ' --output_delim="||"',
                                  expect_success=False)
    assert "Illegal delimiter" in result.stderr

  def test_do_methods(self, empty_table):
    """Ensure that the do_ methods in the shell work.

    Some of the do_ methods are implicitly tested in other tests, and as part of the
    test setup.
    """
    # explain
    args = '-q "explain select 1"'
    run_impala_shell_cmd(args)
    # show
    args = '-q "show tables"'
    run_impala_shell_cmd(args)
    # with
    args = '-q "with t1 as (select 1) select * from t1"'
    run_impala_shell_cmd(args)
    # set
    # spaces around the = sign
    args = '-q "set batch_size  =   10"'
    run_impala_shell_cmd(args)
    # no spaces around the = sign
    args = '-q "set batch_size=10"'
    run_impala_shell_cmd(args)
    # test query options displayed
    args = '-q "set"'
    result_set = run_impala_shell_cmd(args)
    assert 'MEM_LIMIT: [0]' in result_set.stdout
    # test to check that explain_level is 1
    assert 'EXPLAIN_LEVEL: [1]' in result_set.stdout
    # test to check that configs without defaults show up as []
    assert 'COMPRESSION_CODEC: []' in result_set.stdout
    # test values displayed after setting value
    args = '-q "set mem_limit=1g;set"'
    result_set = run_impala_shell_cmd(args)
    # single list means one instance of mem_limit in displayed output
    assert 'MEM_LIMIT: 1g' in result_set.stdout
    assert 'MEM_LIMIT: [0]' not in result_set.stdout
    # Negative tests for set
    # use : instead of =
    args = '-q "set batch_size:10"'
    run_impala_shell_cmd(args, expect_success=False)
    # use 2 = signs
    args = '-q "set batch_size=10=50"'
    run_impala_shell_cmd(args, expect_success=False)
    # describe and desc should return the same result.
    args = '-q "describe %s" -B' % empty_table
    result_describe = run_impala_shell_cmd(args)
    args = '-q "desc %s" -B' % empty_table
    result_desc = run_impala_shell_cmd(args)
    assert result_describe.stdout == result_desc.stdout

  def test_runtime_profile(self):
    # test summary is in both the profile printed by the
    # -p option and the one printed by the profile command
    args = "-p -q 'select 1; profile;'"
    result_set = run_impala_shell_cmd(args)
    # This regex helps us uniquely identify a profile.
    regex = re.compile("Operator\s+#Hosts\s+Avg\s+Time")
    # We expect two query profiles.
    assert len(re.findall(regex, result_set.stdout)) == 2, \
        "Could not detect two profiles, stdout: %s" % result_set.stdout

  def test_summary(self):
    args = "-q 'select count(*) from functional.alltypes; summary;'"
    result_set = run_impala_shell_cmd(args)
    assert "03:AGGREGATE" in result_set.stdout

    args = "-q 'summary;'"
    result_set = run_impala_shell_cmd(args, expect_success=False)
    assert "Could not retrieve summary for query" in result_set.stderr

    args = "-q 'show tables; summary;'"
    result_set = run_impala_shell_cmd(args)
    assert "Summary not available" in result_set.stderr

    # Test queries without an exchange
    args = "-q 'select 1; summary;'"
    result_set = run_impala_shell_cmd(args)
    assert "00:UNION" in result_set.stdout

  @pytest.mark.execute_serially
  def test_queries_closed(self):
    """Regression test for IMPALA-897."""
    args = '-f %s/test_close_queries.sql --quiet -B' % QUERY_FILE_PATH
    cmd = "%s %s" % (SHELL_CMD, args)
    # Execute the shell command async
    p = ImpalaShell(args)

    impalad_service = ImpaladService(IMPALAD.split(':')[0])
    # The last query in the test SQL script will sleep for 10 seconds, so sleep
    # here for 5 seconds and verify the number of in-flight queries is 1.
    sleep(5)
    assert 1 == impalad_service.get_num_in_flight_queries()
    assert p.get_result().rc == 0
    assert 0 == impalad_service.get_num_in_flight_queries()

  def test_cancellation(self):
    """Test cancellation (Ctrl+C event)."""
    args = '-q "select sleep(10000)"'
    p = ImpalaShell(args)
    sleep(3)
    os.kill(p.pid(), signal.SIGINT)
    result = p.get_result()

    assert "Cancelling Query" in result.stderr, result.stderr

  @pytest.mark.execute_serially
  def test_query_cancellation_during_fetch(self):
    """IMPALA-1144: Test cancellation (CTRL+C) while results are being
    fetched"""
    pytest.skip("""Skipping as it occasionally gets stuck in Jenkins builds
                resulting the build to timeout.""")
    # A select query where fetch takes several seconds
    stmt = "with v as (values (1 as x), (2), (3), (4)) " + \
        "select * from v, v v2, v v3, v v4, v v5, v v6, v v7, v v8, " + \
        "v v9, v v10, v v11"
    # Kill happens when the results are being fetched
    self.run_and_verify_query_cancellation_test(stmt, "FINISHED")

  @pytest.mark.execute_serially
  def test_query_cancellation_during_wait_to_finish(self):
    """IMPALA-1144: Test cancellation (CTRL+C) while the query is in the
    wait_to_finish state"""
    # A select where wait_to_finish takes several seconds
    stmt = "select * from tpch.customer c1, tpch.customer c2, " + \
           "tpch.customer c3 order by c1.c_name"
    # Kill happens in wait_to_finish state
    self.run_and_verify_query_cancellation_test(stmt, "RUNNING")

  def wait_for_query_state(self, stmt, state, max_retry=15):
    """Checks the in flight queries on Impala debug page. Polls the state of
    the query statement from parameter every second until the query gets to
    a state given via parameter or a maximum retry count is reached.
    Restriction: Only works if there is only one in flight query."""
    impalad_service = ImpaladService(IMPALAD.split(':')[0])
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

  def run_and_verify_query_cancellation_test(self, stmt, cancel_at_state):
    """Starts the execution of the received query, waits until the query
    execution in fact starts and then cancels it. Expects the query
    cancellation to succeed."""
    args = "-q \"" + stmt + ";\""
    p = ImpalaShell(args)

    self.wait_for_query_state(stmt, cancel_at_state)

    os.kill(p.pid(), signal.SIGINT)
    result = p.get_result()
    assert "Cancelling Query" in result.stderr
    assert "Invalid query handle" not in result.stderr

  def test_get_log_once(self, empty_table):
    """Test that get_log() is always called exactly once."""
    # Query with fetch
    args = '-q "select * from functional.alltypeserror"'
    result = run_impala_shell_cmd(args)
    assert result.stderr.count('WARNINGS') == 1

    # Insert query (doesn't fetch)
    drop_args = '-q "drop table if exists %s"' % empty_table
    run_impala_shell_cmd(drop_args)

    args = '-q "create table %s like functional.alltypeserror"' % empty_table
    run_impala_shell_cmd(args)

    args = '-q "insert overwrite %s partition(year, month)' \
           'select * from functional.alltypeserror"' % empty_table
    result = run_impala_shell_cmd(args)

    assert result.stderr.count('WARNINGS') == 1

  def test_international_characters(self):
    """Sanity test to ensure that the shell can read international characters."""
    russian_chars = (u"А, Б, В, Г, Д, Е, Ё, Ж, З, И, Й, К, Л, М, Н, О, П, Р,"
                     u"С, Т, У, Ф, Х, Ц,Ч, Ш, Щ, Ъ, Ы, Ь, Э, Ю, Я")
    args = """-B -q "select '%s'" """ % russian_chars
    result = run_impala_shell_cmd(args.encode('utf-8'))
    assert 'UnicodeDecodeError' not in result.stderr
    assert russian_chars.encode('utf-8') in result.stdout

  @pytest.mark.execute_serially  # This tests invalidates metadata, and must run serially
  def test_config_file(self):
    """Test the optional configuration file."""
    # Positive tests
    args = '--config_file=%s/good_impalarc' % QUERY_FILE_PATH
    result = run_impala_shell_cmd(args)
    assert 'Query: select 1' in result.stderr

    # override option in config file through command line
    args = '--config_file=%s/good_impalarc --query="select 2"' % QUERY_FILE_PATH
    result = run_impala_shell_cmd(args)
    assert 'Query: select 2' in result.stderr

    # Negative Tests
    # specified config file does not exist
    args = '--config_file=%s/does_not_exist' % QUERY_FILE_PATH
    run_impala_shell_cmd(args, expect_success=False)
    # bad formatting of config file
    args = '--config_file=%s/bad_impalarc' % QUERY_FILE_PATH
    run_impala_shell_cmd(args, expect_success=False)

    # Testing config file related warning and error messages
    args = '--config_file=%s/impalarc_with_warnings' % QUERY_FILE_PATH
    result = run_impala_shell_cmd(args, expect_success=True)
    assert "WARNING: Option 'config_file' can be only set from shell." in result.stderr
    err_msg = ("WARNING: Unable to read configuration file correctly. "
               "Ignoring unrecognized config option: 'invalid_option'\n")
    assert  err_msg in result.stderr

    args = '--config_file=%s/impalarc_with_error' % QUERY_FILE_PATH
    result = run_impala_shell_cmd(args, expect_success=False)
    err_msg = ("Unexpected value in configuration file. "
               "'maybe' is not a valid value for a boolean option.")
    assert  err_msg in result.stderr

  def test_execute_queries_from_stdin(self):
    """Test that queries get executed correctly when STDIN is given as the sql file."""
    args = '-f - --quiet -B'
    query_file = "%s/test_file_comments.sql" % QUERY_FILE_PATH
    query_file_handle = None
    try:
      query_file_handle = open(query_file, 'r')
      query = query_file_handle.read()
      query_file_handle.close()
    except Exception, e:
      assert query_file_handle is not None, "Exception %s: Could not find query file" % e
    result = run_impala_shell_cmd(args, expect_success=True, stdin_input=query)
    output = result.stdout

    args = '-f %s/test_file_no_comments.sql --quiet -B' % QUERY_FILE_PATH
    result = run_impala_shell_cmd(args)
    assert output == result.stdout, "Queries from STDIN not parsed correctly."

  def test_allow_creds_in_clear(self):
    args = '-l'
    result = run_impala_shell_cmd(args, expect_success=False)
    err_msg = ("LDAP credentials may not be sent over insecure connections. "
               "Enable SSL or set --auth_creds_ok_in_clear")

    assert err_msg in result.stderr

    # TODO: Without an Impala daemon running LDAP authentication, we can't test if
    # --auth_creds_ok_in_clear works when correctly set.

  def test_ldap_password_from_shell(self):
    args = "-l --auth_creds_ok_in_clear --ldap_password_cmd='%s'"
    result = run_impala_shell_cmd(args % 'cmddoesntexist', expect_success=False)
    assert ("Error retrieving LDAP password (command was: 'cmddoesntexist', exception "
            "was: '[Errno 2] No such file or directory')") in result.stderr
    result = run_impala_shell_cmd(args % 'cat filedoesntexist', expect_success=False)
    assert ("Error retrieving LDAP password (command was 'cat filedoesntexist', error "
            "was: 'cat: filedoesntexist: No such file or directory')") in result.stderr

    # TODO: Without an Impala daemon with LDAP authentication enabled, we can't test the
    # positive case where the password is correct.

  def test_var_substitution(self):
    args = '--var=foo=123 --var=BAR=456 --delimited --output_delimiter=" " -c -f %s' \
           % (os.path.join(QUERY_FILE_PATH, 'test_var_substitution.sql'))
    result = run_impala_shell_cmd(args, expect_success=True)
    assert_var_substitution(result)
    args = '--var=foo'
    result = run_impala_shell_cmd(args, expect_success=False)
    assert ("Error: Could not parse key-value \"foo\". It must follow the pattern "
             "\"KEY=VALUE\".") in result.stderr

  # Checks if 'messages' exists/does not exist in 'result_stderr' based on the value of
  # 'should_exist'
  def _validate_shell_messages(self, result_stderr, messages, should_exist=True):
    for msg in messages:
      if should_exist:
        assert msg in result_stderr
      else:
        assert msg not in result_stderr

  def test_query_time_and_link_message(self, unique_database):
    shell_messages = ["Query submitted at: ", "(Coordinator: ",
        "Query progress can be monitored at: "]
    # CREATE statements should not print query time and webserver address.
    results = run_impala_shell_cmd('--query="create table %s.shell_msg_test (id int)"' %
        unique_database)
    self._validate_shell_messages(results.stderr, shell_messages, should_exist=False)

    # SELECT, INSERT, CTAS, CTE queries should print the query time message and webserver
    # address.
    results = run_impala_shell_cmd('--query="insert into %s.shell_msg_test values (1)"' %
        unique_database)
    self._validate_shell_messages(results.stderr, shell_messages, should_exist=True)
    results = run_impala_shell_cmd('--query="select * from %s.shell_msg_test"' %
        unique_database)
    self._validate_shell_messages(results.stderr, shell_messages, should_exist=True)
    results = run_impala_shell_cmd('--query="create table %s.shell_msg_ctas_test as \
        select * from %s.shell_msg_test"' % (unique_database, unique_database))
    self._validate_shell_messages(results.stderr, shell_messages, should_exist=True)
    results = run_impala_shell_cmd('--query="create table %s.shell_msg_cte_test(i int); '
                                   'with abc as (select 1) '
                                   'insert overwrite %s.shell_msg_cte_test '
                                   'select * from %s.shell_msg_cte_test;"' %
                                   (unique_database, unique_database, unique_database))
    self._validate_shell_messages(results.stderr, shell_messages, should_exist=True)

    # DROP statements should not print query time and webserver address.
    results = run_impala_shell_cmd('--query="drop table %s.shell_msg_test"' %
        unique_database)
    self._validate_shell_messages(results.stderr, shell_messages, should_exist=False)
    run_impala_shell_cmd('--query="drop table %s.shell_msg_ctas_test"' %
        unique_database)

    # Simple queries should not print query time and webserver address.
    results = run_impala_shell_cmd('--query="use default"')
    self._validate_shell_messages(results.stderr, shell_messages, should_exist=False)
    results = run_impala_shell_cmd('--query="show tables"')
    self._validate_shell_messages(results.stderr, shell_messages, should_exist=False)

  def test_insert_status(self, unique_database):
    run_impala_shell_cmd('--query="create table %s.insert_test (id int)"' %
        unique_database)
    results = run_impala_shell_cmd('--query="insert into %s.insert_test values (1)"' %
        unique_database)
    assert "Modified 1 row(s)" in results.stderr

  def _validate_dml_stmt(self, stmt, expected_rows_modified, expected_row_errors):
    results = run_impala_shell_cmd('--query="%s"' % (stmt))
    expected_output = "Modified %d row(s), %d row error(s)" %\
        (expected_rows_modified, expected_row_errors)
    assert expected_output in results.stderr

  @SkipIf.kudu_not_supported
  def test_kudu_dml_reporting(self, unique_database):
    db = unique_database
    run_impala_shell_cmd('--query="create table %s.dml_test (id int primary key, '\
        'age int null) partition by hash(id) partitions 2 stored as kudu"' % db)

    self._validate_dml_stmt("insert into %s.dml_test (id) values (7), (7)" % db, 1, 1)
    self._validate_dml_stmt("insert into %s.dml_test (id) values (7)" % db, 0, 1)
    self._validate_dml_stmt("upsert into %s.dml_test (id) values (7), (7)" % db, 2, 0)
    self._validate_dml_stmt("update %s.dml_test set age = 1 where id = 7" % db, 1, 0)
    self._validate_dml_stmt("delete from %s.dml_test where id = 7" % db, 1, 0)

    # UPDATE/DELETE where there are no matching rows; there are no errors because the
    # scan produced no rows.
    self._validate_dml_stmt("update %s.dml_test set age = 1 where id = 8" % db, 0, 0)
    self._validate_dml_stmt("delete from %s.dml_test where id = 7" % db, 0, 0)

    # WITH clauses, only apply to INSERT and UPSERT
    self._validate_dml_stmt(\
        "with y as (values(7)) insert into %s.dml_test (id) select * from y" % db, 1, 0)
    self._validate_dml_stmt(\
        "with y as (values(7)) insert into %s.dml_test (id) select * from y" % db, 0, 1)
    self._validate_dml_stmt(\
        "with y as (values(7)) upsert into %s.dml_test (id) select * from y" % db, 1, 0)

  def test_missing_query_file(self):
    result = run_impala_shell_cmd('-f nonexistent.sql', expect_success=False)
    assert "Could not open file 'nonexistent.sql'" in result.stderr
