# encoding=utf-8
# Copyright 2012 Cloudera Inc.
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

import os
import pytest
import re
import shlex
import signal

from impala_shell_results import get_shell_cmd_result
from subprocess import Popen, PIPE, call
from tests.common.impala_cluster import ImpalaCluster
from time import sleep

SHELL_CMD = "%s/bin/impala-shell.sh" % os.environ['IMPALA_HOME']
DEFAULT_QUERY = 'select 1'
TEST_DB = "tmp_shell"
TEST_TBL = "tbl1"
QUERY_FILE_PATH = os.path.join(os.environ['IMPALA_HOME'], 'tests', 'shell')

class TestImpalaShell(object):
  """A set of sanity tests for the Impala shell commandline parameters.

  The tests need to maintain Python 2.4 compatibility as a sub-goal of having
  shell tests is to ensure that it's not broken in systems running Python 2.4.
  The tests need a running impalad instance in order to execute queries.

  TODO:
     * Test individual modules.
     * Add a test for a kerberized impala.
  """

  @classmethod
  def setup_class(cls):
    cls.__create_shell_data()

  @classmethod
  def teardown_class(cls):
    run_impala_shell_cmd('-q "drop table if exists %s.%s"' % (TEST_DB, TEST_TBL))
    run_impala_shell_cmd('-q "drop database if exists %s"' % TEST_DB)

  @classmethod
  def __create_shell_data(cls):
    # Create a temporary table and populate it with test data.
    stmts = ['create database if not exists %s' % TEST_DB,
             'create table if not exists %s.%s (i integer, s string)' % (TEST_DB,
                                                                         TEST_TBL),
             "insert into %s.%s values (1, 'a'),(1, 'b'),(3, 'b')" % (TEST_DB, TEST_TBL)
            ]
    args = '-q "%s"' % (';'.join(stmts))
    run_impala_shell_cmd(args)

  @pytest.mark.execute_serially
  def test_no_args(self):
    args = '-q "%s"' % DEFAULT_QUERY
    run_impala_shell_cmd(args)

  @pytest.mark.execute_serially
  def test_multiple_queries(self):
    queries = ';'.join([DEFAULT_QUERY] * 3)
    args = '-q "%s" -B' % queries
    run_impala_shell_cmd(args)

  @pytest.mark.execute_serially
  def test_multiple_queries_with_escaped_backslash(self):
    # Regression test for string containing an escaped backslash. This relies on the
    # patch at thirdparty/patches/sqlparse/0001-....patch.
    run_impala_shell_cmd(r'''-q "select '\\\\'; select '\\'';" -B''')

  @pytest.mark.execute_serially
  def test_default_db(self):
    args = '-d %s -q "describe %s" --quiet' % (TEST_DB, TEST_TBL)
    run_impala_shell_cmd(args)
    args = '-q "describe %s"' % TEST_TBL
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

  @pytest.mark.execute_serially
  def test_refresh_on_connect(self):
    args = '-r -q "%s"' % DEFAULT_QUERY
    result = run_impala_shell_cmd(args)
    assert 'Invalidating Metadata' in result.stderr, result.stderr

  @pytest.mark.execute_serially
  def test_unsecure_message(self):
    results = run_impala_shell_cmd("")
    assert "Starting Impala Shell without Kerberos authentication" in results.stderr

  @pytest.mark.execute_serially
  def test_print_header(self):
    args = '--print_header -B --output_delim="," -q "select * from %s.%s"' % (TEST_DB,
                                                                              TEST_TBL)
    result = run_impala_shell_cmd(args)
    result_rows = result.stdout.strip().split('\n')
    assert len(result_rows) == 4
    assert result_rows[0].split(',') == ['i', 's']
    args = '-B --output_delim="," -q "select * from %s.%s"' % (TEST_DB, TEST_TBL)
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

  @pytest.mark.execute_serially
  def test_continue_on_error(self):
    args = '-c -q "select foo; select bar;"'
    run_impala_shell_cmd(args)
    # Should fail
    args = '-q "select foo; select bar;"'
    run_impala_shell_cmd(args, expect_success=False)

  @pytest.mark.execute_serially
  def test_execute_queries_from_file(self):
    args = '-f %s/test_file_comments.sql --quiet -B' % QUERY_FILE_PATH
    result = run_impala_shell_cmd(args)
    output = result.stdout
    args = '-f %s/test_file_no_comments.sql --quiet -B' % QUERY_FILE_PATH
    result = run_impala_shell_cmd(args)
    assert output == result.stdout, "Queries with comments not parsed correctly"

  @pytest.mark.execute_serially
  def test_completed_query_errors(self):
    args = ('-q "set abort_on_error=false;'
        ' select count(*) from functional_seq_snap.bad_seq_snap"')
    result = run_impala_shell_cmd(args)
    assert 'WARNINGS:' in result.stderr
    assert 'Bad synchronization marker' in result.stderr
    assert 'Expected: ' in result.stderr
    assert 'Actual: ' in result.stderr
    assert 'Problem parsing file' in result.stderr

    # Regression test for CDH-21036
    # do not print warning log in quiet mode
    args = ('-q "set abort_on_error=false;'
        ' select count(*) from functional_seq_snap.bad_seq_snap" --quiet')
    result = run_impala_shell_cmd(args)
    assert 'WARNINGS:' not in result.stderr

  @pytest.mark.execute_serially
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

  @pytest.mark.execute_serially
  def test_do_methods(self):
    """Ensure that the do_ methods in the shell work.

    Some of the do_ methods are implicitly tested in other tests, and as part of the test
    setup.
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
    args = '-q "set default_order_by_limit  =   10"'
    run_impala_shell_cmd(args)
    # no spaces around the = sign
    args = '-q "set default_order_by_limit=10"'
    run_impala_shell_cmd(args)
    # test query options displayed
    args = '-q "set"'
    result_set = run_impala_shell_cmd(args)
    assert 'MEM_LIMIT: [0]' in result_set.stdout
    # test to check that explain_level is 1
    assert 'EXPLAIN_LEVEL: [1]' in result_set.stdout
    # test values displayed after setting value
    args = '-q "set mem_limit=1g;set"'
    result_set = run_impala_shell_cmd(args)
    # single list means one instance of mem_limit in displayed output
    assert 'MEM_LIMIT: 1g' in result_set.stdout
    assert 'MEM_LIMIT: [0]' not in result_set.stdout
    # Negative tests for set
    # use : instead of =
    args = '-q "set default_order_by_limit:10"'
    run_impala_shell_cmd(args, expect_success=False)
    # use 2 = signs
    args = '-q "set default_order_by_limit=10=50"'
    run_impala_shell_cmd(args, expect_success=False)
    # describe and desc should return the same result.
    args = '-q "describe %s.%s" -B' % (TEST_DB, TEST_TBL)
    result_describe = run_impala_shell_cmd(args)
    args = '-q "desc %s.%s" -B' % (TEST_DB, TEST_TBL)
    result_desc = run_impala_shell_cmd(args)
    assert result_describe.stdout == result_desc.stdout

  @pytest.mark.execute_serially
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

  @pytest.mark.execute_serially
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
    """Regression test for IMPALA-897"""
    args = '-f %s/test_close_queries.sql --quiet -B' % QUERY_FILE_PATH
    cmd = "%s %s" % (SHELL_CMD, args)
    # Execute the shell command async
    p = Popen(shlex.split(cmd), shell=False, stdout=PIPE, stderr=PIPE)

    impala_cluster = ImpalaCluster()
    impalad = impala_cluster.impalads[0].service
    # The last query in the test SQL script will sleep for 10 seconds, so sleep
    # here for 5 seconds and verify the number of in-flight queries is 1.
    sleep(5)
    assert 1 == impalad.get_num_in_flight_queries()
    assert get_shell_cmd_result(p).rc == 0
    assert 0 == impalad.get_num_in_flight_queries()


  @pytest.mark.execute_serially
  def test_cancellation(self):
    """Test cancellation (Ctrl+C event)"""
    args = '-q "select sleep(10000)"'
    cmd = "%s %s" % (SHELL_CMD, args)

    p = Popen(shlex.split(cmd), stderr=PIPE, stdout=PIPE)
    sleep(3)
    os.kill(p.pid, signal.SIGINT)
    result = get_shell_cmd_result(p)

    assert "Cancelling Query" in result.stderr, result.stderr

  @pytest.mark.execute_serially
  def test_get_log_once(self):
    """Test that get_log() is always called exactly once."""
    pytest.xfail(reason="The shell doesn't fetch all the warning logs.")
    # Query with fetch
    args = '-q "select * from functional.alltypeserror"'
    result = run_impala_shell_cmd(args)
    assert result.stderr.count('WARNINGS') == 1

    # Insert query (doesn't fetch)
    INSERT_TBL = "alltypes_get_log"
    DROP_ARGS = '-q "drop table if exists %s.%s"' % (TEST_DB, INSERT_TBL)
    run_impala_shell_cmd(DROP_ARGS)
    args = '-q "create table %s.%s like functional.alltypeserror"' % (TEST_DB, INSERT_TBL)
    run_impala_shell_cmd(args)
    args = '-q "insert overwrite %s.%s partition(year, month)' \
           'select * from functional.alltypeserror"' % (TEST_DB, INSERT_TBL)
    result = run_impala_shell_cmd(args)
    assert result.stderr.count('WARNINGS') == 1
    run_impala_shell_cmd(DROP_ARGS)

  @pytest.mark.execute_serially
  def test_international_characters(self):
    """Sanity test to ensure that the shell can read international characters."""
    RUSSIAN_CHARS = (u"А, Б, В, Г, Д, Е, Ё, Ж, З, И, Й, К, Л, М, Н, О, П, Р,"
                     u"С, Т, У, Ф, Х, Ц,Ч, Ш, Щ, Ъ, Ы, Ь, Э, Ю, Я")
    args = """-B -q "select '%s'" """ % RUSSIAN_CHARS
    result = run_impala_shell_cmd(args.encode('utf-8'))
    assert 'UnicodeDecodeError' not in result.stderr
    #print result.stdout.encode('utf-8')
    assert RUSSIAN_CHARS.encode('utf-8') in result.stdout

  @pytest.mark.execute_serially
  def test_config_file(self):
      """Test the optional configuration file"""
      # Positive tests
      args = '--config_file=%s/good_impalarc' % QUERY_FILE_PATH
      result = run_impala_shell_cmd(args)
      assert 'Query: select 1' in  result.stderr
      assert 'Invalidating Metadata' in result.stderr
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

  @pytest.mark.execute_serially
  def test_execute_queries_from_stdin(self):
    """ Test that queries get executed correctly when STDIN is given as the sql file """
    args = '-f - --quiet -B'
    query_file = "%s/test_file_comments.sql" % QUERY_FILE_PATH
    query_file_handle = None
    try:
      query_file_handle = open(query_file, 'r')
      query = query_file_handle.read()
      query_file_handle.close()
    except Exception, e:
      assert query_file_handle != None, "Exception %s: Could not find query file" % e
    result = run_impala_shell_cmd(args, expect_success=True, stdin_input=query)
    output = result.stdout

    args = '-f %s/test_file_no_comments.sql --quiet -B' % QUERY_FILE_PATH
    result = run_impala_shell_cmd(args)
    assert output == result.stdout, "Queries from STDIN not parsed correctly."

  @pytest.mark.execute_serially
  def test_allow_creds_in_clear(self):
    args = '-l'
    result = run_impala_shell_cmd(args, expect_success=False)
    assert "LDAP credentials may not be sent over insecure connections. " +\
    "Enable SSL or set --auth_creds_ok_in_clear" in result.stderr

    # TODO: Without an Impala daemon running LDAP authentication, we can't test if
    # --auth_creds_ok_in_clear works when correctly set.

def run_impala_shell_cmd(shell_args, expect_success=True, stdin_input=None):
  """Runs the Impala shell on the commandline.

  'shell_args' is a string which represents the commandline options.
  Returns a ImpalaShellResult.
  """
  cmd = "%s %s" % (SHELL_CMD, shell_args)
  p = Popen(shlex.split(cmd), shell=False, stdout=PIPE, stderr=PIPE, stdin=PIPE)
  result = get_shell_cmd_result(p, stdin_input)
  if expect_success:
    assert result.rc == 0, "Cmd %s was expected to succeed: %s" % (cmd, result.stderr)
  else:
    assert result.rc != 0, "Cmd %s was expected to fail" % cmd
  return result
