#!/usr/bin/env impala-python
# -*- coding: utf-8 -*-
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

from __future__ import absolute_import, division, print_function
from builtins import range
import errno
import getpass
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
from tests.common.test_dimensions import (
  create_client_protocol_dimension, create_client_protocol_strict_dimension,
  create_uncompressed_text_dimension, create_single_exec_option_dimension)
from time import sleep, time
from tests.shell.util import (get_impalad_host_port, assert_var_substitution,
  run_impala_shell_cmd, ImpalaShell, build_shell_env, wait_for_query_state,
  create_impala_shell_executable_dimension, get_impala_shell_executable)
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
  table_name = request.node.function.__name__
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


@pytest.yield_fixture
def tmp_file():
  """
  Test fixture which manages a temporary file
  """
  _, tmp_file = tempfile.mkstemp()
  yield tmp_file
  os.remove(tmp_file)


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
    super(TestImpalaShell, cls).add_test_dimensions()
    # Limit to uncompressed text with default exec options
    cls.ImpalaTestMatrix.add_dimension(
        create_uncompressed_text_dimension(cls.get_workload()))
    cls.ImpalaTestMatrix.add_dimension(create_single_exec_option_dimension())
    # Run with both beeswax and HS2 to ensure that behaviour is the same.
    cls.ImpalaTestMatrix.add_dimension(create_client_protocol_dimension())
    cls.ImpalaTestMatrix.add_dimension(create_client_protocol_strict_dimension())
    cls.ImpalaTestMatrix.add_constraint(lambda v:
        v.get_value('protocol') != 'beeswax' or not v.get_value('strict_hs2_protocol'))
    # Test combination of Python versions and tarball/PyPI
    cls.ImpalaTestMatrix.add_dimension(create_impala_shell_executable_dimension())

  def test_no_args(self, vector):
    args = ['-q', DEFAULT_QUERY]
    run_impala_shell_cmd(vector, args)

  def test_multiple_queries(self, vector):
    queries = ';'.join([DEFAULT_QUERY] * 3)
    args = ['-q', queries, '-B']
    run_impala_shell_cmd(vector, args)

  def test_multiple_queries_with_escaped_backslash(self, vector):
    """Regression test for string containing an escaped backslash.
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
    if vector.get_value('strict_hs2_protocol'):
      pytest.skip("Strict protocol always runs with LDAP.")
    results = run_impala_shell_cmd(vector, [], wait_until_connected=False)
    assert "with no authentication" in results.stderr

  def test_fastbinary_warning_message(self, vector):
    results = run_impala_shell_cmd(vector, [], wait_until_connected=False)
    # Verify that we don't print any message about fastbinary
    # This doesn't check the full error string, because that could change.
    assert "fastbinary" not in results.stderr

  def test_print_header(self, vector, populated_table):
    args = ['--print_header', '-B', '--output_delim=,', '-q',
            'select * from {0}'.format(populated_table)]
    result = run_impala_shell_cmd(vector, args)
    result_rows = result.stdout.strip().split('\n')
    assert len(result_rows) == 4
    headers = result_rows[0].split(',')
    assert headers == ['i', 's'] or \
        headers == ['test_print_header.i', 'test_print_header.s']

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
    assert "with Kerberos authentication" in results.stderr
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
    if vector.get_value('strict_hs2_protocol'):
      pytest.skip("The abort_on_error=false is not supported in strict hs2 mode.")
    args = ['-q', 'set abort_on_error=false;'
            'select count(*) from functional_seq_snap.bad_seq_snap']
    result = run_impala_shell_cmd(vector, args)
    assert 'WARNINGS:' in result.stderr
    assert 'Bad synchronization marker' in result.stderr
    assert 'Expected: ' in result.stderr
    assert 'Actual: ' in result.stderr
    assert 'Problem parsing file' in result.stderr

  def test_completed_query_errors_1(self, vector):
    # strict protocol does not support flag "set abort_on_error" since it
    # connects directly to hs2 and hs2 does not know about this flag.
    if vector.get_value('strict_hs2_protocol'):
      pytest.skip("IMPALA-10827: Test not working with strict hs2 protocol.")
    args = ['-q', 'set abort_on_error=true;'
            'select id from functional_parquet.bad_column_metadata t']
    result = run_impala_shell_cmd(vector, args, expect_success=False)
    assert 'ERROR: Column metadata states there are 11 values, ' in result.stderr
    assert 'but read 10 values from column id.' in result.stderr

  def test_completed_query_errors_2(self, vector):
    if vector.get_value('strict_hs2_protocol'):
      pytest.skip("Impala-10827: Multiple queries not supported in strict hs2 mode.")
    args = ['-q', 'set abort_on_error=true; '
            'select id, cnt from functional_parquet.bad_column_metadata t, '
            '(select 1 cnt) u']
    result = run_impala_shell_cmd(vector, args, expect_success=False)
    assert 'ERROR: Column metadata states there are 11 values, ' in result.stderr,\
        result.stderr
    assert 'but read 10 values from column id.' in result.stderr, result.stderr

  def test_no_warnings_in_log_with_quiet_mode(self, vector):
    if vector.get_value('strict_hs2_protocol'):
      pytest.skip("The abort_on_error=false is not supported in strict hs2 mode.")
    """Regression test for IMPALA-4222."""
    args = ['--quiet', '-q', 'set abort_on_error=false;'
            'select count(*) from functional_seq_snap.bad_seq_snap']
    result = run_impala_shell_cmd(vector, args)
    assert 'WARNINGS:' not in result.stderr, result.stderr

  def test_removed_query_option(self, vector):
    if vector.get_value('strict_hs2_protocol'):
      pytest.skip("Impala query options not supported in strict hs2 mode.")
    """Test that removed query options produce warning."""
    result = run_impala_shell_cmd(vector, ['-q', 'set disable_cached_reads=true'],
                                  expect_success=True)
    assert "Ignoring removed query option: 'disable_cached_reads'" in result.stderr,\
        result.stderr

  def test_output_format(self, vector):
    expected_output = ['1', '2', '3']
    args = ['-q', 'select 1 as col_00001, 2 as col_2, 3 as col_03', '--quiet']
    result = run_impala_shell_cmd(vector, args + ['-B'])
    actual_output = [r.strip() for r in result.stdout.split('\t')]
    assert actual_output == expected_output
    result = run_impala_shell_cmd(vector, args + ['-B', '--output_delim=|'])
    actual_output = [r.strip() for r in result.stdout.split('|')]
    assert actual_output == expected_output
    result = run_impala_shell_cmd(vector, args + ['-B', '--output_delim=||'],
                                  expect_success=False)
    assert "Illegal delimiter" in result.stderr
    result = run_impala_shell_cmd(vector, args + ['-E'])
    result_rows = result.stdout.strip().split('\n')
    assert len(result_rows) == 4
    assert "************************************** " \
      "1.row **************************************" == result_rows[0]
    assert "col_00001: 1" == result_rows[1]
    assert "    col_2: 2" == result_rows[2]
    assert "   col_03: 3" == result_rows[3]

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

    # Impala query options not supported in strict hs2 mode."
    if not vector.get_value('strict_hs2_protocol'):
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
    if vector.get_value('strict_hs2_protocol'):
      pytest.skip("Runtime profile is not supported in strict hs2 mode.")
    # test summary is in both the profile printed by the
    # -p option and the one printed by the profile command
    args = ['-p', '-q', 'select 1; profile;']
    result_set = run_impala_shell_cmd(vector, args)
    # This regex helps us uniquely identify a profile.
    regex = re.compile("Operator\s+#Hosts\s+#Inst\s+Avg\s+Time")
    # We expect two query profiles.
    assert len(re.findall(regex, result_set.stdout)) == 2, \
        "Could not detect two profiles, stdout: %s" % result_set.stdout

  def test_runtime_profile_referenced_tables(self, vector, unique_database):
    if vector.get_value('strict_hs2_protocol'):
      pytest.skip("Runtime profile is not supported in strict hs2 mode.")
    db = unique_database
    base_args = ['-p', '-q']

    statements = ['select id from %s.shell_profile_test' % db,
                  'alter table %s.shell_profile_test add column b int' % db,
                  'insert into %s.shell_profile_test(id) values (1)' % db,
                  'truncate table %s.shell_profile_test' % db,
                  'drop table %s.shell_profile_test' % db]

    args = base_args + ['create table %s.shell_profile_test (id int)' % db]
    create = run_impala_shell_cmd(vector, args)
    assert "Referenced Tables: \n" in create.stdout

    for statement in statements:
      args = base_args + [statement]
      result = run_impala_shell_cmd(vector, args)
      assert "Referenced Tables: %s.shell_profile_test" % unique_database in result.stdout

  def test_runtime_profile_multiple_referenced_tables(self, vector, unique_database):
    if vector.get_value('strict_hs2_protocol'):
      pytest.skip("Runtime profile is not supported in strict hs2 mode.")

    def get_referenced_tables(profile):
      return re.findall(r'Referenced Tables: (.*)', profile)[0].split(', ')

    db = unique_database
    base_args = ['-p', '-q']

    for i in range(0, 2):
      args = base_args + ['create table %s.shell_profile_test%d (id int)' % (db, i)]
      run_impala_shell_cmd(vector, args)

    args = base_args + ["select * from {db}.shell_profile_test0 t0 inner join "
                        "{db}.shell_profile_test1 t1 on t0.id = t1.id".format(db=db)]
    result = run_impala_shell_cmd(vector, args)
    referenced_tables = get_referenced_tables(result.stdout)

    assert len(referenced_tables) == 2
    for i in range(0, 2):
      assert "{db}.shell_profile_test{index}".format(db=db, index=i) in referenced_tables

    args = base_args + ["select * from {db}.shell_profile_test0 t0 inner join "
                        "{db}.shell_profile_test1 t1 on t0.id = t1.id inner join "
                        "{db}.shell_profile_test1 t11 on t0.id = t11.id".format(db=db)]

    result = run_impala_shell_cmd(vector, args)
    referenced_tables = get_referenced_tables(result.stdout)

    assert len(referenced_tables) == 2
    for i in range(0, 2):
      assert "{db}.shell_profile_test{index}".format(db=db, index=i) in referenced_tables

  def test_summary(self, vector):
    if vector.get_value('strict_hs2_protocol'):
      pytest.skip("Summary not supported in strict hs2 mode.")
    args = ['-q', 'select count(*) from functional.alltypes; summary;']
    result_set = run_impala_shell_cmd(vector, args)
    assert "03:AGGREGATE" in result_set.stdout

    args = ['-q', 'summary;']
    result_set = run_impala_shell_cmd(vector, args, expect_success=False)
    assert "Could not retrieve summary: no previous query" in result_set.stderr

    args = ['-q', 'show tables; summary;']
    result_set = run_impala_shell_cmd(vector, args)
    assert "Summary not available" in result_set.stderr

    args = ['-q', 'show tables; summary 1;']
    result_set = run_impala_shell_cmd(vector, args, expect_success=False)
    invalid_err = "Invalid value for query attempt display mode"
    valid_opts = "Valid values are [ALL | LATEST | ORIGINAL]"
    assert "{0}: '1'. {1}".format(invalid_err, valid_opts) in result_set.stdout

    # Test queries without an exchange
    args = ['-q', 'select 1; summary;']
    result_set = run_impala_shell_cmd(vector, args)
    assert "00:UNION" in result_set.stdout

    args = ['-q', 'select 1; summary all;']
    result_set = run_impala_shell_cmd(vector, args)
    assert "00:UNION" in result_set.stdout

    args = ['-q', 'select 1; summary latest;']
    result_set = run_impala_shell_cmd(vector, args)
    assert "00:UNION" in result_set.stdout

    args = ['-q', 'select 1; summary original;']
    result_set = run_impala_shell_cmd(vector, args)
    assert "No failed summary found" in result_set.stdout

  @pytest.mark.execute_serially
  def test_queries_closed(self, vector):
    """Regression test for IMPALA-897."""
    if vector.get_value('strict_hs2_protocol'):
      pytest.skip("Strict hs2 mode does not support checking in flight queries.")
    args = ['-f', '{0}/test_close_queries.sql'.format(QUERY_FILE_PATH), '--quiet', '-B']
    # Execute the shell command async
    p = ImpalaShell(vector, args)

    impalad_service = ImpaladService(get_impalad_host_port(vector).split(':')[0])
    # The last query in the test SQL script will sleep for 10 seconds, so sleep
    # here for 5 seconds and verify the number of in-flight queries is 1.
    sleep(5)
    assert 1 == impalad_service.get_num_in_flight_queries()
    assert p.get_result().rc == 0
    assert impalad_service.wait_for_num_in_flight_queries(0)

  def test_cancellation(self, vector):
    """Test cancellation (Ctrl+C event). Run a query that sleeps 10ms per row so will run
    for 110s if not cancelled, but will detect cancellation quickly because of the small
    batch size."""
    if vector.get_value('strict_hs2_protocol'):
      pytest.skip("IMPALA-10827: Message not reported in strict hs2 mode.")
    query = "set num_nodes=1; set mt_dop=1; set batch_size=1; \
             select sleep(10) from functional_parquet.alltypesagg"
    p = ImpalaShell(vector, ['-q', query])
    try:
      p.wait_for_query_start()
    finally:
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
    if vector.get_value('strict_hs2_protocol'):
      pytest.skip("Checking in flight queries not supported in strict hs2 mode.")
    # A select where wait_to_finish takes several seconds
    stmt = "select * from tpch.customer c1, tpch.customer c2, " + \
           "tpch.customer c3 order by c1.c_name"
    # Kill happens in wait_to_finish state
    self.run_and_verify_query_cancellation_test(vector, stmt, "RUNNING")

  def run_and_verify_query_cancellation_test(self, vector, stmt, cancel_at_state):
    """Starts the execution of the received query, waits until the query
    execution in fact starts and then cancels it. Expects the query
    cancellation to succeed."""
    if vector.get_value('strict_hs2_protocol'):
      pytest.skip("Checking in flight queries not supported in strict hs2 mode.")
    p = ImpalaShell(vector, ['-q', stmt])
    try:
      wait_for_query_state(vector, stmt, cancel_at_state)
    finally:
      os.kill(p.pid(), signal.SIGINT)
    result = p.get_result()
    assert "Cancelling Query" in result.stderr
    assert "Invalid or unknown query handle" not in result.stderr

  def test_get_log_once(self, vector, empty_table):
    """Test that get_log() is always called exactly once."""
    if vector.get_value('strict_hs2_protocol'):
      pytest.skip("IMPALA-10827: Warnings are not supported in strict hs2 mode.")
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
      assert protocol in ('hs2', 'hs2-http'), protocol
      assert 'Reverting to tab delimited text' not in result.stderr
    assert 'UnicodeDecodeError' not in result.stderr
    assert RUSSIAN_CHARS.encode('utf-8') in result.stdout

  def test_international_characters_profile(self, vector):
    """IMPALA-12145: ensure we can handle international characters in the profile. """
    if vector.get_value('strict_hs2_protocol'):
      pytest.skip("Profile not supported in strict hs2 mode.")
    text = RUSSIAN_CHARS.encode('utf-8')
    args = ['-o', '/dev/null', '-p', '-q', "select '{0}'".format(text)]
    result = run_impala_shell_cmd(vector, args)
    assert 'UnicodeDecodeError' not in result.stderr
    assert text in result.stdout

  def test_utf8_decoding_error_handling(self, vector):
    """IMPALA-10145,IMPALA-10299: Regression tests for elegantly handling malformed utf-8
    characters."""
    result = run_impala_shell_cmd(vector, ['-B', '-q', "select substr('引擎', 1, 4)"])
    assert 'UnicodeDecodeError' not in result.stderr
    # Thrift changes its internal strings representation from bytes to unicodes since
    # 0.10.0. The results differ when impala-shell uses different versions of Thrift.
    # The UTF-8 encoded bytes of "引擎" are \xe5\xbc\x95\xe6\x93\x8e. The substr result
    # gets the first 4 bytes. In thrift-0.9.3-p8, it will be raw bytes, i.e. "引\xe6".
    # In thrift-0.11.0-p4, it will be decoded to utf-8 strings. The last byte can't be
    # decoded correctly so it will be replaced to \xef\xbf\xbd, i.e. U+FFFD. The result
    # is "引\xef\xbf\xbd". To make this test robust in all branches, here we just check
    # the existense of "引".
    assert '引' in result.stdout
    result = run_impala_shell_cmd(vector, ['-B', '-q', "select unhex('aa')"])
    assert 'UnicodeDecodeError' not in result.stderr
    # Same as above, the result using thrift <0.10.0 is '\xaa'. The result using
    # thrift >=0.10.0 is '\xef\xbf\xbd'. When testing with strict_hs2_protocol, this
    # is running against Hive which is another variable. On thrift 0.14 and higher,
    # talking to Hive, the result is b'\\xaa', so allow this as another possibility.
    assert '\xef\xbf\xbd' in result.stdout or '\xaa' in result.stdout or \
           '\\xaa' in result.stdout

  def test_global_config_file(self, vector):
    """Test global and user configuration files."""
    args = []
    # shell uses shell options in global config
    env = dict(os.environ)
    env[ImpalaShellClass.GLOBAL_CONFIG_FILE] = '{0}/good_impalarc2'.format(
        QUERY_FILE_PATH)
    result = run_impala_shell_cmd(vector, args, env=env)
    assert 'WARNING:' not in result.stderr, \
      "A valid config file should not trigger any warning: {0}".format(result.stderr)
    assert 'Query: select 2' in result.stderr

    # Various config options not supported in strict hs2 mode.
    if not vector.get_value('strict_hs2_protocol'):
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

    # "set" not supported for strict hs2 mode
    if not vector.get_value('strict_hs2_protocol'):
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

    # Test the optional configuration file with live_progress and live_summary
    # Positive test
    args = ['--config_file=%s/good_impalarc3' % QUERY_FILE_PATH]
    result = run_impala_shell_cmd(vector, args)
    assert 'WARNING:' not in result.stderr, \
      "A valid config file should not trigger any warning: {0}".format(result.stderr)
    # Negative Tests
    # specified config file with live_summary enabled for non interactive mode
    args = ['--config_file=%s/good_impalarc3' % QUERY_FILE_PATH, '--query=select 3']
    result = run_impala_shell_cmd(vector, args, expect_success=False)
    assert 'live_summary is available for interactive mode only' in result.stderr
    # testing config file related warning messages
    args = ['--config_file=%s/impalarc_with_warnings2' % QUERY_FILE_PATH]
    result = run_impala_shell_cmd(
      vector, args, expect_success=True, wait_until_connected=False)
    err_msg = ("WARNING: Unable to read configuration file correctly. "
               "Ignoring unrecognized config option: 'Live_Progress'\n")
    assert err_msg in result.stderr
    # bad formatting of config file with invalid value
    args = ['--config_file={0}/impalarc_with_error2'.format(QUERY_FILE_PATH)]
    result = run_impala_shell_cmd(vector, args, expect_success=False)
    err_msg = ("Unexpected value in configuration file. "
               "'maybe' is not a valid value for a boolean option.")
    assert err_msg in result.stderr

  def test_execute_queries_from_stdin(self, vector):
    """Test that queries get executed correctly when STDIN is given as the sql file."""
    args = ['-f', '-', '--quiet', '-B']
    query_file = "{0}/test_file_comments.sql".format(QUERY_FILE_PATH)
    query_file_handle = None
    try:
      query_file_handle = open(query_file, 'r')
      query = query_file_handle.read()
      query_file_handle.close()
    except Exception as e:
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

    result_1 = run_impala_shell_cmd(vector, args + ['--ldap_password_cmd=cmddoesntexist'],
                                    expect_success=False)

    assert "Error retrieving LDAP password" in result_1.stderr
    assert "command was: 'cmddoesntexist'" in result_1.stderr
    # On GCE instances, the error thrown in subprocess is "[Errno 13] Permission denied".
    assert "No such file or directory" in result_1.stderr \
        or "Permission denied" in result_1.stderr

    result_2 = run_impala_shell_cmd(vector,
                                    args + ['--ldap_password_cmd=cat filedoesntexist'],
                                    expect_success=False)

    assert "Error retrieving LDAP password" in result_2.stderr
    assert "command was 'cat filedoesntexist'" in result_2.stderr
    assert "No such file or directory" in result_2.stderr

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
        "Query state can be monitored at: "]
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

    if vector.get_value('strict_hs2_protocol'):
      assert "Time elapsed" in results.stderr
    else:
      assert "Modified 1 row(s)" in results.stderr

  def test_kudu_dml_reporting(self, vector, unique_database):
    if vector.get_value('strict_hs2_protocol'):
      pytest.skip("Kudu not supported in strict hs2 mode.")
    create_sql = 'create table %s (id int primary key, age int null)' \
        'partition by hash(id) partitions 2 stored as kudu'
    self._test_dml_reporting(vector, create_sql, unique_database, True)

  def test_iceberg_dml_reporting(self, vector, unique_database):
    if vector.get_value('strict_hs2_protocol'):
      pytest.skip("DML results are not completely supported in strict hs2 mode.")
    create_sql = 'create table %s (id int, age int) ' \
        'stored as iceberg tblproperties("format-version"="2")'
    self._test_dml_reporting(vector, create_sql, unique_database, False)

  def _test_dml_reporting(self, vector, create_sql, db, is_kudu):
    """ Runs DMLs on Kudu or Iceberg tables and verifies that modifed / deleted row
        count and number of row errors in Kudu are reported correctly. Kudu and Iceberg
        can have different results when adding rows with the same primary key as this
        leads to row errors in Kudu.
    """
    tbl = db + ".dml_test"
    run_impala_shell_cmd(vector, ['--query=' + create_sql % tbl])

    def validate(stmt, expected_rows_modified_iceberg, expected_rows_modified_kudu,
                 expected_row_errors_kudu, is_delete=False):
      results = run_impala_shell_cmd(vector, ['--query=' + stmt % tbl])
      expected = ""
      if is_kudu:
        expected = "Modified %d row(s), %d row error(s)" \
            % (expected_rows_modified_kudu, expected_row_errors_kudu)
      elif is_delete and expected_rows_modified_iceberg > 0:
        expected = "Deleted %d row(s)" % expected_rows_modified_iceberg
      else:
        expected = "Modified %d row(s)" % expected_rows_modified_iceberg
      assert expected in results.stderr, results.stderr

    validate("insert into %s (id) values (7), (7)", 2, 1, 1)
    validate("insert into %s (id) values (7)", 1, 0, 1)
    if is_kudu:
      validate("upsert into %s (id) values (7), (7)", -1, 2, 0)
    validate("update %s set age = 1 where id = 7", 3, 1, 0)
    validate("delete from %s where id = 7", 3, 1, 0, is_delete=True)

    # UPDATE/DELETE where there are no matching rows; there are no errors in Kudu because
    # the scan produced no rows.
    validate("update %s set age = 1 where id = 8", 0, 0, 0)
    validate("delete from %s where id = 7", 0, 0, 0, is_delete=True)

    # WITH clauses, only apply to INSERT and UPSERT
    validate("with y as (values(7)) insert into %s (id) select * from y", 1, 1, 0)
    validate("with y as (values(7)) insert into %s (id) select * from y", 1, 0, 1)
    if is_kudu:
      validate("with y as (values(7)) upsert into %s (id) select * from y", -1, 1, 0)

  def test_missing_query_file(self, vector):
    result = run_impala_shell_cmd(vector, ['-f', 'nonexistent.sql'], expect_success=False)
    assert "Could not open file 'nonexistent.sql'" in result.stderr

  def _validate_expected_socket_connected(self, vector, args, sock):
    # Building an one-off shell command instead of using Util::ImpalaShell since we need
    # to customize the impala daemon socket.
    protocol = vector.get_value("protocol")
    impala_shell_executable = get_impala_shell_executable(vector)
    shell_cmd = impala_shell_executable + ["--protocol={0}".format(protocol)]
    if protocol == 'beeswax':
      expected_output = "get_default_configuration"
    else:
      assert protocol == 'hs2'
      expected_output = "OpenSession"
    with open(os.devnull, 'w') as devnull:
      try:
        connection = None
        impala_shell = Popen(shell_cmd + args, stdout=devnull, stderr=devnull,
                             env=build_shell_env())
        # IMPALA-9547: retry accept(). This is required in Python < 3.5 because some
        # EINTR return calls from syscalls are not automatically retried. See PEP475.
        while True:
          try:
            connection, client_address = sock.accept()
            break
          except IOError as e:
            if e.errno != errno.EINTR:
              raise
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
    if vector.get_value('strict_hs2_protocol'):
      pytest.skip("Strict mode only supports LDAP protocol, will hang here.")
    if vector.get_value('protocol') == 'hs2-http':
      pytest.skip("--kerberos_host_fqdn not yet supported.")
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
    if vector.get_value('strict_hs2_protocol'):
      assert "Could not execute command:" in result.stderr
    else:
      assert "Encountered: EOF" in result.stderr
    args = ['-q', 'with v as (select 1) \;"']
    result = run_impala_shell_cmd(vector, args, expect_success=False)
    if vector.get_value('strict_hs2_protocol'):
      assert "cannot recognize input near"
    else:
      assert "Encountered: Unexpected character" in result.stderr

  def test_large_sql(self, vector, unique_database):
    if vector.get_value('strict_hs2_protocol'):
      pytest.skip("IMPALA-10827: Test did not pass for strict hs2 mode.")
    # In this test, we are only interested in the performance of Impala shell and not
    # the performance of Impala in general. So, this test will execute a large query
    # from a non-existent table since this will make the query execution time negligible.
    sql_file, sql_path = tempfile.mkstemp()
    # This generates a sql file size of ~50K.
    num_cols = 1000
    os.write(sql_file, "select \n")
    for i in range(num_cols):
      if i < num_cols:
        os.write(sql_file, "col_{0} as a{1},\n".format(i, i))
        os.write(sql_file, "col_{0} as b{1},\n".format(i, i))
        os.write(sql_file, "col_{0} as c{1}{2}\n".format(
            i, i, "," if i < num_cols - 1 else ""))
    os.write(sql_file, "from non_existence_large_table;")
    os.close(sql_file)

    try:
      args = ['-f', sql_path, '-d', unique_database]
      start_time = time()
      result = run_impala_shell_cmd(vector, args, expect_success=False)
      assert "Could not resolve table reference: 'non_existence_large_table'" \
          in result.stderr
      end_time = time()
      # Use higher timeout in ASAN/UBSAN to avoid flakiness (IMPALA-11921).
      build_runs_slowly = ImpalaTestClusterProperties.get_instance().runs_slowly()
      time_limit_s = 60 if build_runs_slowly else 20
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
    if vector.get_value('strict_hs2_protocol'):
      pytest.skip("Set does not work with strict hs2 mode.")
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

    # --connect_timeout_ms not supported with HTTP transport. Refer to the comment
    # in ImpalaClient::_get_http_transport() for details.
    # --http_socket_timeout_s not supported for strict_hs2_protocol.
    if (vector.get_value('protocol') == 'hs2-http' and
          vector.get_value('strict_hs2_protocol')):
        pytest.skip("THRIFT-4600")

    with closing(socket.socket()) as s:
      s.bind(("", 0))
      # maximum number of queued connections on this socket is 1.
      s.listen(1)
      test_port = s.getsockname()[1]
      args = ['-q', 'select foo; select bar;', '--ssl', '-t', '2000',
              '--http_socket_timeout_s', '2', '-i', 'localhost:%d' % (test_port)]
      run_impala_shell_cmd(vector, args, expect_success=False)

  def test_client_identifier(self, vector):
    """Confirms that a version string is passed along."""
    if vector.get_value('strict_hs2_protocol'):
      pytest.skip("Profile not supported in strict hs2 mode.")
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
    if vector.get_value('strict_hs2_protocol'):
      pytest.skip("IMPALA-10827: Failed locally, needs investigation.")
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
    if vector.get_value('strict_hs2_protocol'):
      pytest.skip("The typeof function is not supported in strict hs2 mode.")
    query = """select typeof(round(cast(8.072 as double), 3)), cast(0.5 as float),
                      round(cast(8.072 as double), 3), round(cast(8 as double), 3),
                      round(cast(8.0719999999 as double),3),
                      round(cast(8.072 as double), 0), round(8.072, 4)"""
    result = run_impala_shell_cmd(vector, ['-q', query, '-B'])
    # Note that Beeswax formats results differently (i.e. the "fix" for IMPALA-266 stopped
    # working for Beeswax at some point.
    protocol = vector.get_value("protocol")
    if protocol in ('hs2', 'hs2-http'):
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

  def test_binary_display(self, vector):
    """Test that binary values are displayed correctly."""
    query = "select binary_col from functional.binary_tbl"
    result = run_impala_shell_cmd(vector, ['-q', query])
    assert "| binary1            |" in result.stdout, result.stdout
    assert "| NULL               |" in result.stdout, result.stdout
    assert "|                    |" in result.stdout, result.stdout
    assert "| árvíztűrőtükörfúró |" in result.stdout, result.stdout
    assert "| 你好hello          |" in result.stdout, result.stdout
    assert "| \x00\xef\xbf\xbd\x00\xef\xbf\xbd                 |" in result.stdout, \
        result.stdout
    assert '| \xef\xbf\xbdD3"\x11\x00              |' in result.stdout, result.stdout

  def test_binary_as_string(self, vector):
    query = """select cast(binary_col as string) from functional.binary_tbl
               where string_col != "invalid utf8" """
    result = run_impala_shell_cmd(vector, ['-q', query])
    # Column length omitted because some strict HS2 protocol returns header "binary_col"
    # while others return "cast(binary_col as string)".
    assert "| binary1            " in result.stdout, result.stdout
    assert "| NULL               " in result.stdout, result.stdout
    assert "|                    " in result.stdout, result.stdout
    assert "| árvíztűrőtükörfúró " in result.stdout, result.stdout
    assert "| 你好hello          " in result.stdout, result.stdout

  def test_null_values(self, vector):
    """Test that null values are displayed correctly."""
    if vector.get_value('strict_hs2_protocol'):
      pytest.skip("IMPALA-10827: Need to investigate, this hung.")
    query = """select id, int_col, double_col
               from functional_parquet.alltypesagg where id < 5 order by id"""
    result = run_impala_shell_cmd(vector, ['-q', query, '-B'])
    assert "0\tNULL\tNULL" in result.stdout, result.stdout
    assert "0\tNULL\tNULL" in result.stdout, result.stdout
    assert "1\t1\t10.1" in result.stdout, result.stdout
    assert "2\t2\t20.2" in result.stdout, result.stdout

    # The HS2 client returns binary values for float/double types, and these must
    # be converted to strings for display. However, due to differences between the
    # way that python2 and python3 represent floating point values, the output
    # from the shell will differ with regard to which version of python the
    # shell is running under.
    assert("3\t3\t30.299999999999997" in result.stdout or
      "3\t3\t30.3" in result.stdout), result.stdout

    assert "4\t4\t40.4" in result.stdout, result.stdout

  def test_large_fetch(self, vector):
    query = "select ss_sold_time_sk from tpcds.store_sales limit 50000"
    output = run_impala_shell_cmd(vector, ['-q', query, '-B', '--output_delimiter=;'])
    assert "Fetched 50000 row(s)" in output.stderr

  def test_single_null_fetch(self, vector):
    query = "select null"
    output = run_impala_shell_cmd(vector, ['-q', query, '-B', '--output_delimiter=;'])
    assert "NULL" in output.stdout
    assert "Fetched 1 row(s)" in output.stderr

  def test_fetch_size(self, vector):
    """Test the --fetch_size option with and without result spooling enabled."""
    if vector.get_value('strict_hs2_protocol'):
      pytest.skip("IMPALA-10827: Failed locally, needs investigation.")
    query = "select * from functional.alltypes limit 1024"
    query_with_result_spooling = "set spool_query_results=true; " + query
    for query in [query, query_with_result_spooling]:
      result = run_impala_shell_cmd(vector, ['-q', query, '-B', '--fetch_size', '512'])
      result_rows = result.stdout.strip().split('\n')
      assert len(result_rows) == 1024

  def test_result_spooling_timeout(self, vector):
    """Regression test for IMPALA-9953. Validates that if a fetch timeout occurs in the
    middle of reading rows from Impala that all rows are still printed by the Impala
    shell."""
    if vector.get_value('strict_hs2_protocol'):
      pytest.skip("Spooling not supported in strict hs2 mode.")
    # This query was stolen from __test_fetch_timeout in test_fetch_timeout.py. The query
    # has a large delay between RowBatch production. So a fetch timeout will occur while
    # fetching rows.
    query_options = "set num_nodes=1; \
                     set fetch_rows_timeout_ms=1; \
                     set batch_size=1; \
                     set spool_query_results=true;"
    query = "select bool_col, avg(id) from functional.alltypes group by bool_col"
    result = run_impala_shell_cmd(vector, ['-q', query_options + query, '-B'])
    result_rows = result.stdout.strip().split('\n')
    assert len(result_rows) == 2

  def test_quiet_mode(self, vector):
    """Checks that extraneous output isn't included when --quiet is set."""
    args = ['--quiet', '-q', 'select 1']
    result = run_impala_shell_cmd(vector, args)
    if vector.get_value('strict_hs2_protocol'):
      expected_result = """+-----+\n| _c0 |\n+-----+\n| 1   |\n+-----+\n"""
    else:
      expected_result = """+---+\n| 1 |\n+---+\n| 1 |\n+---+\n"""
    assert result.stdout == expected_result
    assert result.stderr == ""

  def test_user_flag(self, vector):
    if vector.get_value('strict_hs2_protocol'):
      pytest.skip("The user functions are not supported in strict hs2 mode.")
    """Check that the --user flag is respected. This test assumes we are running against
    an unsecured cluster and can impersonate any user when connecting."""
    base_args = ['--quiet', '-B', '-q', 'select user(), effective_user()']

    # Test that default user is the user that impala-shell is run under.
    result = run_impala_shell_cmd(vector, base_args)
    expected_result = """{0}\t{0}\n""".format(getpass.getuser())
    assert result.stdout == expected_result
    assert result.stderr == ""

    # Test that we can set an arbitrary user.
    result = run_impala_shell_cmd(vector, base_args + ['--user=mickey mouse'])
    expected_result = """mickey mouse\tmickey mouse\n"""
    assert result.stdout == expected_result
    assert result.stderr == ""

    # Test that empty user results in "anonymous" - see IMPALA-10027.
    result = run_impala_shell_cmd(vector, base_args + ['--user='])
    expected_result = """anonymous\tanonymous\n"""
    assert result.stdout == expected_result
    assert result.stderr == ""

  def test_output_file(self, vector, tmp_file):
    """Test that writing output to a file using '--output_file' produces the same output
    as is written to stdout."""
    row_count = 6000  # Should be > 2048 to tickle IMPALA-10447.
    query = "select * from tpcds.item order by i_item_sk limit %d" % row_count
    # Run the query normally and keep the stdout.
    output = run_impala_shell_cmd(vector, ['-q', query, '-B', '--output_delimiter=;'])
    assert "Fetched %d row(s)" % row_count in output.stderr
    rows_from_stdout = output.stdout.strip().split('\n')
    # Run the query with output sent to a file using '--output_file'.
    result = run_impala_shell_cmd(vector, ['-q', query, '-B', '--output_delimiter=;',
                                           '--output_file=%s' % tmp_file])
    assert "Fetched %d row(s)" % row_count in result.stderr
    # Check that the output from the file is the same as that written to stdout.
    with open(tmp_file, "r") as f:
      rows_from_file = [line.rstrip() for line in f]
      assert rows_from_stdout == rows_from_file

  def test_output_file_utf8(self, vector, tmp_file):
    """Test that writing UTF-8 output to a file using '--output_file' produces the
    same output as written to stdout."""
    # This is purely about UTF-8 output, so it doesn't need multiple rows.
    query = "select '引'"
    # Run the query normally and keep the stdout
    output = run_impala_shell_cmd(vector, ['-q', query, '-B', '--output_delimiter=;'])
    assert "Fetched 1 row(s)" in output.stderr
    rows_from_stdout = output.stdout.strip().split('\n')
    # Run the query with output sent to a file using '--output_file'.
    result = run_impala_shell_cmd(vector, ['-q', query, '-B', '--output_delimiter=;',
                                           '--output_file=%s' % tmp_file])
    assert "Fetched 1 row(s)" in result.stderr
    with open(tmp_file, "r") as f:
      rows_from_file = [line.rstrip() for line in f]
      assert rows_from_stdout == rows_from_file

  def test_http_socket_timeout(self, vector):
    """Test setting different http_socket_timeout_s values."""
    if (vector.get_value('strict_hs2_protocol') or
          vector.get_value('protocol') != 'hs2-http'):
        pytest.skip("http socket timeout not supported in strict hs2 mode."
                    " Only supported with hs2-http protocol.")
    # Test http_socket_timeout_s=0, expect errors
    args = ['--quiet', '-B', '--query', 'select 0;']
    result = run_impala_shell_cmd(vector, args + ['--http_socket_timeout_s=0'],
                                  expect_success=False)

    # Outside the docker-based tests, Python 2 and Python 3 produce "Operating
    # now in progress" with slightly different error classes. When running with
    # docker-based tests, it results in a different error code and "Cannot
    # assign requested address" for both Python 2 and Python 3.
    # Tolerate all three of these variants.
    error_template = (
      "Caught exception [Errno {0}] {1}, type=<class '{2}'> in OpenSession. "
      "Num remaining tries: 3")
    expected_err_py2 = \
        error_template.format(115, "Operation now in progress", "socket.error")
    expected_err_py3 = \
        error_template.format(115, "Operation now in progress", "BlockingIOError")
    expected_err_docker = \
        error_template.format(99, "Cannot assign requested address", "OSError")
    actual_err = result.stderr.splitlines()[0]
    assert actual_err in [expected_err_py2, expected_err_py3, expected_err_docker]

    # Test http_socket_timeout_s=-1, expect errors
    result = run_impala_shell_cmd(vector, args + ['--http_socket_timeout_s=-1'],
                                  expect_success=False)
    expected_err = ("http_socket_timeout_s must be a nonnegative floating point number"
                    " expressing seconds, or None")
    assert result.stderr.splitlines()[0] == expected_err

    # Test http_socket_timeout_s>0, expect success
    result = run_impala_shell_cmd(vector, args + ['--http_socket_timeout_s=2'])
    assert result.stderr == ""
    assert result.stdout == "0\n"

    # Test http_socket_timeout_s=None, expect success
    result = run_impala_shell_cmd(vector, args + ['--http_socket_timeout_s=None'])
    assert result.stderr == ""
    assert result.stdout == "0\n"

  def test_connect_max_tries(self, vector):
    """Test setting different connect_max_tries values."""
    if (vector.get_value('strict_hs2_protocol')
        or vector.get_value('protocol') != 'hs2-http'):
      pytest.skip("connect_max_tries not supported in strict hs2 mode."
                  " Only supported with hs2-http protocol.")

    # Test connect_max_tries=-1, expect errors
    args = ['--quiet', '-B', '--query', 'select 0;']
    result = run_impala_shell_cmd(vector, args + ['--connect_max_tries=-1'],
                                  expect_success=False)
    expected_err = ("connect_max_tries must be greater than or equal to 1")
    assert result.stderr.splitlines()[0] == expected_err

    # Test connect_max_tries=0, expect errors
    result = run_impala_shell_cmd(vector, args + ['--connect_max_tries=0'],
                                  expect_success=False)
    expected_err = ("connect_max_tries must be greater than or equal to 1")
    assert result.stderr.splitlines()[0] == expected_err

    # Test connect_max_tries>0, expect success
    result = run_impala_shell_cmd(vector, args + ['--connect_max_tries=2'])
    assert result.stderr == ""
    assert result.stdout == "0\n"

  def test_trailing_whitespace(self, vector):
    """Test CSV output with trailing whitespace"""

    # Ten trailing spaces
    query = "select 'Trailing Whitespace          ' as x"
    # Only one column, no need for output_delimiter
    output = run_impala_shell_cmd(vector, ['-q', query, '-B'])
    assert "Fetched 1 row(s)" in output.stderr
    assert "Trailing Whitespace          \n" in output.stdout
    output = run_impala_shell_cmd(vector, ['-q', query, '-E'])
    assert "Fetched 1 row(s)" in output.stderr
    assert "x: Trailing Whitespace          \n" in output.stdout

  def test_shell_flush(self, vector, tmp_file):
    """Verify that the rows are flushed before the Fetch X row(s) message"""

    # Run a simple "select 1" with stdout and stderr redirected to the same file.
    with open(tmp_file, "w") as f:
      output = run_impala_shell_cmd(vector, ['-q', DEFAULT_QUERY, '-B'], stdout_file=f,
                                    stderr_file=f)
      # Stdout and stderr should be empty
      assert output.stderr is None
      assert output.stdout is None

    # Verify the file contents
    # The output should be in this order:
    # 1\n
    # Fetched 1 row(s) in ...\n
    with open(tmp_file, "r") as f:
      lines = f.readlines()
      assert len(lines) >= 2
      assert "1\n" in lines[len(lines) - 2]
      assert "Fetched 1 row(s)" in lines[len(lines) - 1]

  def skip_if_protocol_is_beeswax(self, vector,
      skip_msg="Floating-point value formatting is not supported with Beeswax"):
    """Helper to skip Beeswax protocol on formatting tests"""
    if vector.get_value("protocol") == "beeswax":
      pytest.skip(skip_msg)

  def validate_fp_format(self, vector, column_type, format, value, expected_values):
    args = ['--hs2_fp_format', format, '-q',
           'select cast("%s" as %s) as fp_value' % (value, column_type)]
    result = run_impala_shell_cmd(vector, args)

    assert(any(expected_value in result.stdout for expected_value in expected_values))

  def test_hs2_fp_format_types(self, vector):
    """Tests formatting with double and float value"""
    self.skip_if_protocol_is_beeswax(vector)
    self.validate_fp_format(vector, column_type='double', format='.16f',
                            value='0.1234567891011121314',
                            expected_values='0.1234567891011121')

    expected_values_float = ['0.12345679104328156', '0.12345679000000000']
    self.validate_fp_format(vector, column_type='float', format='.17f',
                            value='0.1234567891011121314',
                            expected_values=expected_values_float)

  def test_hs2_fp_format_modifiers(self, vector):
    """Test formatting with various modifiers, like mode, grouping, sign"""
    self.skip_if_protocol_is_beeswax(vector)
    self.validate_fp_format(vector, column_type='double', format='+f',
                            value='123456789123456789',
                            expected_values='+123456789123456784.000000')

    self.validate_fp_format(vector, column_type='double', format='+.16f',
                            value='-12.3456789123456789',
                            expected_values='-12.3456789123456794')

    expected_grouped_value = '+123,456,788,999,999,994,034,486,658,482,569,216.000000'
    self.validate_fp_format(vector, column_type='double', format='+,f',
                            value='1.23456789e+35',
                            expected_values=expected_grouped_value)

    self.validate_fp_format(vector, column_type='double', format='+E',
                            value='12345678912345',
                            expected_values='+1.234568E+13')

  def test_hs2_fp_format_nan_inf_null(self, vector):
    """Test NaN, inf, null value formatting"""
    self.skip_if_protocol_is_beeswax(vector)
    self.validate_fp_format(vector, column_type='double', format='F',
                            value='NaN',
                            expected_values='NAN')

    self.validate_fp_format(vector, column_type='double', format='F',
                            value='-inf',
                            expected_values=['-INF', 'NULL'])

    self.validate_fp_format(vector, column_type='double', format='F',
                            value='NULL',
                            expected_values='NULL')

  def test_hs2_fp_format_invalid(self, vector):
    """Test invalid format specification"""
    self.skip_if_protocol_is_beeswax(vector)
    error_message = 'Invalid floating point format specification: invalid'
    args = ['--hs2_fp_format', 'invalid']
    result = run_impala_shell_cmd(vector, args, expect_success=False)

    assert error_message in result.stderr

  def test_fp_default(self, vector):
    """Test default floating point value formatting"""
    expected_py2 = '0.123456789101'
    expected_py3 = '0.1234567891011121'
    args = ['-q', 'select cast("0.1234567891011121314" as double) as fp_value', '-B']
    result = run_impala_shell_cmd(vector, args)

    assert expected_py2 in result.stdout or expected_py3 in result.stdout

  def test_output_rpc_to_screen_and_file(self, vector, populated_table, tmp_file):
    """Tests the flags that output hs2 rpc call details to both stdout
    and a file.  Asserts the expected text is written."""
    self.skip_if_protocol_is_beeswax(vector,
        "rpc detail output not supported with beeswax protocol")

    args = ['--rpc_stdout', '--rpc_file', tmp_file,
            '-q', 'select * from {0}'.format(populated_table),
            '--protocol={0}'.format(vector.get_value("protocol"))]
    if vector.get_value("strict_hs2_protocol") is True:
      args.append('--strict_hs2_protocol')

    result = run_impala_shell_cmd(vector, args)

    stdout_data = result.stdout.strip()
    rpc_file_data = open(tmp_file, "r").read().strip()

    # compare the rpc details from stdout and file to ensure they match
    # stdout contains additional output such as query results, remove all non-rpc details
    rpc_sep = "------------------------------------------------" + \
              "------------------------------------------------"
    stdout_data_rpc_only = ""
    in_rpc_detail = False
    for line in stdout_data.split("\n"):
      if line == rpc_sep:
        in_rpc_detail = not in_rpc_detail
        stdout_data_rpc_only += line + "\n"
      elif in_rpc_detail:
        stdout_data_rpc_only += line + "\n"

    # rpc only stdout data contains an extra ending newline
    rpc_file_data += "\n"

    assert stdout_data_rpc_only == rpc_file_data, \
        "difference found between stdout and rpc file:\nSTDOUT:\n{0}\nFILE:\n{1}" \
        .format(stdout_data_rpc_only, rpc_file_data)

    def check_multiline(check_desc, regex_lines):
      """Build and runs a multi-line regular expression against both the
      shell's stdout and rpc file contents to ensure the expected data about
      the hs2 rpc calls was outputted."""
      the_re = re.compile("^" + "\n".join(regex_lines) + "$", re.MULTILINE)
      assert the_re.search(stdout_data), \
             "'{0}' assert failed in stdout: \n{1}" \
             .format(check_desc, stdout_data)
      assert the_re.search(rpc_file_data), \
             "'{0}' assert failed in the rpc file contents: \n{1}" \
             .format(check_desc, rpc_file_data)

    check_multiline("Open Session Request",
                    [
                      "{0}{1}".format(
                        "\\[\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d{6}\\] ",
                        "RPC CALL STARTED:"),
                      "OPERATION: OpenSession",
                      "DETAILS:",
                      "  \\* Impala Session Id: None",
                      "  \\* Impala Query Id:   None",
                      "  \\* Attempt Count:     1",
                      "",
                      "RPC REQUEST:",
                      "\\<TOpenSessionReq\\>",
                      "    - client_protocol: 5",
                      "    - configuration: <None>",
                      "    - password: <None>",
                      "    - username: .*?",
                    ])

    check_multiline("Open Session Response",
                    [
                      "{0}{1}".format(
                        "\\[\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d{6}\\] ",
                        "RPC CALL FINISHED:"),
                      "OPERATION: OpenSession",
                      "DETAILS:",
                      "  \\* Time:   \\d+(\\.\\d+)?ms",
                      "  \\* Result: SUCCESS",
                      "",
                      "RPC RESPONSE:",
                      "\\<TOpenSessionResp\\>",
                      "    - configuration: .*?",
                      "    - serverProtocolVersion: .*?",
                      "    - sessionHandle: \\<TSessionHandle\\>",
                      "                       - sessionId: \\<THandleIdentifier\\>",
                      "                                      - guid: \\<binary data\\>",
                      "                                      - secret: "
                      "\\*\\*\\*\\*\\*\\*\\*",
                      "    - status: \\<TStatus\\>",
                      "                - errorCode: \\<None\\>",
                      "                - errorMessage: \\<None\\>",
                      "                - infoMessages: \\<None\\>",
                      "                - sqlState: \\<None\\>",
                      "                - statusCode: 0",
                    ])

    check_multiline("Session and Query Id",
                    [
                      "{0}{1}".format(
                        "\\[\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d{6}\\] ",
                        "RPC CALL STARTED:"),
                      "OPERATION: FetchResults",
                      "DETAILS:",
                      "  \\* Impala Session Id: [a-z0-9]*:[a-z0-9]*",
                      "  \\* Impala Query Id:   [a-z0-9]*:[a-z0-9]*",
                    ])

    check_multiline("TRowSet Skipped",
                    [
                      "{0}{1}".format(
                        "\\[\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d{6}\\] ",
                        "RPC CALL FINISHED:"),
                      "OPERATION: FetchResults",
                      "DETAILS:",
                      "  \\* Time:   \\d+(\\.\\d+)?ms",
                      "  \\* Result: SUCCESS",
                      "",
                      "RPC RESPONSE:",
                      "<TFetchResultsResp>",
                      "    - hasMoreRows: False",
                      "    - results: <TRowSet> - <skipping>",
                      "    - status: <TStatus>",
                      "                - errorCode: <None>",
                      "                - errorMessage: <None>",
                      "                - infoMessages: <None>",
                      "                - sqlState: <None>",
                      "                - statusCode: 0",
                    ])
