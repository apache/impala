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
from copy import copy
import os
import re
import pytest
import tempfile
from subprocess import call, check_call

from tests.beeswax.impala_beeswax import ImpalaBeeswaxException
from tests.common.impala_cluster import ImpalaCluster
from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.skip import SkipIfLocal
from tests.common.test_dimensions import (
    create_exec_option_dimension,
    create_exec_option_dimension_from_dict,
    create_uncompressed_text_dimension)
from tests.util.calculation_util import get_random_id
from tests.util.filesystem_utils import get_fs_path, WAREHOUSE
from tests.verifiers.metric_verifier import MetricVerifier

class TestUdfBase(ImpalaTestSuite):
  """
  Base class with utility functions for testing UDFs.
  """
  def _check_mem_limit_exception(self, e):
    """Return without error if the exception is MEM_LIMIT_EXCEEDED, re-raise 'e'
    in all other cases."""
    if 'Memory limit exceeded' in str(e):
      return
    raise e

  def _run_query_all_impalads(self, exec_options, query, expected):
    impala_cluster = ImpalaCluster.get_e2e_test_cluster()
    for impalad in impala_cluster.impalads:
      client = impalad.service.create_beeswax_client()
      result = self.execute_query_expect_success(client, query, exec_options)
      assert result.data == expected, impalad

  def _load_functions(self, template, vector, database, location):
    queries = template.format(database=database, location=location)
    # Split queries and remove empty lines
    queries = [q for q in queries.split(';') if q.strip()]
    exec_options = vector.get_value('exec_option')
    for query in queries:
      if query.strip() == '': continue
      result = self.execute_query_expect_success(self.client, query, exec_options)
      assert result is not None

  # Create sample UDA functions in {database} from library {location}
  create_sample_udas_template = """
create aggregate function {database}.test_count(int) returns bigint
location '{location}' update_fn='CountUpdate';

create aggregate function {database}.hll(int) returns string
location '{location}' update_fn='HllUpdate';

create aggregate function {database}.sum_small_decimal(decimal(9,2))
returns decimal(9,2) location '{location}' update_fn='SumSmallDecimalUpdate';
"""

  # Create test UDA functions in {database} from library {location}
  create_test_udas_template = """
create aggregate function {database}.trunc_sum(double)
returns bigint intermediate double location '{location}'
update_fn='TruncSumUpdate' merge_fn='TruncSumMerge'
serialize_fn='TruncSumSerialize' finalize_fn='TruncSumFinalize';

create aggregate function {database}.arg_is_const(int, int)
returns boolean location '{location}'
init_fn='ArgIsConstInit' update_fn='ArgIsConstUpdate' merge_fn='ArgIsConstMerge';

create aggregate function {database}.toggle_null(int)
returns int location '{location}'
update_fn='ToggleNullUpdate' merge_fn='ToggleNullMerge';

create aggregate function {database}.count_nulls(bigint)
returns bigint location '{location}'
update_fn='CountNullsUpdate' merge_fn='CountNullsMerge';

create aggregate function {database}.agg_intermediate(int)
returns bigint intermediate string location '{location}'
init_fn='AggIntermediateInit' update_fn='AggIntermediateUpdate'
merge_fn='AggIntermediateMerge' finalize_fn='AggIntermediateFinalize';

create aggregate function {database}.agg_decimal_intermediate(decimal(2,1), int)
returns decimal(6,5) intermediate decimal(4,3) location '{location}'
init_fn='AggDecimalIntermediateInit' update_fn='AggDecimalIntermediateUpdate'
merge_fn='AggDecimalIntermediateMerge' finalize_fn='AggDecimalIntermediateFinalize';

create aggregate function {database}.agg_date_intermediate(date, int)
returns date intermediate date location '{location}'
init_fn='AggDateIntermediateInit' update_fn='AggDateIntermediateUpdate'
merge_fn='AggDateIntermediateMerge' finalize_fn='AggDateIntermediateFinalize';

create aggregate function {database}.agg_string_intermediate(decimal(20,10), bigint, string)
returns decimal(20,0) intermediate string location '{location}'
init_fn='AggStringIntermediateInit' update_fn='AggStringIntermediateUpdate'
merge_fn='AggStringIntermediateMerge' finalize_fn='AggStringIntermediateFinalize';

create aggregate function {database}.agg_binary_intermediate(decimal(20,10), bigint, binary)
returns decimal(20,0) intermediate binary location '{location}'
init_fn='AggStringIntermediateInit' update_fn='AggStringIntermediateUpdate'
merge_fn='AggStringIntermediateMerge' finalize_fn='AggStringIntermediateFinalize';

create aggregate function {database}.char_intermediate_sum(int) returns int
intermediate char(10) LOCATION '{location}' update_fn='AggCharIntermediateUpdate'
init_fn='AggCharIntermediateInit' merge_fn='AggCharIntermediateMerge'
serialize_fn='AggCharIntermediateSerialize' finalize_fn='AggCharIntermediateFinalize';
"""

  # Create test UDF functions in {database} from library {location}
  create_udfs_template = """
create function {database}.identity(boolean) returns boolean
location '{location}' symbol='Identity';

create function {database}.identity(tinyint) returns tinyint
location '{location}' symbol='Identity';

create function {database}.identity(smallint) returns smallint
location '{location}' symbol='Identity';

create function {database}.identity(int) returns int
location '{location}' symbol='Identity';

create function {database}.identity(bigint) returns bigint
location '{location}' symbol='Identity';

create function {database}.identity(float) returns float
location '{location}' symbol='Identity';

create function {database}.identity(double) returns double
location '{location}' symbol='Identity';

create function {database}.identity(string) returns string
location '{location}'
symbol='_Z8IdentityPN10impala_udf15FunctionContextERKNS_9StringValE';

create function {database}.identity(binary) returns binary
location '{location}'
symbol='_Z8IdentityPN10impala_udf15FunctionContextERKNS_9StringValE';

create function {database}.identity(timestamp) returns timestamp
location '{location}'
symbol='_Z8IdentityPN10impala_udf15FunctionContextERKNS_12TimestampValE';

create function {database}.identity(date) returns date
location '{location}'
symbol='_Z8IdentityPN10impala_udf15FunctionContextERKNS_7DateValE';

create function {database}.identity(decimal(9,0)) returns decimal(9,0)
location '{location}'
symbol='_Z8IdentityPN10impala_udf15FunctionContextERKNS_10DecimalValE';

create function {database}.identity(decimal(18,1)) returns decimal(18,1)
location '{location}'
symbol='_Z8IdentityPN10impala_udf15FunctionContextERKNS_10DecimalValE';

create function {database}.identity(decimal(38,10)) returns decimal(38,10)
location '{location}'
symbol='_Z8IdentityPN10impala_udf15FunctionContextERKNS_10DecimalValE';

create function {database}.all_types_fn(
    string, boolean, tinyint, smallint, int, bigint, float, double, decimal(2,0), date,
    binary)
returns int
location '{location}' symbol='AllTypes';

create function {database}.no_args() returns string
location '{location}'
symbol='_Z6NoArgsPN10impala_udf15FunctionContextE';

create function {database}.var_and(boolean...) returns boolean
location '{location}' symbol='VarAnd';

create function {database}.var_sum(int...) returns int
location '{location}' symbol='VarSum';

create function {database}.var_sum(double...) returns double
location '{location}' symbol='VarSum';

create function {database}.var_sum(string...) returns int
location '{location}' symbol='VarSum';

create function {database}.var_sum(decimal(4,2)...) returns decimal(18,2)
location '{location}' symbol='VarSum';

create function {database}.var_sum_multiply(double, int...) returns double
location '{location}'
symbol='_Z14VarSumMultiplyPN10impala_udf15FunctionContextERKNS_9DoubleValEiPKNS_6IntValE';

create function {database}.var_sum_multiply2(double, int...) returns double
location '{location}'
symbol='_Z15VarSumMultiply2PN10impala_udf15FunctionContextERKNS_9DoubleValEiPKNS_6IntValE';

create function {database}.xpow(double, double) returns double
location '{location}'
symbol='_ZN6impala13MathFunctions3PowEPN10impala_udf15FunctionContextERKNS1_9DoubleValES6_';

create function {database}.to_lower(string) returns string
location '{location}'
symbol='_Z7ToLowerPN10impala_udf15FunctionContextERKNS_9StringValE';

create function {database}.to_upper(string) returns string
location '{location}'
symbol='_Z7ToUpperPN10impala_udf15FunctionContextERKNS_9StringValE';

create function {database}.constant_timestamp() returns timestamp
location '{location}' symbol='ConstantTimestamp';

create function {database}.constant_date() returns date
location '{location}' symbol='ConstantDate';

create function {database}.validate_arg_type(string) returns boolean
location '{location}' symbol='ValidateArgType';

create function {database}.count_rows() returns bigint
location '{location}' symbol='Count' prepare_fn='CountPrepare' close_fn='CountClose';

create function {database}.constant_arg(int) returns int
location '{location}' symbol='ConstantArg' prepare_fn='ConstantArgPrepare' close_fn='ConstantArgClose';

create function {database}.validate_open(int) returns boolean
location '{location}' symbol='ValidateOpen'
prepare_fn='ValidateOpenPrepare' close_fn='ValidateOpenClose';

create function {database}.mem_test(bigint) returns bigint
location '{location}' symbol='MemTest'
prepare_fn='MemTestPrepare' close_fn='MemTestClose';

create function {database}.mem_test_leaks(bigint) returns bigint
location '{location}' symbol='MemTest'
prepare_fn='MemTestPrepare';

-- Regression test for IMPALA-1475
create function {database}.unmangled_symbol() returns bigint
location '{location}' symbol='UnmangledSymbol';

create function {database}.four_args(int, int, int, int) returns int
location '{location}' symbol='FourArgs';

create function {database}.five_args(int, int, int, int, int) returns int
location '{location}' symbol='FiveArgs';

create function {database}.six_args(int, int, int, int, int, int) returns int
location '{location}' symbol='SixArgs';

create function {database}.seven_args(int, int, int, int, int, int, int) returns int
location '{location}' symbol='SevenArgs';

create function {database}.eight_args(int, int, int, int, int, int, int, int) returns int
location '{location}' symbol='EightArgs';

create function {database}.twenty_args(int, int, int, int, int, int, int, int, int, int,
    int, int, int, int, int, int, int, int, int, int) returns int
location '{location}' symbol='TwentyArgs';

create function {database}.twenty_one_args(int, int, int, int, int, int, int, int, int, int,
    int, int, int, int, int, int, int, int, int, int, int) returns int
location '{location}' symbol='TwentyOneArgs';
"""

class TestUdfExecution(TestUdfBase):
  """Test execution of UDFs with a combination of different query options."""
  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestUdfExecution, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(
        create_exec_option_dimension_from_dict({"disable_codegen" : [False, True],
          "disable_codegen_rows_threshold" : [0],
          "exec_single_node_rows_threshold" : [0,100],
          "enable_expr_rewrites" : [False, True]}))
    # There is no reason to run these tests using all dimensions.
    cls.ImpalaTestMatrix.add_dimension(
        create_uncompressed_text_dimension(cls.get_workload()))

  def test_native_functions(self, vector, unique_database):
    enable_expr_rewrites = vector.get_value('exec_option')['enable_expr_rewrites']
    self._load_functions(
      self.create_udfs_template, vector, unique_database,
      get_fs_path('/test-warehouse/libTestUdfs.so'))
    self._load_functions(
      self.create_sample_udas_template, vector, unique_database,
      get_fs_path('/test-warehouse/libudasample.so'))
    self._load_functions(
      self.create_test_udas_template, vector, unique_database,
      get_fs_path('/test-warehouse/libTestUdas.so'))

    self.run_test_case('QueryTest/udf', vector, use_db=unique_database)
    if not vector.get_value('exec_option')['disable_codegen']:
      self.run_test_case('QueryTest/udf-codegen-required', vector, use_db=unique_database)
    self.run_test_case('QueryTest/uda', vector, use_db=unique_database)
    self.run_test_case('QueryTest/udf-init-close', vector, use_db=unique_database)
    # Some tests assume no expr rewrites.
    if enable_expr_rewrites:
      self.run_test_case('QueryTest/udf-init-close-deterministic', vector,
          use_db=unique_database)
    else:
      self.run_test_case('QueryTest/udf-no-expr-rewrite', vector,
          use_db=unique_database)

  def test_ir_functions(self, vector, unique_database):
    if vector.get_value('exec_option')['disable_codegen']:
      # IR functions require codegen to be enabled.
      return
    enable_expr_rewrites = vector.get_value('exec_option')['enable_expr_rewrites']
    self._load_functions(
      self.create_udfs_template, vector, unique_database,
      get_fs_path('/test-warehouse/test-udfs.ll'))
    self.run_test_case('QueryTest/udf', vector, use_db=unique_database)
    self.run_test_case('QueryTest/udf-init-close', vector, use_db=unique_database)
    # Some tests assume determinism or non-determinism, which depends on expr rewrites.
    if enable_expr_rewrites:
      self.run_test_case('QueryTest/udf-init-close-deterministic', vector,
          use_db=unique_database)
    else:
      self.run_test_case('QueryTest/udf-no-expr-rewrite', vector, use_db=unique_database)

  def test_java_udfs(self, vector, unique_database):
    vector = copy(vector)
    vector.get_value('exec_option')['abort_java_udf_on_exception'] = True
    self.run_test_case('QueryTest/load-java-udfs', vector, use_db=unique_database)
    self.run_test_case('QueryTest/load-java-udfs-fail', vector, use_db=unique_database)
    self.run_test_case('QueryTest/java-udf', vector, use_db=unique_database)
    vector.get_value('exec_option')['abort_java_udf_on_exception'] = False
    self.run_test_case('QueryTest/java-udf-no-abort-on-exception', vector,
        use_db=unique_database)

  def test_generic_java_udfs(self, vector, unique_database):
    vector = copy(vector)
    vector.get_value('exec_option')['abort_java_udf_on_exception'] = True
    self.run_test_case('QueryTest/load-generic-java-udfs', vector, use_db=unique_database)
    self.run_test_case('QueryTest/generic-java-udf', vector, use_db=unique_database)
    vector.get_value('exec_option')['abort_java_udf_on_exception'] = False
    self.run_test_case('QueryTest/generic-java-udf-no-abort-on-exception', vector,
        use_db=unique_database)

  def test_udf_errors(self, vector, unique_database):
    # Only run with codegen disabled to force interpretation path to be taken.
    # Aim to exercise two failure cases:
    # 1. too many arguments
    # 2. IR UDF
    fd, dir_name = tempfile.mkstemp()
    hdfs_path = get_fs_path("/test-warehouse/{0}_bad_udf.ll".format(unique_database))
    try:
      with open(dir_name, "w") as f:
        f.write("Hello World")
      self.filesystem_client.copy_from_local(f.name, hdfs_path)
      if vector.get_value('exec_option')['disable_codegen']:
        self.run_test_case('QueryTest/udf-errors', vector, use_db=unique_database)
    finally:
      if os.path.exists(f.name):
        os.remove(f.name)
      call(["hadoop", "fs", "-rm", "-f", hdfs_path])
      os.close(fd)

  # Run serially because this will blow the process limit, potentially causing other
  # queries to fail
  @pytest.mark.execute_serially
  def test_mem_limits(self, vector, unique_database):
    # Set the mem_limit and buffer_pool_limit high enough that the query makes it through
    # admission control and a simple scan can run.
    vector = copy(vector)
    vector.get_value('exec_option')['mem_limit'] = '1mb'
    vector.get_value('exec_option')['buffer_pool_limit'] = '4.04mb'
    try:
      self.run_test_case('QueryTest/udf-mem-limit', vector, use_db=unique_database)
      assert False, "Query was expected to fail"
    except ImpalaBeeswaxException as e:
      self._check_mem_limit_exception(e)

    try:
      self.run_test_case('QueryTest/uda-mem-limit', vector, use_db=unique_database)
      assert False, "Query was expected to fail"
    except ImpalaBeeswaxException as e:
      self._check_mem_limit_exception(e)

    # It takes a long time for Impala to free up memory after this test, especially if
    # ASAN is enabled. Verify that all fragments finish executing before moving on to the
    # next test to make sure that the next test is not affected.
    for impalad in ImpalaCluster.get_e2e_test_cluster().impalads:
      verifier = MetricVerifier(impalad.service)
      verifier.wait_for_metric("impala-server.num-fragments-in-flight", 0)
      verifier.verify_num_unused_buffers()

  def test_udf_constant_folding(self, vector, unique_database):
    """Test that constant folding of UDFs is handled correctly. Uses count_rows(),
    which returns a unique value every time it is evaluated in the same thread."""
    exec_options = copy(vector.get_value('exec_option'))
    # Execute on a single node so that all counter values will be unique.
    exec_options["num_nodes"] = 1
    create_fn_query = """create function {database}.count_rows() returns bigint
                         location '{location}' symbol='Count' prepare_fn='CountPrepare'
                         close_fn='CountClose'"""
    self._load_functions(create_fn_query, vector, unique_database,
        get_fs_path('/test-warehouse/libTestUdfs.so'))

    # Only one distinct value if the expression is constant folded, otherwise one
    # value per row in alltypes
    expected_ndv = 1 if exec_options['enable_expr_rewrites'] else 7300

    # Test fully constant expression, evaluated in FE.
    query = "select `{0}`.count_rows() from functional.alltypes".format(unique_database)
    result = self.execute_query_expect_success(self.client, query, exec_options)
    actual_ndv = len(set(result.data))
    assert actual_ndv == expected_ndv

    # Test constant argument to a non-constant expr. The argument value can be
    # cached in the backend.
    query = """select concat(cast(`{0}`.count_rows() as string), '-', string_col)
               from functional.alltypes""".format(unique_database)
    result = self.execute_query_expect_success(self.client, query, exec_options)
    actual_ndv = len(set(value.split("-")[0] for value in result.data))
    assert actual_ndv == expected_ndv


class TestUdfTargeted(TestUdfBase):
  """Targeted UDF tests that don't need to be run under the full combination of
  exec options."""
  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestUdfTargeted, cls).add_test_dimensions()
    # There is no reason to run these tests using all dimensions.
    cls.ImpalaTestMatrix.add_dimension(
        create_uncompressed_text_dimension(cls.get_workload()))

  def test_udf_invalid_symbol(self, vector, unique_database):
    """ IMPALA-1642: Impala crashes if the symbol for a Hive UDF doesn't exist
        Invalid symbols are checked at UDF creation time."""
    src_udf_path = os.path.join(
        os.environ['IMPALA_HOME'], 'testdata/udfs/impala-hive-udfs.jar')
    tgt_udf_path = get_fs_path(
        '/test-warehouse/{0}.db/impala-hive-udfs.jar'.format(unique_database))
    drop_fn_stmt = (
        "drop function if exists `{0}`.fn_invalid_symbol(STRING)".format(unique_database))
    create_fn_stmt = (
        "create function `{0}`.fn_invalid_symbol(STRING) returns "
        "STRING LOCATION '{1}' SYMBOL='not.a.Symbol'".format(
            unique_database, tgt_udf_path))

    self.filesystem_client.copy_from_local(src_udf_path, tgt_udf_path)
    self.client.execute(drop_fn_stmt)
    ex = self.execute_query_expect_failure(self.client, create_fn_stmt)
    assert "ClassNotFoundException" in str(ex)

  def test_hidden_symbol(self, vector, unique_database):
    """Test that symbols in the test UDFs are hidden by default and that therefore
    they cannot be used as a UDF entry point."""
    symbol = "_Z16UnexportedSymbolPN10impala_udf15FunctionContextE"
    ex = self.execute_query_expect_failure(self.client, """
        create function `{0}`.unexported() returns BIGINT LOCATION '{1}'
        SYMBOL='{2}'""".format(
        unique_database, get_fs_path('/test-warehouse/libTestUdfs.so'), symbol))
    assert "Could not find symbol '{0}'".format(symbol) in str(ex), str(ex)
    # IMPALA-8196: IR UDFs ignore whether symbol is hidden or not. Exercise the current
    # behaviour, where the UDF can be created and executed.
    result = self.execute_query_expect_success(self.client, """
        create function `{0}`.unexported() returns BIGINT LOCATION '{1}'
        SYMBOL='{2}'""".format(
        unique_database, get_fs_path('/test-warehouse/test-udfs.ll'), symbol))
    result = self.execute_query_expect_success(self.client,
        "select `{0}`.unexported()".format(unique_database))
    assert result.data[0][0] == '5'

  @SkipIfLocal.multiple_impalad
  def test_hive_udfs_missing_jar(self, vector, unique_database):
    """ IMPALA-2365: Impalad shouldn't crash if the udf jar isn't present
    on HDFS"""
    # Copy hive-exec.jar to a temporary file
    jar_path = get_fs_path("/test-warehouse/{0}.db/".format(unique_database)
                           + get_random_id(5) + ".jar")
    hive_jar = get_fs_path("/test-warehouse/hive-exec.jar")
    self.filesystem_client.copy(hive_jar, jar_path)
    drop_fn_stmt = (
        "drop function if exists "
        "`{0}`.`pi_missing_jar`()".format(unique_database))
    create_fn_stmt = (
        "create function `{0}`.`pi_missing_jar`() returns double location '{1}' "
        "symbol='org.apache.hadoop.hive.ql.udf.UDFPI'".format(unique_database, jar_path))

    cluster = ImpalaCluster.get_e2e_test_cluster()
    impalad = cluster.get_any_impalad()
    client = impalad.service.create_beeswax_client()
    # Create and drop functions with sync_ddl to make sure they are reflected
    # in every impalad.
    exec_option = copy(vector.get_value('exec_option'))
    exec_option['sync_ddl'] = 1

    self.execute_query_expect_success(client, drop_fn_stmt, exec_option)
    self.execute_query_expect_success(client, create_fn_stmt, exec_option)
    # Delete the udf jar
    check_call(["hadoop", "fs", "-rm", jar_path])

    different_impalad = cluster.get_different_impalad(impalad)
    client = different_impalad.service.create_beeswax_client()
    # Run a query using the udf from an impalad other than the one
    # we used to create the function. This is to bypass loading from
    # the cache
    try:
      self.execute_query_using_client(
          client, "select `{0}`.`pi_missing_jar`()".format(unique_database), vector)
      assert False, "Query expected to fail"
    except ImpalaBeeswaxException as e:
      assert "Failed to get file info" in str(e)

  def test_libs_with_same_filenames(self, vector, unique_database):
    self.run_test_case('QueryTest/libs_with_same_filenames', vector, use_db=unique_database)

  def test_udf_update_via_drop(self, vector, unique_database):
    """Test updating the UDF binary without restarting Impala. Dropping
    the function should remove the binary from the local cache."""
    # Run with sync_ddl to guarantee the drop is processed by all impalads.
    exec_options = copy(vector.get_value('exec_option'))
    exec_options['sync_ddl'] = 1
    old_udf = os.path.join(
        os.environ['IMPALA_HOME'], 'testdata/udfs/impala-hive-udfs.jar')
    new_udf = os.path.join(
        os.environ['IMPALA_HOME'], 'testdata/udfs/impala-hive-udfs-modified.jar')
    udf_dst = get_fs_path(
        '/test-warehouse/{0}.db/impala-hive-udfs.jar'.format(unique_database))

    drop_fn_stmt = (
        'drop function if exists `{0}`.`udf_update_test_drop`()'.format(unique_database))
    create_fn_stmt = (
        "create function `{0}`.`udf_update_test_drop`() returns string LOCATION '{1}' "
        "SYMBOL='org.apache.impala.TestUpdateUdf'".format(unique_database, udf_dst))
    query_stmt = "select `{0}`.`udf_update_test_drop`()".format(unique_database)

    # Put the old UDF binary on HDFS, make the UDF in Impala and run it.
    self.filesystem_client.copy_from_local(old_udf, udf_dst)
    self.execute_query_expect_success(self.client, drop_fn_stmt, exec_options)
    self.execute_query_expect_success(self.client, create_fn_stmt, exec_options)
    self._run_query_all_impalads(exec_options, query_stmt, ["Old UDF"])

    # Update the binary, drop and create the function again. The new binary should
    # be running.
    self.filesystem_client.copy_from_local(new_udf, udf_dst)
    self.execute_query_expect_success(self.client, drop_fn_stmt, exec_options)
    self.execute_query_expect_success(self.client, create_fn_stmt, exec_options)
    self._run_query_all_impalads(exec_options, query_stmt, ["New UDF"])

  def test_udf_update_via_create(self, vector, unique_database):
    """Test updating the UDF binary without restarting Impala. Creating a new function
    from the library should refresh the cache."""
    # Run with sync_ddl to guarantee the create is processed by all impalads.
    exec_options = copy(vector.get_value('exec_option'))
    exec_options['sync_ddl'] = 1
    old_udf = os.path.join(
        os.environ['IMPALA_HOME'], 'testdata/udfs/impala-hive-udfs.jar')
    new_udf = os.path.join(
        os.environ['IMPALA_HOME'], 'testdata/udfs/impala-hive-udfs-modified.jar')
    udf_dst = get_fs_path(
        '/test-warehouse/{0}.db/impala-hive-udfs.jar'.format(unique_database))
    old_function_name = "udf_update_test_create1"
    new_function_name = "udf_update_test_create2"

    drop_fn_template = 'drop function if exists `{0}`.`{{0}}`()'.format(unique_database)
    self.execute_query_expect_success(
        self.client, drop_fn_template.format(old_function_name), exec_options)
    self.execute_query_expect_success(
        self.client, drop_fn_template.format(new_function_name), exec_options)

    create_fn_template = (
        "create function `{0}`.`{{0}}`() returns string LOCATION '{1}' "
        "SYMBOL='org.apache.impala.TestUpdateUdf'".format(unique_database, udf_dst))

    query_template = "select `{0}`.`{{0}}`()".format(unique_database)

    # Put the old UDF binary on HDFS, make the UDF in Impala and run it.
    self.filesystem_client.copy_from_local(old_udf, udf_dst)
    self.execute_query_expect_success(
        self.client, create_fn_template.format(old_function_name), exec_options)
    self._run_query_all_impalads(
        exec_options, query_template.format(old_function_name), ["Old UDF"])

    # Update the binary, and create a new function using the binary. The new binary
    # should be running.
    self.filesystem_client.copy_from_local(new_udf, udf_dst)
    self.execute_query_expect_success(
        self.client, create_fn_template.format(new_function_name), exec_options)
    self._run_query_all_impalads(
        exec_options, query_template.format(new_function_name), ["New UDF"])

    # The old function should use the new library now
    self._run_query_all_impalads(
        exec_options, query_template.format(old_function_name), ["New UDF"])

  def test_drop_function_while_running(self, vector, unique_database):
    self.client.execute("drop function if exists `{0}`.drop_while_running(BIGINT)"
                        .format(unique_database))
    self.client.execute(
        "create function `{0}`.drop_while_running(BIGINT) returns "
        "BIGINT LOCATION '{1}' SYMBOL='Identity'".format(
            unique_database,
            get_fs_path('/test-warehouse/libTestUdfs.so')))
    query = ("select `{0}`.drop_while_running(l_orderkey) from tpch.lineitem limit 10000"
             .format(unique_database))

    # Run this query asynchronously.
    handle = self.execute_query_async(query, vector.get_value('exec_option'),
                                      table_format=vector.get_value('table_format'))

    # Fetch some rows from the async query to make sure the UDF is being used
    results = self.client.fetch(query, handle, 1)
    assert results.success
    assert len(results.data) == 1

    # Drop the function while the original query is running.
    self.client.execute(
        "drop function `{0}`.drop_while_running(BIGINT)".format(unique_database))

    # Fetch the rest of the rows, this should still be able to run the UDF
    results = self.client.fetch(query, handle, -1)
    assert results.success
    assert len(results.data) == 9999

  def test_udf_profile(self, vector, unique_database):
    """Test to validate that explain plans and runtime profiles contain information about
    any custom UDFs used in an Impala query."""
    self.client.execute(
        "create function {0}.hive_substring(string, int) returns string location '{1}' "
        "symbol='org.apache.hadoop.hive.ql.udf.UDFSubstr'".format(
            unique_database, get_fs_path('/test-warehouse/hive-exec.jar')))
    profile = self.execute_query_expect_success(self.client,
        "select {0}.hive_substring(string_col, 1), {0}.hive_substring(string_col, 2) "
        "from functional.alltypes limit 10".format(unique_database)).runtime_profile

    assert re.search("output exprs.*hive_substring.*/\* JAVA UDF \*/", profile)
    # Ensure that hive_substring only shows up once in the list of UDFs.
    assert re.search(
        "User Defined Functions \(UDFs\): {0}\.hive_substring\s*[\r\n]".format(
            unique_database), profile)

  def test_set_fallback_db_for_functions(self, vector, unique_database):
    """IMPALA-11728: Set fallback database for functions."""
    create_function_stmt = "create function `{0}`.fn() returns int "\
          "location '{1}/libTestUdfs.so' symbol='NoArgs'".format(unique_database,
          WAREHOUSE)
    self.client.execute(create_function_stmt)

    # case 1: When the function name is fully qualified then this query option
    # has no effect.
    assert '6' == self.execute_scalar("select {0}.fn() from functional.alltypes "
          "limit 1".format(unique_database))

    # case 2: Throw an exception without specifying the database.
    query_stmt = "select fn() from functional.alltypes limit 1"
    result = self.execute_query_expect_failure(self.client, query_stmt)
    assert "default.fn() unknown for database default" in str(result)

    # case 3: Use fn() in fallback db after setting FALLBACK_DB_FOR_FUNCTIONS
    assert '6' == self.execute_scalar(query_stmt, query_options={
        'fallback_db_for_functions': unique_database})

    # case 4: Test a function name that also exists as builtin function.
    # Use function in _impala_builtins.
    create_function_stmt = "create function `{0}`.abs(int) returns int "\
          "location '{1}/libTestUdfs.so' symbol='Identity'".format(unique_database,
          WAREHOUSE)
    self.client.execute(create_function_stmt)

    assert '1' == self.execute_scalar("select abs(-1)", query_options={
        'fallback_db_for_functions': unique_database})

    # case 5: It should return empty result for show function, even when
    # FALLBACK_DB_FOR_FUNCTIONS is set.
    result = self.execute_scalar("show functions", query_options={
        'fallback_db_for_functions': unique_database})
    assert result is None
