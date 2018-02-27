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

from copy import copy
import os
import pytest
import random
import threading
import time
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
from tests.util.filesystem_utils import get_fs_path, IS_S3
from tests.verifiers.metric_verifier import MetricVerifier

class TestUdfBase(ImpalaTestSuite):
  """
  Base class with utility functions for testing UDFs.
  """
  def _check_exception(self, e):
    # The interesting exception message may be in 'e' or in its inner_exception
    # depending on the point of query failure.
    if 'Memory limit exceeded' in str(e) or 'Cancelled' in str(e):
      return
    if e.inner_exception is not None\
       and ('Memory limit exceeded' in e.inner_exception.message
            or 'Cancelled' not in e.inner_exception.message):
      return
    raise e

  def _run_query_all_impalads(self, exec_options, query, expected):
    impala_cluster = ImpalaCluster()
    for impalad in impala_cluster.impalads:
      client = impalad.service.create_beeswax_client()
      result = self.execute_query_expect_success(client, query, exec_options)
      assert result.data == expected

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

create aggregate function {database}.agg_string_intermediate(decimal(20,10), bigint, string)
returns decimal(20,0) intermediate string location '{location}'
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

create function {database}.identity(timestamp) returns timestamp
location '{location}'
symbol='_Z8IdentityPN10impala_udf15FunctionContextERKNS_12TimestampValE';

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
    string, boolean, tinyint, smallint, int, bigint, float, double, decimal(2,0))
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
    # Some tests assume determinism or non-determinism, which depends on expr rewrites.
    if enable_expr_rewrites:
      self.run_test_case('QueryTest/udf-init-close-deterministic', vector,
          use_db=unique_database)
    else:
      self.run_test_case('QueryTest/udf-non-deterministic', vector,
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
      self.run_test_case('QueryTest/udf-non-deterministic', vector,
          use_db=unique_database)

  # Runs serially as a temporary workaround for IMPALA_6092.
  @pytest.mark.execute_serially
  def test_java_udfs(self, vector, unique_database):
    self.run_test_case('QueryTest/load-java-udfs', vector, use_db=unique_database)
    self.run_test_case('QueryTest/java-udf', vector, use_db=unique_database)

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
      check_call(["hadoop", "fs", "-put", "-f", f.name, hdfs_path])
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
    # Set the mem limit high enough that a simple scan can run
    mem_limit = 1024 * 1024
    vector = copy(vector)
    vector.get_value('exec_option')['mem_limit'] = mem_limit
    try:
      self.run_test_case('QueryTest/udf-mem-limit', vector, use_db=unique_database)
      assert False, "Query was expected to fail"
    except ImpalaBeeswaxException, e:
      self._check_exception(e)

    try:
      self.run_test_case('QueryTest/uda-mem-limit', vector, use_db=unique_database)
      assert False, "Query was expected to fail"
    except ImpalaBeeswaxException, e:
      self._check_exception(e)

    # It takes a long time for Impala to free up memory after this test, especially if
    # ASAN is enabled. Verify that all fragments finish executing before moving on to the
    # next test to make sure that the next test is not affected.
    for impalad in ImpalaCluster().impalads:
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
        Crashing is non-deterministic so we run the UDF several times."""
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
    query = "select `{0}`.fn_invalid_symbol('test')".format(unique_database)

    # Dropping the function can interact with other tests whose Java classes are in
    # the same jar. Use a copy of the jar to avoid unintended interactions.
    # See IMPALA-6215 and IMPALA-6092 for examples.
    check_call(["hadoop", "fs", "-put", "-f", src_udf_path, tgt_udf_path])
    self.client.execute(drop_fn_stmt)
    self.client.execute(create_fn_stmt)
    for _ in xrange(5):
      ex = self.execute_query_expect_failure(self.client, query)
      assert "Unable to find class" in str(ex)
    self.client.execute(drop_fn_stmt)

  @SkipIfLocal.multiple_impalad
  def test_hive_udfs_missing_jar(self, vector, unique_database):
    """ IMPALA-2365: Impalad shouldn't crash if the udf jar isn't present
    on HDFS"""
    # Copy hive-exec.jar to a temporary file
    jar_path = get_fs_path("/test-warehouse/{0}.db/".format(unique_database)
                           + get_random_id(5) + ".jar")
    hive_jar = get_fs_path("/test-warehouse/hive-exec.jar")
    check_call(["hadoop", "fs", "-cp", hive_jar, jar_path])
    drop_fn_stmt = (
        "drop function if exists "
        "`{0}`.`pi_missing_jar`()".format(unique_database))
    create_fn_stmt = (
        "create function `{0}`.`pi_missing_jar`() returns double location '{1}' "
        "symbol='org.apache.hadoop.hive.ql.udf.UDFPI'".format(unique_database, jar_path))

    cluster = ImpalaCluster()
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
    except ImpalaBeeswaxException, e:
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
        os.environ['IMPALA_HOME'], 'tests/test-hive-udfs/target/test-hive-udfs-1.0.jar')
    udf_dst = get_fs_path(
        '/test-warehouse/{0}.db/impala-hive-udfs.jar'.format(unique_database))

    drop_fn_stmt = (
        'drop function if exists `{0}`.`udf_update_test_drop`()'.format(unique_database))
    create_fn_stmt = (
        "create function `{0}`.`udf_update_test_drop`() returns string LOCATION '{1}' "
        "SYMBOL='org.apache.impala.TestUpdateUdf'".format(unique_database, udf_dst))
    query_stmt = "select `{0}`.`udf_update_test_drop`()".format(unique_database)

    # Put the old UDF binary on HDFS, make the UDF in Impala and run it.
    check_call(["hadoop", "fs", "-put", "-f", old_udf, udf_dst])
    self.execute_query_expect_success(self.client, drop_fn_stmt, exec_options)
    self.execute_query_expect_success(self.client, create_fn_stmt, exec_options)
    self._run_query_all_impalads(exec_options, query_stmt, ["Old UDF"])

    # Update the binary, drop and create the function again. The new binary should
    # be running.
    check_call(["hadoop", "fs", "-put", "-f", new_udf, udf_dst])
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
        os.environ['IMPALA_HOME'], 'tests/test-hive-udfs/target/test-hive-udfs-1.0.jar')
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
    check_call(["hadoop", "fs", "-put", "-f", old_udf, udf_dst])
    self.execute_query_expect_success(
        self.client, create_fn_template.format(old_function_name), exec_options)
    self._run_query_all_impalads(
        exec_options, query_template.format(old_function_name), ["Old UDF"])

    # Update the binary, and create a new function using the binary. The new binary
    # should be running.
    check_call(["hadoop", "fs", "-put", "-f", new_udf, udf_dst])
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
