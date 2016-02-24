# Copyright (c) 2012 Cloudera, Inc. All rights reserved.

import os
from tests.beeswax.impala_beeswax import ImpalaBeeswaxException
from tests.common.test_vector import *
from tests.common.impala_test_suite import *
from tests.common.impala_cluster import ImpalaCluster
from tests.common.skip import SkipIfLocal
from tests.common.skip import SkipIfS3
from tests.util.calculation_util import get_random_id
from tests.util.filesystem_utils import get_fs_path, IS_S3
from subprocess import check_call

class TestUdfs(ImpalaTestSuite):
  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestUdfs, cls).add_test_dimensions()
    # Without limiting the test suite to a single exec option, the tests will fail
    # because the same test case may be executed in parallel with different exec option
    # values leading to conflicting DDL ops.
    cls.TestMatrix.add_dimension(create_single_exec_option_dimension())

    # There is no reason to run these tests using all dimensions.
    cls.TestMatrix.add_dimension(create_uncompressed_text_dimension(cls.get_workload()))

  def test_native_functions(self, vector):
    database = 'native_function_test'

    self.__load_functions(
      self.create_udfs_template, vector, database,
      get_fs_path('/test-warehouse/libTestUdfs.so'))
    self.__load_functions(
      self.create_sample_udas_template, vector, database,
      get_fs_path('/test-warehouse/libudasample.so'))
    self.__load_functions(
      self.create_test_udas_template, vector, database,
      get_fs_path('/test-warehouse/libTestUdas.so'))

    self.run_test_case('QueryTest/udf', vector, use_db=database)
    if not IS_S3: # S3 doesn't support INSERT
      self.run_test_case('QueryTest/udf-init-close', vector, use_db=database)
    self.run_test_case('QueryTest/uda', vector, use_db=database)

  def test_ir_functions(self, vector):
    database = 'ir_function_test'
    self.__load_functions(
      self.create_udfs_template, vector, database,
      get_fs_path('/test-warehouse/test-udfs.ll'))
    self.run_test_case('QueryTest/udf', vector, use_db=database)
    if not IS_S3: # S3 doesn't support INSERT
      self.run_test_case('QueryTest/udf-init-close', vector, use_db=database)

  def test_udf_errors(self, vector):
    self.run_test_case('QueryTest/udf-errors', vector)

  def test_udf_invalid_symbol(self, vector):
    """ IMPALA-1642: Impala crashes if the symbol for a Hive UDF doesn't exist
        Crashing is non-deterministic so we run the UDF several times."""
    drop_fn_stmt = "drop function if exists default.fn_invalid_symbol(STRING)"
    create_fn_stmt = ("create function default.fn_invalid_symbol(STRING) returns "
        "STRING LOCATION '%s' SYMBOL='not.a.Symbol'" %
        get_fs_path('/test-warehouse/impala-hive-udfs.jar'))
    query = "select default.fn_invalid_symbol('test')"

    self.client.execute(drop_fn_stmt)
    try:
      self.client.execute(create_fn_stmt)
      for _ in xrange(5):
        ex = self.execute_query_expect_failure(self.client, query)
        assert "Unable to find class" in str(ex)
    finally:
      self.client.execute(drop_fn_stmt)

  def test_java_udfs(self, vector):
    self.client.execute("create database if not exists java_udfs_test "\
        "location '%s'" % get_fs_path('/test-warehouse/java_udf_test.db'))
    self.client.execute("create database if not exists udf_test "\
        "location '%s'" % get_fs_path('/test-warehouse/udf_test.db'))
    try:
      self.run_test_case('QueryTest/load-java-udfs', vector)
      self.run_test_case('QueryTest/java-udf', vector)
    finally:
      self.client.execute("drop database if exists java_udfs_test cascade")
      self.client.execute("drop database if exists udf_test cascade")

  @SkipIfLocal.multiple_impalad
  def test_hive_udfs_missing_jar(self, vector):
    """ IMPALA-2365: Impalad shouldn't crash if the udf jar isn't present
    on HDFS"""
    # Copy hive-exec.jar to a temporary file
    jar_path = get_fs_path("/test-warehouse/" + get_random_id(5) + ".jar")
    hive_jar = get_fs_path("/test-warehouse/hive-exec.jar")
    check_call(["hadoop", "fs", "-cp", hive_jar, jar_path])
    drop_fn_stmt = "drop function if exists default.pi_missing_jar()"
    create_fn_stmt = "create function default.pi_missing_jar() returns double \
        location '%s' symbol='org.apache.hadoop.hive.ql.udf.UDFPI'" % jar_path

    cluster = ImpalaCluster()
    impalad = cluster.get_any_impalad()
    client = impalad.service.create_beeswax_client()
    # Create and drop functions with sync_ddl to make sure they are reflected
    # in every impalad.
    exec_option = vector.get_value('exec_option')
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
      self.execute_query_using_client(client,
          "select default.pi_missing_jar()", vector)
      assert False, "Query expected to fail"
    except ImpalaBeeswaxException, e:
      assert "Failed to get file info" in str(e)

  def test_libs_with_same_filenames(self, vector):
    self.run_test_case('QueryTest/libs_with_same_filenames', vector)

  def test_udf_update_via_drop(self, vector):
    """Test updating the UDF binary without restarting Impala. Dropping
    the function should remove the binary from the local cache."""
    # Run with sync_ddl to guarantee the drop is processed by all impalads.
    exec_options = vector.get_value('exec_option')
    exec_options['sync_ddl'] = 1
    old_udf = os.path.join(os.environ['IMPALA_HOME'],
        'testdata/udfs/impala-hive-udfs.jar')
    new_udf = os.path.join(os.environ['IMPALA_HOME'],
        'tests/test-hive-udfs/target/test-hive-udfs-1.0.jar')
    udf_dst = get_fs_path('/test-warehouse/impala-hive-udfs2.jar')

    drop_fn_stmt = 'drop function if exists default.udf_update_test_drop()'
    create_fn_stmt = "create function default.udf_update_test_drop() returns string "\
        "LOCATION '" + udf_dst + "' SYMBOL='com.cloudera.impala.TestUpdateUdf'"
    query_stmt = "select default.udf_update_test_drop()"

    # Put the old UDF binary on HDFS, make the UDF in Impala and run it.
    check_call(["hadoop", "fs", "-put", "-f", old_udf, udf_dst])
    self.execute_query_expect_success(self.client, drop_fn_stmt, exec_options)
    self.execute_query_expect_success(self.client, create_fn_stmt, exec_options)
    self.__run_query_all_impalads(exec_options, query_stmt, ["Old UDF"])

    # Update the binary, drop and create the function again. The new binary should
    # be running.
    check_call(["hadoop", "fs", "-put", "-f", new_udf, udf_dst])
    self.execute_query_expect_success(self.client, drop_fn_stmt, exec_options)
    self.execute_query_expect_success(self.client, create_fn_stmt, exec_options)
    self.__run_query_all_impalads(exec_options, query_stmt, ["New UDF"])

  def test_udf_update_via_create(self, vector):
    """Test updating the UDF binary without restarting Impala. Creating a new function
    from the library should refresh the cache."""
    # Run with sync_ddl to guarantee the create is processed by all impalads.
    exec_options = vector.get_value('exec_option')
    exec_options['sync_ddl'] = 1
    old_udf = os.path.join(os.environ['IMPALA_HOME'],
        'testdata/udfs/impala-hive-udfs.jar')
    new_udf = os.path.join(os.environ['IMPALA_HOME'],
        'tests/test-hive-udfs/target/test-hive-udfs-1.0.jar')
    udf_dst = get_fs_path('/test-warehouse/impala-hive-udfs3.jar')
    old_function_name = "udf_update_test_create1"
    new_function_name = "udf_update_test_create2"

    drop_fn_template = 'drop function if exists default.%s()'
    self.execute_query_expect_success(
      self.client, drop_fn_template % old_function_name, exec_options)
    self.execute_query_expect_success(
      self.client, drop_fn_template % new_function_name, exec_options)

    create_fn_template = "create function default.%s() returns string "\
        "LOCATION '" + udf_dst + "' SYMBOL='com.cloudera.impala.TestUpdateUdf'"
    query_template = "select default.%s()"

    # Put the old UDF binary on HDFS, make the UDF in Impala and run it.
    check_call(["hadoop", "fs", "-put", "-f", old_udf, udf_dst])
    self.execute_query_expect_success(
      self.client, create_fn_template % old_function_name, exec_options)
    self.__run_query_all_impalads(
      exec_options, query_template % old_function_name, ["Old UDF"])

    # Update the binary, and create a new function using the binary. The new binary
    # should be running.
    check_call(["hadoop", "fs", "-put", "-f", new_udf, udf_dst])
    self.execute_query_expect_success(
      self.client, create_fn_template % new_function_name, exec_options)
    self.__run_query_all_impalads(
      exec_options, query_template % new_function_name, ["New UDF"])

    # The old function should use the new library now
    self.__run_query_all_impalads(
      exec_options, query_template % old_function_name, ["New UDF"])

  def test_drop_function_while_running(self, vector):
    self.client.execute("drop function if exists default.drop_while_running(BIGINT)")
    self.client.execute("create function default.drop_while_running(BIGINT) returns "\
        "BIGINT LOCATION '%s' SYMBOL='Identity'" %
        get_fs_path('/test-warehouse/libTestUdfs.so'))
    query = \
        "select default.drop_while_running(l_orderkey) from tpch.lineitem limit 10000";

    # Run this query asynchronously.
    handle = self.execute_query_async(query, vector.get_value('exec_option'),
                                      table_format=vector.get_value('table_format'))

    # Fetch some rows from the async query to make sure the UDF is being used
    results = self.client.fetch(query, handle, 1)
    assert results.success
    assert len(results.data) == 1

    # Drop the function while the original query is running.
    self.client.execute("drop function default.drop_while_running(BIGINT)")

    # Fetch the rest of the rows, this should still be able to run the UDF
    results = self.client.fetch(query, handle, -1)
    assert results.success
    assert len(results.data) == 9999

  # Run serially because this will blow the process limit, potentially causing other
  # queries to fail
  @pytest.mark.execute_serially
  def test_mem_limits(self, vector):
    # Set the mem limit high enough that a simple scan can run
    mem_limit = 1024 * 1024
    vector.get_value('exec_option')['mem_limit'] = mem_limit

    try:
      self.run_test_case('QueryTest/udf-mem-limit', vector)
      assert False, "Query was expected to fail"
    except ImpalaBeeswaxException, e:
      self.__check_exception(e)

    try:
      self.run_test_case('QueryTest/uda-mem-limit', vector)
      assert False, "Query was expected to fail"
    except ImpalaBeeswaxException, e:
      self.__check_exception(e)

  def __check_exception(self, e):
    # The interesting exception message may be in 'e' or in its inner_exception
    # depending on the point of query failure.
    if 'Memory limit exceeded' in str(e) or 'Cancelled' in str(e):
      return
    if e.inner_exception is not None\
       and ('Memory limit exceeded' in e.inner_exception.message
            or 'Cancelled' not in e.inner_exception.message):
      return
    raise e

  def __run_query_all_impalads(self, exec_options, query, expected):
    impala_cluster = ImpalaCluster()
    for impalad in impala_cluster.impalads:
      client = impalad.service.create_beeswax_client()
      result = self.execute_query_expect_success(client, query, exec_options)
      assert result.data == expected

  def __load_functions(self, template, vector, database, location):
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
drop function if exists {database}.test_count(int);
drop function if exists {database}.hll(int);
drop function if exists {database}.sum_small_decimal(decimal(9,2));

create database if not exists {database};

create aggregate function {database}.test_count(int) returns bigint
location '{location}' update_fn='CountUpdate';

create aggregate function {database}.hll(int) returns string
location '{location}' update_fn='HllUpdate';

create aggregate function {database}.sum_small_decimal(decimal(9,2))
returns decimal(9,2) location '{location}' update_fn='SumSmallDecimalUpdate';
"""

  # Create test UDA functions in {database} from library {location}
  create_test_udas_template = """
drop function if exists {database}.trunc_sum(double);

create database if not exists {database};

create aggregate function {database}.trunc_sum(double)
returns bigint intermediate double location '{location}'
update_fn='TruncSumUpdate' merge_fn='TruncSumMerge'
serialize_fn='TruncSumSerialize' finalize_fn='TruncSumFinalize';
"""

  # Create test UDF functions in {database} from library {location}
  create_udfs_template = """
drop function if exists {database}.identity(boolean);
drop function if exists {database}.identity(tinyint);
drop function if exists {database}.identity(smallint);
drop function if exists {database}.identity(int);
drop function if exists {database}.identity(bigint);
drop function if exists {database}.identity(float);
drop function if exists {database}.identity(double);
drop function if exists {database}.identity(string);
drop function if exists {database}.identity(timestamp);
drop function if exists {database}.identity(decimal(9,0));
drop function if exists {database}.identity(decimal(18,1));
drop function if exists {database}.identity(decimal(38,10));
drop function if exists {database}.all_types_fn(
    string, boolean, tinyint, smallint, int, bigint, float, double, decimal(2,0));
drop function if exists {database}.no_args();
drop function if exists {database}.var_and(boolean...);
drop function if exists {database}.var_sum(int...);
drop function if exists {database}.var_sum(double...);
drop function if exists {database}.var_sum(string...);
drop function if exists {database}.var_sum(decimal(4,2)...);
drop function if exists {database}.var_sum_multiply(double, int...);
drop function if exists {database}.constant_timestamp();
drop function if exists {database}.validate_arg_type(string);
drop function if exists {database}.count_rows();
drop function if exists {database}.constant_arg(int);
drop function if exists {database}.validate_open(int);
drop function if exists {database}.mem_test(bigint);
drop function if exists {database}.mem_test_leaks(bigint);
drop function if exists {database}.unmangled_symbol();
drop function if exists {database}.four_args(int, int, int, int);
drop function if exists {database}.five_args(int, int, int, int, int);
drop function if exists {database}.six_args(int, int, int, int, int, int);
drop function if exists {database}.seven_args(int, int, int, int, int, int, int);
drop function if exists {database}.eight_args(int, int, int, int, int, int, int, int);

create database if not exists {database};

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
"""
