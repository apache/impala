#!/usr/bin/env python
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.

from tests.common.test_vector import *
from tests.common.impala_test_suite import *
from tests.common.impala_cluster import ImpalaCluster
from subprocess import call

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
    cls.TestMatrix.add_constraint(lambda v:\
        v.get_value('table_format').file_format == 'text' and\
        v.get_value('table_format').compression_codec == 'none')

  def test_native_functions(self, vector):
    database = 'native_function_test'

    self.__load_functions(
      self.create_udfs_template, vector, database, '/test-warehouse/libTestUdfs.so')
    self.__load_functions(
      self.create_udas_template, vector, database, '/test-warehouse/libudasample.so')

    self.run_test_case('QueryTest/udf', vector, use_db=database)
    self.run_test_case('QueryTest/udf-init-close', vector, use_db=database)
    self.run_test_case('QueryTest/uda', vector, use_db=database)

  def test_ir_functions(self, vector):
    database = 'ir_function_test'
    self.__load_functions(
      self.create_udfs_template, vector, database, '/test-warehouse/test-udfs.ll')
    self.run_test_case('QueryTest/udf', vector, use_db=database)
    self.run_test_case('QueryTest/udf-init-close', vector, use_db=database)

  def test_hive_udfs(self, vector):
    self.client.execute('create database if not exists udf_test')
    self.client.execute('create database if not exists uda_test')
    self.run_test_case('QueryTest/load-hive-udfs', vector)
    self.run_test_case('QueryTest/hive-udf', vector)

  def test_libs_with_same_filenames(self, vector):
    self.run_test_case('QueryTest/libs_with_same_filenames', vector)

  def test_udf_update(self, vector):
    # Test updating the UDF binary without restarting Impala. Dropping
    # the function should remove the binary from the local cache.
    # Run with sync_ddl to guarantee the drop is processed by all impalads.
    exec_options = vector.get_value('exec_option')
    exec_options['sync_ddl'] = 1
    old_udf = os.path.join(os.environ['IMPALA_HOME'],
        'testdata/udfs/impala-hive-udfs.jar')
    new_udf = os.path.join(os.environ['IMPALA_HOME'],
        'tests/test-hive-udfs/target/test-hive-udfs-1.0.jar')
    udf_dst = '/test-warehouse/impala-hive-udfs2.jar'

    drop_fn_stmt = 'drop function if exists default.udf_update_test()'
    create_fn_stmt = "create function default.udf_update_test() returns string "\
        "LOCATION '" + udf_dst + "' SYMBOL='com.cloudera.impala.TestUpdateUdf'"
    query_stmt = "select default.udf_update_test()"

    # Put the old UDF binary on HDFS, make the UDF in Impala and run it.
    call(["hadoop", "fs", "-put", "-f", old_udf, udf_dst])
    self.execute_query_expect_success(self.client, drop_fn_stmt, exec_options)
    self.execute_query_expect_success(self.client, create_fn_stmt, exec_options)
    self.__run_query_all_impalads(exec_options, query_stmt, ["Old UDF"])

    # Update the binary, drop and create the function again. The new binary should
    # be running.
    call(["hadoop", "fs", "-put", "-f", new_udf, udf_dst])
    self.execute_query_expect_success(self.client, drop_fn_stmt, exec_options)
    self.execute_query_expect_success(self.client, create_fn_stmt, exec_options)
    self.__run_query_all_impalads(exec_options, query_stmt, ["New UDF"])

  def test_drop_function_while_running(self, vector):
    self.client.execute("drop function if exists default.drop_while_running(BIGINT)")
    self.client.execute("create function default.drop_while_running(BIGINT) returns "\
        "BIGINT LOCATION '/test-warehouse/libTestUdfs.so' SYMBOL='Identity'")
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

  # Create test UDA functions in {database} from library {location}
  create_udas_template = """
drop function if exists {database}.test_count(int);
drop function if exists {database}.hll(int);

create database if not exists {database};

create aggregate function {database}.test_count(int) returns bigint
location '{location}' update_fn='CountUpdate';

create aggregate function {database}.hll(int) returns string
location '{location}' update_fn='HllUpdate';
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
drop function if exists {database}.all_types_fn(
    string, boolean, tinyint, smallint, int, bigint, float, double);
drop function if exists {database}.no_args();
drop function if exists {database}.var_and(boolean...);
drop function if exists {database}.var_sum(int...);
drop function if exists {database}.var_sum(double...);
drop function if exists {database}.var_sum(string...);
drop function if exists {database}.var_sum_multiply(double, int...);
drop function if exists {database}.constant_timestamp();
drop function if exists {database}.validate_arg_type(string);
drop function if exists {database}.count_rows();
drop function if exists {database}.constant_arg(int);
drop function if exists {database}.validate_open(int);

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

create function {database}.all_types_fn(
    string, boolean, tinyint, smallint, int, bigint, float, double)
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

create function {database}.var_sum_multiply(double, int...) returns double
location '{location}'
symbol='_Z14VarSumMultiplyPN10impala_udf15FunctionContextERKNS_9DoubleValEiPKNS_6IntValE';

create function {database}.constant_timestamp() returns timestamp
location '{location}' symbol='ConstantTimestamp';

create function {database}.validate_arg_type(string) returns boolean
location '{location}' symbol='ValidateArgType';

create function {database}.count_rows() returns bigint
location '{location}' symbol='Count'
prepare_fn='_Z12CountPreparePN10impala_udf15FunctionContextENS0_18FunctionStateScopeE'
close_fn='_Z10CountClosePN10impala_udf15FunctionContextENS0_18FunctionStateScopeE';

create function {database}.constant_arg(int) returns int
location '{location}' symbol="ConstantArg"
prepare_fn='_Z18ConstantArgPreparePN10impala_udf15FunctionContextENS0_18FunctionStateScopeE'
close_fn='_Z16ConstantArgClosePN10impala_udf15FunctionContextENS0_18FunctionStateScopeE';

create function {database}.validate_open(int) returns boolean
location '{location}' symbol='ValidateOpen'
prepare_fn='_Z19ValidateOpenPreparePN10impala_udf15FunctionContextENS0_18FunctionStateScopeE'
close_fn='_Z17ValidateOpenClosePN10impala_udf15FunctionContextENS0_18FunctionStateScopeE';
"""
