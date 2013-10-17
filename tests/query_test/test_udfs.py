#!/usr/bin/env python
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.

from tests.common.test_vector import *
from tests.common.impala_test_suite import *

class TestUdfs(ImpalaTestSuite):
  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestUdfs, cls).add_test_dimensions()
    # UDFs require codegen
    cls.TestMatrix.add_constraint(
      lambda v: v.get_value('exec_option')['disable_codegen'] == False)
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
    self.run_test_case('QueryTest/uda', vector, use_db=database)

  def test_ir_functions(self, vector):
    database = 'ir_function_test'
    self.__load_functions(
      self.create_udfs_template, vector, database, '/test-warehouse/test-udfs.ll')
    self.run_test_case('QueryTest/udf', vector, use_db=database)

  def test_hive_udfs(self, vector):
    self.run_test_case('QueryTest/load-hive-udfs', vector)
    self.run_test_case('QueryTest/hive-udf', vector)

  def __load_functions(self, template, vector, database, location):
    queries = template.format(database=database, location=location)
    # Split queries and remove empty lines
    queries = [q for q in queries.split(';') if q.strip()]
    exec_options = vector.get_value('exec_option')
    for query in queries:
      if query.strip() == '': continue
      result = self.execute_query_expect_success(IMPALAD, query, exec_options)
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
"""
