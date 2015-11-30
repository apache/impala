# Copyright (c) 2015 Cloudera, Inc. All rights reserved.
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
#
# Tests for IMPALA-1748

import os
from tests.beeswax.impala_beeswax import ImpalaBeeswaxException
from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.util.filesystem_utils import get_fs_path

class TestUdfPersistence(CustomClusterTestSuite):
  """ Tests the behavior of udfs and udas between catalog restarts With IMPALA-1748, these
  functions are persisted to the metastore and are loaded again during catalog startup"""

  DATABASE = 'udf_permanent_test'

  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestUdfPersistence, cls).add_test_dimensions()
    cls.TestMatrix.add_dimension(create_uncompressed_text_dimension(cls.get_workload()))

  def setup_method(self, method):
    super(TestUdfPersistence, self).setup_method(method)
    impalad = self.cluster.impalads[0]
    self.client = impalad.service.create_beeswax_client()
    self.__cleanup()
    self.__load_drop_functions(
        self.CREATE_UDFS_TEMPLATE, self.DATABASE,
        get_fs_path('/test-warehouse/libTestUdfs.so'))
    self.__load_drop_functions(
        self.DROP_SAMPLE_UDAS_TEMPLATE, self.DATABASE,
        get_fs_path('/test-warehouse/libudasample.so'))
    self.__load_drop_functions(
        self.CREATE_SAMPLE_UDAS_TEMPLATE, self.DATABASE,
        get_fs_path('/test-warehouse/libudasample.so'))
    self.__load_drop_functions(
        self.CREATE_TEST_UDAS_TEMPLATE, self.DATABASE,
        get_fs_path('/test-warehouse/libTestUdas.so'))
    self.uda_count =\
        self.CREATE_SAMPLE_UDAS_TEMPLATE.count("create aggregate function") +\
        self.CREATE_TEST_UDAS_TEMPLATE.count("create aggregate function")
    self.udf_count = self.CREATE_UDFS_TEMPLATE.count("create function")

  def teardown_method(self, method):
    self.__cleanup()

  def __cleanup(self):
    self.client.execute("DROP DATABASE IF EXISTS %s CASCADE" % self.DATABASE)

  def __load_drop_functions(self, template, database, location):
    queries = template.format(database=database, location=location)
    # Split queries and remove empty lines
    queries = [q for q in queries.split(';') if q.strip()]
    for query in queries:
      result = self.client.execute(query)
      assert result is not None

  def __restart_cluster(self):
    self._stop_impala_cluster()
    self._start_impala_cluster(list())
    impalad = self.cluster.impalads[0]
    self.client = impalad.service.create_beeswax_client()

  def verify_function_count(self, query, count):
    result = self.client.execute(query)
    assert result is not None and len(result.data) == count

  def test_permanent_udfs(self):
    # Make sure the pre-calculated count tallies with the number of
    # functions shown using "show [aggregate] functions" statement
    self.verify_function_count(
            "SHOW FUNCTIONS in {0}".format(self.DATABASE), self.udf_count);
    self.verify_function_count(
            "SHOW AGGREGATE FUNCTIONS in {0}".format(self.DATABASE), self.uda_count)
    # invalidate metadata and make sure the count tallies
    result = self.client.execute("INVALIDATE METADATA")
    self.verify_function_count(
            "SHOW FUNCTIONS in {0}".format(self.DATABASE), self.udf_count);
    self.verify_function_count(
            "SHOW AGGREGATE FUNCTIONS in {0}".format(self.DATABASE), self.uda_count)
    # Restart the cluster, this triggers a full metadata reload
    self.__restart_cluster()
    # Make sure the counts of udfs and udas match post restart
    self.verify_function_count(
            "SHOW FUNCTIONS in {0}".format(self.DATABASE), self.udf_count);
    self.verify_function_count(
            "SHOW AGGREGATE FUNCTIONS in {0}".format(self.DATABASE), self.uda_count)
    # Drop sample udas and verify the count matches pre and post restart
    self.__load_drop_functions(
        self.DROP_SAMPLE_UDAS_TEMPLATE, self.DATABASE,
        get_fs_path('/test-warehouse/libudasample.so'))
    self.verify_function_count(
            "SHOW AGGREGATE FUNCTIONS in {0}".format(self.DATABASE), 1)
    self.__restart_cluster()
    self.verify_function_count(
            "SHOW AGGREGATE FUNCTIONS in {0}".format(self.DATABASE), 1)

  # Create sample UDA functions in {database} from library {location}

  DROP_SAMPLE_UDAS_TEMPLATE = """
    drop function if exists {database}.test_count(int);
    drop function if exists {database}.hll(int);
    drop function if exists {database}.sum_small_decimal(decimal(9,2));
  """

  CREATE_SAMPLE_UDAS_TEMPLATE = """
    create database if not exists {database};

    create aggregate function {database}.test_count(int) returns bigint
    location '{location}' update_fn='CountUpdate';

    create aggregate function {database}.hll(int) returns string
    location '{location}' update_fn='HllUpdate';

    create aggregate function {database}.sum_small_decimal(decimal(9,2))
    returns decimal(9,2) location '{location}' update_fn='SumSmallDecimalUpdate';
  """

  # Create test UDA functions in {database} from library {location}
  CREATE_TEST_UDAS_TEMPLATE = """
    drop function if exists {database}.trunc_sum(double);

    create database if not exists {database};

    create aggregate function {database}.trunc_sum(double)
    returns bigint intermediate double location '{location}'
    update_fn='TruncSumUpdate' merge_fn='TruncSumMerge'
    serialize_fn='TruncSumSerialize' finalize_fn='TruncSumFinalize';
  """

  # Create test UDF functions in {database} from library {location}
  CREATE_UDFS_TEMPLATE = """
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
  """
