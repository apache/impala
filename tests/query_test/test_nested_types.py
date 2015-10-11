#!/usr/bin/env python
# Copyright (c) 2015 Cloudera, Inc. All rights reserved.

import os
import random
from subprocess import check_call

import pytest
from tests.beeswax.impala_beeswax import ImpalaBeeswaxException
from tests.common.test_vector import *
from tests.common.impala_test_suite import *
from tests.common.skip import SkipIfOldAggsJoins, SkipIfIsilon, SkipIfS3
from tests.util.filesystem_utils import WAREHOUSE, get_fs_path
from subprocess import call, check_call

@SkipIfOldAggsJoins.nested_types
class TestNestedTypes(ImpalaTestSuite):
  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestNestedTypes, cls).add_test_dimensions()
    cls.TestMatrix.add_constraint(lambda v:
        v.get_value('table_format').file_format == 'parquet')

  def test_scanner_basic(self, vector):
    """Queries that do not materialize arrays."""
    self.run_test_case('QueryTest/nested-types-scanner-basic', vector)

  def test_scanner_array_materialization(self, vector):
    """Queries that materialize arrays."""
    self.run_test_case('QueryTest/nested-types-scanner-array-materialization', vector)

  def test_scanner_multiple_materialization(self, vector):
    """Queries that materialize the same array multiple times."""
    self.run_test_case('QueryTest/nested-types-scanner-multiple-materialization', vector)

  def test_scanner_position(self, vector):
    """Queries that materialize the artifical position element."""
    self.run_test_case('QueryTest/nested-types-scanner-position', vector)

  def test_scanner_map(self, vector):
    """Queries that materialize maps. (Maps looks like arrays of key/value structs, so
    most map functionality is already tested by the array tests.)"""
    self.run_test_case('QueryTest/nested-types-scanner-maps', vector)

  def test_runtime(self, vector):
    """Queries that send collections through the execution runtime."""
    self.run_test_case('QueryTest/nested-types-runtime', vector)

  def test_subplan(self, vector):
    """Test subplans with various exec nodes inside it."""
    self.run_test_case('QueryTest/nested-types-subplan', vector)

  def test_with_clause(self, vector):
    """Queries using nested types and with WITH clause."""
    self.run_test_case('QueryTest/nested-types-with-clause', vector)

  def test_tpch(self, vector):
    """Queries over the larger nested TPCH dataset."""
    self.run_test_case('QueryTest/nested-types-tpch', vector)

@SkipIfOldAggsJoins.nested_types
class TestParquetArrayEncodings(ImpalaTestSuite):
  # Create a unique database name so we can run multiple instances of this test class in
  # parallel
  DATABASE = "test_parquet_list_encodings_" + str(random.randint(0, 10**10))
  TESTFILE_DIR = os.environ['IMPALA_HOME'] + "/testdata/parquet_nested_types_encodings"

  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestParquetArrayEncodings, cls).add_test_dimensions()
    cls.TestMatrix.add_constraint(lambda v:
        v.get_value('table_format').file_format == 'parquet')

  @classmethod
  def setup_class(cls):
    super(TestParquetArrayEncodings, cls).setup_class()
    cls.cleanup_db(cls.DATABASE)
    cls.client.execute("create database if not exists " + cls.DATABASE)

  @classmethod
  def teardown_class(cls):
    cls.cleanup_db(cls.DATABASE)
    super(TestParquetArrayEncodings, cls).teardown_class()

  # $ parquet-tools schema SingleFieldGroupInList.parquet
  # message SingleFieldGroupInList {
  #   optional group single_element_groups (LIST) {
  #     repeated group single_element_group {
  #       required int64 count;
  #     }
  #   }
  # }
  #
  # $ parquet-tools cat SingleFieldGroupInList.parquet
  # single_element_groups:
  # .single_element_group:
  # ..count = 1234
  # .single_element_group:
  # ..count = 2345
  def test_single_field_group_in_list(self, vector):
    tablename = self._create_test_table("SingleFieldGroupInList",
        "SingleFieldGroupInList.parquet", "col1 array<struct<count: bigint>>")

    full_name = "%s.%s" % (self.DATABASE, tablename)

    result = self.client.execute("select item.count from %s.col1" % full_name)
    assert len(result.data) == 2
    assert result.data == ['1234', '2345']

    result = self.client.execute("select item.count from %s t, t.col1" % full_name)
    assert len(result.data) == 2
    assert result.data == ['1234', '2345']

    result = self.client.execute(
      "select cnt from %s t, (select count(*) cnt from t.col1) v" % full_name)
    assert len(result.data) == 1
    assert result.data == ['2']

  # $ parquet-tools schema AvroPrimitiveInList.parquet
  # message AvroPrimitiveInList {
  #   required group list_of_ints (LIST) {
  #     repeated int32 array;
  #   }
  # }
  #
  # $ parquet-tools cat AvroPrimitiveInList.parquet
  # list_of_ints:
  # .array = 34
  # .array = 35
  # .array = 36
  def test_avro_primitive_in_list(self, vector):
    tablename = self._create_test_table("AvroPrimitiveInList",
        "AvroPrimitiveInList.parquet", "col1 array<int>")

    full_name = "%s.%s" % (self.DATABASE, tablename)

    result = self.client.execute("select item from %s.col1" % full_name)
    assert len(result.data) == 3
    assert result.data == ['34', '35', '36']

    result = self.client.execute("select item from %s t, t.col1" % full_name)
    assert len(result.data) == 3
    assert result.data == ['34', '35', '36']

    result = self.client.execute(
      "select cnt from %s t, (select count(*) cnt from t.col1) v" % full_name)
    assert len(result.data) == 1
    assert result.data == ['3']

  # $ parquet-tools schema AvroSingleFieldGroupInList.parquet
  # message AvroSingleFieldGroupInList {
  #   optional group single_element_groups (LIST) {
  #     repeated group array {
  #       required int64 count;
  #     }
  #   }
  # }
  #
  # $ parquet-tools cat AvroSingleFieldGroupInList.parquet
  # single_element_groups:
  # .array:
  # ..count = 1234
  # .array:
  # ..count = 2345
  def test_avro_single_field_group_in_list(self, vector):
# Note that the field name does not match the field name in the file schema.
    tablename = self._create_test_table("AvroSingleFieldGroupInList",
        "AvroSingleFieldGroupInList.parquet", "col1 array<struct<f1: bigint>>")

    full_name = "%s.%s" % (self.DATABASE, tablename)

    result = self.client.execute("select item.f1 from %s.col1" % full_name)
    assert len(result.data) == 2
    assert result.data == ['1234', '2345']

    result = self.client.execute("select item.f1 from %s t, t.col1" % full_name)
    assert len(result.data) == 2
    assert result.data == ['1234', '2345']

    result = self.client.execute(
      "select cnt from %s t, (select count(*) cnt from t.col1) v" % full_name)
    assert len(result.data) == 1
    assert result.data == ['2']

  # $ parquet-tools schema bad-avro.parquet 
  # message org.apache.spark.sql.execution.datasources.parquet.test.avro.AvroArrayOfArray {
  #   required group int_arrays_column (LIST) {
  #     repeated group array (LIST) {
  #       repeated int32 array;
  #     }
  #   }
  # }
  # 
  # $ parquet-tools cat bad-avro.parquet 
  # int_arrays_column:
  # .array:
  # ..array = 0
  # ..array = 1
  # ..array = 2
  # .array:
  # ..array = 3
  # ..array = 4
  # ..array = 5
  # .array:
  # ..array = 6
  # ..array = 7
  # ..array = 8
  # 
  # int_arrays_column:
  # .array:
  # ..array = 0
  # ..array = 1
  # ..array = 2
  # .array:
  # ..array = 3
  # ..array = 4
  # ..array = 5
  # .array:
  # ..array = 6
  # ..array = 7
  # ..array = 8
  # 
  # [Same int_arrays_column repeated 8x more]
  def test_avro_array_of_arrays(self, vector):
    tablename = self._create_test_table("AvroArrayOfArrays", "bad-avro.parquet",
                                        "col1 array<array<int>>")

    full_name = "%s.%s" % (self.DATABASE, tablename)

    result = self.client.execute("select item from %s.col1.item" % full_name)
    assert len(result.data) == 9 * 10
    assert result.data == ['0', '1', '2', '3', '4', '5', '6', '7', '8'] * 10

    result = self.client.execute(
      "select a2.item from %s t, t.col1 a1, a1.item a2" % full_name)
    assert len(result.data) == 9 * 10
    assert result.data == ['0', '1', '2', '3', '4', '5', '6', '7', '8'] * 10

    result = self.client.execute(
      "select cnt from %s t, (select count(*) cnt from t.col1) v" % full_name)
    assert len(result.data) == 10
    assert result.data == ['3'] * 10

    result = self.client.execute(
      "select cnt from %s t, t.col1 a1, (select count(*) cnt from a1.item) v" % full_name)
    assert len(result.data) == 3 * 10
    assert result.data == ['3', '3', '3'] * 10

  # $ parquet-tools schema ThriftPrimitiveInList.parquet
  # message ThriftPrimitiveInList {
  #   required group list_of_ints (LIST) {
  #     repeated int32 list_of_ints_tuple;
  #   }
  # }
  #
  # $ parquet-tools cat ThriftPrimitiveInList.parquet
  # list_of_ints:
  # .list_of_ints_tuple = 34
  # .list_of_ints_tuple = 35
  # .list_of_ints_tuple = 36
  def test_thrift_primitive_in_list(self, vector):
    tablename = self._create_test_table("ThriftPrimitiveInList",
        "ThriftPrimitiveInList.parquet", "col1 array<int>")

    full_name = "%s.%s" % (self.DATABASE, tablename)

    result = self.client.execute("select item from %s.col1" % full_name)
    assert len(result.data) == 3
    assert result.data == ['34', '35', '36']

    result = self.client.execute("select item from %s t, t.col1" % full_name)
    assert len(result.data) == 3
    assert result.data == ['34', '35', '36']

    result = self.client.execute(
      "select cnt from %s t, (select count(*) cnt from t.col1) v" % full_name)
    assert len(result.data) == 1
    assert result.data == ['3']

  # $ parquet-tools schema ThriftSingleFieldGroupInList.parquet
  # message ThriftSingleFieldGroupInList {
  #   optional group single_element_groups (LIST) {
  #     repeated group single_element_groups_tuple {
  #       required int64 count;
  #     }
  #   }
  # }
  #
  # $ parquet-tools cat ThriftSingleFieldGroupInList.parquet
  # single_element_groups:
  # .single_element_groups_tuple:
  # ..count = 1234
  # .single_element_groups_tuple:
  # ..count = 2345
  def test_thrift_single_field_group_in_list(self, vector):
    tablename = self._create_test_table("ThriftSingleFieldGroupInList",
        "ThriftSingleFieldGroupInList.parquet", "col1 array<struct<f1: bigint>>")

    full_name = "%s.%s" % (self.DATABASE, tablename)

    result = self.client.execute("select item.f1 from %s.col1" % full_name)
    assert len(result.data) == 2
    assert result.data == ['1234', '2345']

    result = self.client.execute("select item.f1 from %s t, t.col1" % full_name)
    assert len(result.data) == 2
    assert result.data == ['1234', '2345']

    result = self.client.execute(
      "select cnt from %s t, (select count(*) cnt from t.col1) v" % full_name)
    assert len(result.data) == 1
    assert result.data == ['2']

  # $ parquet-tools schema bad-thrift.parquet 
  # message ParquetSchema {
  #   required group intListsColumn (LIST) {
  #     repeated group intListsColumn_tuple (LIST) {
  #       repeated int32 intListsColumn_tuple_tuple;
  #     }
  #   }
  # }
  # 
  # $ parquet-tools cat bad-thrift.parquet 
  # intListsColumn:
  # .intListsColumn_tuple:
  # ..intListsColumn_tuple_tuple = 0
  # ..intListsColumn_tuple_tuple = 1
  # ..intListsColumn_tuple_tuple = 2
  # .intListsColumn_tuple:
  # ..intListsColumn_tuple_tuple = 3
  # ..intListsColumn_tuple_tuple = 4
  # ..intListsColumn_tuple_tuple = 5
  # .intListsColumn_tuple:
  # ..intListsColumn_tuple_tuple = 6
  # ..intListsColumn_tuple_tuple = 7
  # ..intListsColumn_tuple_tuple = 8
  def test_thrift_array_of_arrays(self, vector):
    tablename = self._create_test_table("ThriftArrayOfArrays", "bad-thrift.parquet",
                                        "col1 array<array<int>>")

    full_name = "%s.%s" % (self.DATABASE, tablename)

    result = self.client.execute("select item from %s.col1.item" % full_name)
    assert len(result.data) == 9
    assert result.data == ['0', '1', '2', '3', '4', '5', '6', '7', '8']

    result = self.client.execute(
      "select a2.item from %s t, t.col1 a1, a1.item a2" % full_name)
    assert len(result.data) == 9
    assert result.data == ['0', '1', '2', '3', '4', '5', '6', '7', '8']

    result = self.client.execute(
      "select cnt from %s t, (select count(*) cnt from t.col1) v" % full_name)
    assert len(result.data) == 1
    assert result.data == ['3']

    result = self.client.execute(
      "select cnt from %s t, t.col1 a1, (select count(*) cnt from a1.item) v" % full_name)
    assert len(result.data) == 3
    assert result.data == ['3', '3', '3']

  # $ parquet-tools schema UnannotatedListOfPrimitives.parquet
  # message UnannotatedListOfPrimitives {
  #   repeated int32 list_of_ints;
  # }
  #
  # $ parquet-tools cat UnannotatedListOfPrimitives.parquet
  # list_of_ints = 34
  # list_of_ints = 35
  # list_of_ints = 36
  def test_unannotated_list_of_primitives(self, vector):
    tablename = self._create_test_table("UnannotatedListOfPrimitives",
        "UnannotatedListOfPrimitives.parquet", "col1 array<int>")

    full_name = "%s.%s" % (self.DATABASE, tablename)

    result = self.client.execute("select item from %s.col1" % full_name)
    assert len(result.data) == 3
    assert result.data == ['34', '35', '36']

    result = self.client.execute("select item from %s t, t.col1" % full_name)
    assert len(result.data) == 3
    assert result.data == ['34', '35', '36']

    result = self.client.execute(
      "select cnt from %s t, (select count(*) cnt from t.col1) v" % full_name)
    assert len(result.data) == 1
    assert result.data == ['3']

  # $ parquet-tools schema UnannotatedListOfGroups.parquet
  # message UnannotatedListOfGroups {
  #   repeated group list_of_points {
  #     required float x;
  #     required float y;
  #   }
  # }
  #
  # $ parquet-tools cat UnannotatedListOfGroups.parquet
  # list_of_points:
  # .x = 1.0
  # .y = 1.0
  # list_of_points:
  # .x = 2.0
  # .y = 2.0
  def test_unannotated_list_of_groups(self, vector):
    tablename = self._create_test_table("UnannotatedListOfGroups",
        "UnannotatedListOfGroups.parquet", "col1 array<struct<f1: float, f2: float>>")

    full_name = "%s.%s" % (self.DATABASE, tablename)

    result = self.client.execute("select f1, f2 from %s.col1" % full_name)
    assert len(result.data) == 2
    assert result.data == ['1\t1', '2\t2']

    result = self.client.execute("select f1, f2 from %s t, t.col1" % full_name)
    assert len(result.data) == 2
    assert result.data == ['1\t1', '2\t2']

    result = self.client.execute(
      "select cnt from %s t, (select count(*) cnt from t.col1) v" % full_name)
    assert len(result.data) == 1
    assert result.data == ['2']

  def _create_test_table(self, tablename, filename, columns):
    """Returns a unique tablename based on the input 'tablename'. This allows multiple
    instances of the same test to be run in parallel (e.g. during an exhaustive run)."""
    tablename = "%s_%s" % (tablename, random.randint(0, 10**5))
    location = get_fs_path("/test-warehouse/%s_%s" % (self.DATABASE, tablename))
    self.client.execute("create table %s.%s (%s) stored as parquet location '%s'" %
                          (self.DATABASE, tablename, columns, location))
    local_path = self.TESTFILE_DIR + "/" + filename
    check_call(["hadoop", "fs", "-put", local_path, location], shell=False)
    self.client.execute("invalidate metadata %s.%s" % (self.DATABASE, tablename))
    return tablename

@SkipIfOldAggsJoins.nested_types
class TestMaxNestingDepth(ImpalaTestSuite):
  TEST_DB = 'max_nesting_depth'
  TEST_HDFS_ROOT_DIR = "%s/max_nesting_depth/" % WAREHOUSE

  # Should be kept in sync with the FE's Type.MAX_NESTING_DEPTH
  MAX_NESTING_DEPTH = 100

  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestMaxNestingDepth, cls).add_test_dimensions()
    cls.TestMatrix.add_constraint(lambda v:
        v.get_value('table_format').file_format == 'parquet')

  def setup_method(self, method):
    self.cleanup_db(TestMaxNestingDepth.TEST_DB)
    self.client.execute("create database %s location '%s/%s.db'" %
        (TestMaxNestingDepth.TEST_DB, WAREHOUSE,
        TestMaxNestingDepth.TEST_DB))
    if method.__name__ == "test_max_nesting_depth":
      # Clean up HDFS directory and copy the test files.
      self.__remove_test_hdfs_dir()
      self.__copy_test_hdfs_dir()

  def teardown_method(self, method):
    self.cleanup_db(TestMaxNestingDepth.TEST_DB)
    if method.__name__ == "test_max_nesting_depth":
      self.__remove_test_hdfs_dir()

  def __remove_test_hdfs_dir(self):
    call(["hdfs", "dfs", "-rm", "-r",
      TestMaxNestingDepth.TEST_HDFS_ROOT_DIR], shell=False)

  def __copy_test_hdfs_dir(self):
    check_call(["hdfs", "dfs", "-copyFromLocal",
      "%s/testdata/max_nesting_depth" % os.environ['IMPALA_HOME'],
      WAREHOUSE], shell=False)

  @pytest.mark.execute_serially
  def test_max_nesting_depth(self, vector):
    """Tests that Impala can scan Parquet files having complex types of
    the maximum nesting depth."""
    self.run_test_case('QueryTest/max-nesting-depth', vector)

  @pytest.mark.execute_serially
  @SkipIfIsilon.hive
  @SkipIfS3.hive
  def test_load_hive_table(self, vector):
    """Tests that Impala rejects Hive-created tables with complex types that exceed
    the maximum nesting depth."""
    # Type with a nesting depth of MAX_NESTING_DEPTH + 1
    type_sql = ("array<" * self.MAX_NESTING_DEPTH) + "int" +\
      (">" * self.MAX_NESTING_DEPTH)
    create_table_sql = "CREATE TABLE %s.above_max_depth (f %s) STORED AS PARQUET" %\
      (self.TEST_DB, type_sql)
    check_call(["hive", "-e", create_table_sql], shell=False)
    self.client.execute("invalidate metadata %s.above_max_depth" % self.TEST_DB)
    try:
      self.client.execute("explain select 1 from %s.above_max_depth" % self.TEST_DB)
      assert False, "Expected table loading to fail."
    except ImpalaBeeswaxException, e:
      assert "Type exceeds the maximum nesting depth" in str(e)
