#!/usr/bin/env python
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
from copy import deepcopy
import pytest
from subprocess import check_call
from pytest import skip

from tests.beeswax.impala_beeswax import ImpalaBeeswaxException
from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.skip import (
    SkipIfIsilon,
    SkipIfS3,
    SkipIfABFS,
    SkipIfADLS,
    SkipIfEC,
    SkipIfLocal,
    SkipIfNotHdfsMinicluster,
    SkipIfHive3
    )
from tests.common.test_vector import ImpalaTestDimension
from tests.util.filesystem_utils import WAREHOUSE, get_fs_path, IS_HDFS

class TestNestedTypes(ImpalaTestSuite):
  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestNestedTypes, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_constraint(lambda v:
        v.get_value('table_format').file_format in ['parquet', 'orc'])

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
    db_suffix = vector.get_value('table_format').db_suffix()
    self.run_test_case('QueryTest/nested-types-subplan', vector,
                       use_db='tpch_nested' + db_suffix)

  def test_subplan_single_node(self, vector):
    """Test subplans with various exec nodes inside it and num_nodes=1."""
    new_vector = deepcopy(vector)
    new_vector.get_value('exec_option')['num_nodes'] = 1
    self.run_test_case('QueryTest/nested-types-subplan-single-node', new_vector)

  def test_with_clause(self, vector):
    """Queries using nested types and with WITH clause."""
    db_suffix = vector.get_value('table_format').db_suffix()
    self.run_test_case('QueryTest/nested-types-with-clause', vector,
                       use_db='tpch_nested' + db_suffix)

  def test_tpch(self, vector):
    """Queries over the larger nested TPCH dataset."""
    db_suffix = vector.get_value('table_format').db_suffix()
    self.run_test_case('QueryTest/nested-types-tpch', vector,
                       use_db='tpch_nested' + db_suffix)

  def test_tpch_limit(self, vector):
    """Queries over the larger nested TPCH dataset with limits in their subplan."""
    vector.get_value('exec_option')['batch_size'] = 10
    db_suffix = vector.get_value('table_format').db_suffix()
    self.run_test_case('QueryTest/nested-types-tpch-limit', vector,
                       use_db='tpch_nested' + db_suffix)

  @SkipIfNotHdfsMinicluster.tuned_for_minicluster
  def test_tpch_mem_limit(self, vector):
    """Queries over the larger nested TPCH dataset with memory limits tuned for
    a 3-node HDFS minicluster."""
    db_suffix = vector.get_value('table_format').db_suffix()
    self.run_test_case('QueryTest/nested-types-tpch-mem-limit', vector,
                       use_db='tpch_nested' + db_suffix)

  @SkipIfNotHdfsMinicluster.tuned_for_minicluster
  def test_tpch_mem_limit_single_node(self, vector):
    """Queries over the larger nested TPCH dataset with memory limits tuned for
    a 3-node HDFS minicluster with num_nodes=1."""
    new_vector = deepcopy(vector)
    new_vector.get_value('exec_option')['num_nodes'] = 1
    if vector.get_value('table_format').file_format == 'orc':
      # IMPALA-8336: lower memory limit for ORC
      new_vector.get_value('exec_option')['mem_limit'] = '20M'
    else:
      new_vector.get_value('exec_option')['mem_limit'] = '28M'
    db_suffix = vector.get_value('table_format').db_suffix()
    self.run_test_case('QueryTest/nested-types-tpch-mem-limit-single-node',
                       new_vector, use_db='tpch_nested' + db_suffix)

  @SkipIfEC.fix_later
  def test_parquet_stats(self, vector):
    """Queries that test evaluation of Parquet row group statistics."""
    if vector.get_value('table_format').file_format == 'orc':
      pytest.skip('Predicate push down on ORC stripe statistics is not supported' +
                  '(IMPALA-6505)')
    self.run_test_case('QueryTest/nested-types-parquet-stats', vector)

  @SkipIfIsilon.hive
  @SkipIfS3.hive
  @SkipIfABFS.hive
  @SkipIfADLS.hive
  @SkipIfLocal.hive
  def test_upper_case_field_name(self, unique_database):
    """IMPALA-5994: Tests that a Hive-created table with a struct field name with upper
    case characters can be selected."""
    table_name = "%s.upper_case_test" % unique_database
    create_table = "CREATE TABLE %s (s struct<upperCaseName:int>) STORED AS PARQUET" % \
        table_name
    self.run_stmt_in_hive(create_table)
    self.client.execute("invalidate metadata %s" % table_name)
    self.client.execute("select s.UppercasenamE from %s" % table_name)
    self.client.execute("select s.* from %s" % table_name)

  def test_partitioned_table(self, vector, unique_database):
    """IMPALA-6370: Test that a partitioned table with nested types can be scanned."""
    table = "complextypes_partitioned"
    db_table = "{0}.{1}".format(unique_database, table)
    table_format_info = vector.get_value('table_format')  # type: TableFormatInfo
    file_format = table_format_info.file_format
    db_suffix = table_format_info.db_suffix()
    self.client.execute("""
        CREATE EXTERNAL TABLE {0} (
          id BIGINT,
          int_array ARRAY<INT>,
          int_array_array ARRAY<ARRAY<INT>>,
          int_map MAP<STRING,INT>,
          int_map_array ARRAY<MAP<STRING,INT>>,
          nested_struct STRUCT<
              a:INT,
              b:ARRAY<INT>,
              c:STRUCT<d:ARRAY<ARRAY<STRUCT<e:INT,f:STRING>>>>,
              g:MAP<STRING,STRUCT<h:STRUCT<i:ARRAY<DOUBLE>>>>>
        )
        PARTITIONED BY (
          part int
        )
        STORED AS {1}""".format(db_table, file_format))
    # Add multiple partitions pointing to the complextypes_tbl data.
    for partition in [1, 2]:
      self.client.execute("ALTER TABLE {0} ADD PARTITION(part={1}) LOCATION '{2}'".format(
          db_table, partition,
          self._get_table_location("functional%s.complextypestbl" % db_suffix, vector)))
    self.run_test_case('QueryTest/nested-types-basic-partitioned', vector,
        unique_database)

class TestParquetArrayEncodings(ImpalaTestSuite):
  TESTFILE_DIR = os.path.join(os.environ['IMPALA_HOME'],
                              "testdata/parquet_nested_types_encodings")

  ARRAY_RESOLUTION_POLICIES = ["three_level", "two_level", "two_level_then_three_level"]

  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestParquetArrayEncodings, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(ImpalaTestDimension(
      'parquet_array_resolution', *TestParquetArrayEncodings.ARRAY_RESOLUTION_POLICIES))
    cls.ImpalaTestMatrix.add_constraint(lambda v:
        v.get_value('table_format').file_format == 'parquet')

  def __init_arr_res(self, vector):
    arr_res = vector.get_value('parquet_array_resolution')
    qopts = vector.get_value('exec_option')
    qopts['parquet_array_resolution'] = arr_res
    return (arr_res, qopts)

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
  def test_single_field_group_in_list(self, vector, unique_database):
    self.__test_single_field_group_in_list(unique_database, "SingleFieldGroupInList",
      "SingleFieldGroupInList.parquet", vector)

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
  def test_avro_single_field_group_in_list(self, vector, unique_database):
    self.__test_single_field_group_in_list(unique_database, "AvroSingleFieldGroupInList",
      "AvroSingleFieldGroupInList.parquet", vector)

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
  def test_thrift_single_field_group_in_list(self, vector, unique_database):
    self.__test_single_field_group_in_list(unique_database,
      "ThriftSingleFieldGroupInList", "ThriftSingleFieldGroupInList.parquet", vector)

  def __test_single_field_group_in_list(self, db, tbl, parq_file, vector):
    arr_res, qopts = self.__init_arr_res(vector)
    full_name = "%s.%s" % (db, tbl)
    if arr_res == "two_level" or arr_res == "two_level_then_three_level":
      self._create_test_table(db, tbl, parq_file, "col1 array<struct<f1: bigint>>")
      result = self.execute_query("select item.f1 from %s.col1" % full_name, qopts)
      assert result.data == ['1234', '2345']
      result = self.execute_query("select item.f1 from %s t, t.col1" % full_name, qopts)
      assert result.data == ['1234', '2345']

    if arr_res == "three_level":
      self._create_test_table(db, tbl, parq_file, "col1 array<bigint>")
      result = self.execute_query("select item from %s.col1" % full_name, qopts)
      assert result.data == ['1234', '2345']
      result = self.execute_query("select item from %s t, t.col1" % full_name, qopts)
      assert result.data == ['1234', '2345']

    result = self.execute_query(
      "select cnt from %s t, (select count(*) cnt from t.col1) v" % full_name, qopts)
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
  @SkipIfS3.eventually_consistent
  def test_avro_primitive_in_list(self, vector, unique_database):
    self.__test_primitive_in_list(unique_database, "AvroPrimitiveInList",
      "AvroPrimitiveInList.parquet", vector)

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
  def test_thrift_primitive_in_list(self, vector, unique_database):
    self.__test_primitive_in_list(unique_database, "ThriftPrimitiveInList",
      "ThriftPrimitiveInList.parquet", vector)

  def __test_primitive_in_list(self, db, tbl, parq_file, vector):
    arr_res, qopts = self.__init_arr_res(vector)
    full_name = "%s.%s" % (db, tbl)
    self._create_test_table(db, tbl, parq_file, "col1 array<int>")

    if arr_res == "two_level" or arr_res == "two_level_then_three_level":
      result = self.execute_query("select item from %s.col1" % full_name, qopts)
      assert result.data == ['34', '35', '36']
      result = self.execute_query("select item from %s t, t.col1" % full_name, qopts)
      assert result.data == ['34', '35', '36']

    if arr_res == "three_level":
      result = self.execute_query("select item from %s.col1" % full_name, qopts)
      assert result.data == ['NULL', 'NULL', 'NULL']
      result = self.execute_query("select item from %s t, t.col1" % full_name, qopts)
      assert result.data == ['NULL', 'NULL', 'NULL']

    result = self.execute_query(
      "select cnt from %s t, (select count(*) cnt from t.col1) v" % full_name, qopts)
    assert result.data == ['3']

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
  def test_avro_array_of_arrays(self, vector, unique_database):
    self.__test_array_of_arrays(unique_database, "AvroArrayOfArrays",
      "bad-avro.parquet", vector, 10)

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
  @SkipIfS3.eventually_consistent
  def test_thrift_array_of_arrays(self, vector, unique_database):
    self.__test_array_of_arrays(unique_database, "ThriftArrayOfArrays",
      "bad-thrift.parquet", vector, 1)

  def __test_array_of_arrays(self, db, tbl, parq_file, vector, mult):
    arr_res, qopts = self.__init_arr_res(vector)
    full_name = "%s.%s" % (db, tbl)
    self._create_test_table(db, tbl, parq_file, "col1 array<array<int>>")

    if arr_res == "two_level" or arr_res == "two_level_then_three_level":
      result = self.execute_query("select item from %s.col1.item" % full_name, qopts)
      assert result.data == ['0', '1', '2', '3', '4', '5', '6', '7', '8'] * mult
      result = self.execute_query(
        "select a2.item from %s t, t.col1 a1, a1.item a2" % full_name)
      assert result.data == ['0', '1', '2', '3', '4', '5', '6', '7', '8'] * mult
      result = self.execute_query(
        "select cnt from %s t, (select count(*) cnt from t.col1) v" % full_name, qopts)
      assert result.data == ['3'] * mult
      result = self.execute_query(
        "select cnt from %s t, t.col1 a1, (select count(*) cnt from a1.item) v"\
        % full_name, qopts)
      assert result.data == ['3', '3', '3'] * mult

    if arr_res == "three_level":
      expected_err = "has an incompatible Parquet schema"
      try:
        self.execute_query("select item from %s.col1.item" % full_name, qopts)
      except Exception, e:
        assert expected_err in str(e)
      try:
        self.execute_query("select cnt from %s t, (select count(*) cnt from t.col1) v"\
          % full_name, qopts)
      except Exception, e:
        assert expected_err in str(e)

  # $ parquet-tools schema UnannotatedListOfPrimitives.parquet
  # message UnannotatedListOfPrimitives {
  #   repeated int32 list_of_ints;
  # }
  #
  # $ parquet-tools cat UnannotatedListOfPrimitives.parquet
  # list_of_ints = 34
  # list_of_ints = 35
  # list_of_ints = 36
  def test_unannotated_list_of_primitives(self, vector, unique_database):
    arr_res, qopts = self.__init_arr_res(vector)
    tablename = "UnannotatedListOfPrimitives"
    full_name = "%s.%s" % (unique_database, tablename)
    self._create_test_table(unique_database, tablename,
        "UnannotatedListOfPrimitives.parquet", "col1 array<int>")

    result = self.execute_query("select item from %s.col1" % full_name, qopts)
    assert result.data == ['34', '35', '36']
    result = self.execute_query("select item from %s t, t.col1" % full_name, qopts)
    assert result.data == ['34', '35', '36']
    result = self.execute_query(
      "select cnt from %s t, (select count(*) cnt from t.col1) v" % full_name, qopts)
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
  def test_unannotated_list_of_groups(self, vector, unique_database):
    arr_res, qopts = self.__init_arr_res(vector)
    tablename = "UnannotatedListOfGroups"
    full_name = "%s.%s" % (unique_database, tablename)
    self._create_test_table(unique_database, tablename,
        "UnannotatedListOfGroups.parquet", "col1 array<struct<f1: float, f2: float>>")

    result = self.execute_query("select f1, f2 from %s.col1" % full_name, qopts)
    assert result.data == ['1\t1', '2\t2']
    result = self.execute_query("select f1, f2 from %s t, t.col1" % full_name, qopts)
    assert result.data == ['1\t1', '2\t2']
    result = self.execute_query(
      "select cnt from %s t, (select count(*) cnt from t.col1) v" % full_name, qopts)
    assert result.data == ['2']

  # $ parquet-tools schema AmbiguousList_Modern.parquet
  # message org.apache.impala.nested {
  #   required group ambigArray (LIST) {
  #     repeated group list {
  #       required group element {
  #         required group s2 {
  #           optional int32 f21;
  #           optional int32 f22;
  #         }
  #         optional int32 F11;
  #         optional int32 F12;
  #       }
  #     }
  #   }
  # }
  # $ parquet-tools cat AmbiguousList_Modern.parquet
  # ambigArray:
  # .list:
  # ..element:
  # ...s2:
  # ....f21 = 21
  # ....f22 = 22
  # ...F11 = 11
  # ...F12 = 12
  # .list:
  # ..element:
  # ...s2:
  # ....f21 = 210
  # ....f22 = 220
  # ...F11 = 110
  # ...F12 = 120
  #
  # $ parquet-tools schema AmbiguousList_Legacy.parquet
  # message org.apache.impala.nested {
  #  required group ambigArray (LIST) {
  #    repeated group array {
  #       required group s2 {
  #         optional int32 f21;
  #         optional int32 f22;
  #       }
  #       optional int32 F11;
  #       optional int32 F12;
  #     }
  #   }
  # }
  # $ parquet-tools cat AmbiguousList_Legacy.parquet
  # ambigArray:
  # .array:
  # ..s2:
  # ...f21 = 21
  # ...f22 = 22
  # ..F11 = 11
  # ..F12 = 12
  # .array:
  # ..s2:
  # ...f21 = 210
  # ...f22 = 220
  # ..F11 = 110
  # ..F12 = 120
  @SkipIfS3.eventually_consistent
  def test_ambiguous_list(self, vector, unique_database):
    """IMPALA-4725: Tests the schema-resolution behavior with different values for the
    PARQUET_ARRAY_RESOLUTION and PARQUET_FALLBACK_SCHEMA_RESOLUTION query options.
    The schema of the Parquet test files is constructed to induce incorrect results
    with index-based resolution and the default TWO_LEVEL_THEN_THREE_LEVEL array
    resolution policy. Regardless of whether the Parquet data files use the 2-level or
    3-level encoding, incorrect results may be returned if the array resolution does
    not exactly match the data files'. The name-based policy generally does not have
    this problem because it avoids traversing incorrect schema paths.
    """

    # The Parquet resolution policy is manually set in the .test files.
    if vector.get_value('parquet_array_resolution') != "three_level":
      skip("Test only run with three_level")

    ambig_modern_tbl = "ambig_modern"
    self._create_test_table(unique_database, ambig_modern_tbl,
        "AmbiguousList_Modern.parquet",
        "ambigarray array<struct<s2:struct<f21:int,f22:int>,f11:int,f12:int>>")
    self.run_test_case('QueryTest/parquet-ambiguous-list-modern',
                        vector, unique_database)

    ambig_legacy_tbl = "ambig_legacy"
    self._create_test_table(unique_database, ambig_legacy_tbl,
        "AmbiguousList_Legacy.parquet",
        "ambigarray array<struct<s2:struct<f21:int,f22:int>,f11:int,f12:int>>")
    self.run_test_case('QueryTest/parquet-ambiguous-list-legacy',
                        vector, unique_database)

  def _create_test_table(self, dbname, tablename, filename, columns):
    """Creates a table in the given database with the given name and columns. Copies
    the file with the given name from TESTFILE_DIR into the table."""
    location = get_fs_path("/test-warehouse/%s.db/%s" % (dbname, tablename))
    self.client.execute("create table %s.%s (%s) stored as parquet location '%s'" %
                        (dbname, tablename, columns, location))
    local_path = self.TESTFILE_DIR + "/" + filename
    check_call(["hadoop", "fs", "-put", local_path, location], shell=False)

class TestMaxNestingDepth(ImpalaTestSuite):
  # Should be kept in sync with the FE's Type.MAX_NESTING_DEPTH
  MAX_NESTING_DEPTH = 100
  TABLES = ['struct', 'int_array', 'struct_array', 'int_map', 'struct_map']
  TEMP_TABLE_SUFFIX = '_parquet'

  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestMaxNestingDepth, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_constraint(lambda v:
        v.get_value('table_format').file_format in ['parquet', 'orc'])

  @SkipIfHive3.slow_nested_types
  def test_max_nesting_depth(self, vector, unique_database):
    """Tests that Impala can scan Parquet and ORC files having complex types of
    the maximum nesting depth."""
    file_format = vector.get_value('table_format').file_format
    if file_format == 'orc' and not IS_HDFS:
      pytest.skip('Orc table loading needs Hive and thus only works with HDFS.')

    if file_format == 'parquet':
      self.__create_parquet_tables(unique_database)
    elif file_format == 'orc':
      self.__create_orc_tables(unique_database)
    self.run_test_case('QueryTest/max-nesting-depth', vector, unique_database)

  def __create_parquet_tables(self, unique_database, as_target=True):
    """Create Parquet tables from files. If 'as_target' is False, the Parquet tables will
     be used to create ORC tables, so we add a suffix in the table names."""
    check_call(["hdfs", "dfs", "-copyFromLocal",
      "%s/testdata/max_nesting_depth" % os.environ['IMPALA_HOME'],
      "%s/%s.db/" % (WAREHOUSE, unique_database)], shell=False)
    tbl_suffix = '' if as_target else self.TEMP_TABLE_SUFFIX
    for tbl in self.TABLES:
      tbl_name = "%s.%s_tbl%s" % (unique_database, tbl, tbl_suffix)
      tbl_location = "%s/%s.db/max_nesting_depth/%s/" % (WAREHOUSE, unique_database, tbl)
      create_table = "CREATE EXTERNAL TABLE %s LIKE PARQUET '%s' STORED AS PARQUET" \
          " location '%s'" % (tbl_name, tbl_location + 'file.parq', tbl_location)
      self.client.execute(create_table)

  def __create_orc_tables(self, unique_database):
    # Creating ORC tables from ORC files (IMPALA-8046) has not been supported.
    # We create the Parquet tables first and then transform them into ORC tables.
    self.__create_parquet_tables(unique_database, False)
    for tbl in self.TABLES:
      tbl_name = "%s.%s_tbl" % (unique_database, tbl)
      from_tbl_name = tbl_name + self.TEMP_TABLE_SUFFIX
      create_table = "CREATE TABLE %s LIKE %s STORED AS ORC" % (tbl_name, from_tbl_name)
      insert_table = "INSERT INTO %s SELECT * FROM %s" % (tbl_name, from_tbl_name)
      self.run_stmt_in_hive(create_table)
      self.run_stmt_in_hive(insert_table)
      self.client.execute("INVALIDATE METADATA %s" % tbl_name)

  @SkipIfIsilon.hive
  @SkipIfS3.hive
  @SkipIfABFS.hive
  @SkipIfADLS.hive
  @SkipIfLocal.hive
  def test_load_hive_table(self, vector, unique_database):
    """Tests that Impala rejects Hive-created tables with complex types that exceed
    the maximum nesting depth."""
    # Type with a nesting depth of MAX_NESTING_DEPTH + 1
    type_sql = ("array<" * self.MAX_NESTING_DEPTH) + "int" +\
      (">" * self.MAX_NESTING_DEPTH)
    file_format = vector.get_value('table_format').file_format
    create_table_sql = "CREATE TABLE %s.above_max_depth (f %s) STORED AS %s" %\
      (unique_database, type_sql, file_format)
    self.run_stmt_in_hive(create_table_sql)
    self.client.execute("invalidate metadata %s.above_max_depth" % unique_database)
    try:
      self.client.execute("explain select 1 from %s.above_max_depth" % unique_database)
      assert False, "Expected table loading to fail."
    except ImpalaBeeswaxException, e:
      assert "Type exceeds the maximum nesting depth" in str(e)
