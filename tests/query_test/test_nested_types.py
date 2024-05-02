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
import os
from copy import deepcopy
import pytest

from tests.beeswax.impala_beeswax import ImpalaBeeswaxException
from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.skip import SkipIfFS, SkipIfHive2, SkipIfNotHdfsMinicluster
from tests.common.test_dimensions import (create_exec_option_dimension,
    create_exec_option_dimension_from_dict, create_client_protocol_dimension,
    create_orc_dimension, orc_schema_resolution_constraint)
from tests.common.test_vector import ImpalaTestDimension
from tests.util.filesystem_utils import WAREHOUSE, get_fs_path, IS_HDFS

class TestNestedTypes(ImpalaTestSuite):
  """Functional tests for nested types, run for all file formats that support nested
  types."""
  @classmethod
  def get_workload(self):
    return 'functional-query'

  @staticmethod
  def orc_schema_resolution_constraint(vector):
    """ Constraint to use multiple orc_schema_resolution only in case of orc files"""
    file_format = vector.get_value('table_format').file_format
    orc_schema_resolution = vector.get_value('orc_schema_resolution')
    return file_format == 'orc' or orc_schema_resolution == 0

  @classmethod
  def add_test_dimensions(cls):
    super(TestNestedTypes, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_constraint(lambda v:
        v.get_value('table_format').file_format in ['parquet', 'orc'])
    cls.ImpalaTestMatrix.add_dimension(
        ImpalaTestDimension('mt_dop', 0, 2))
    cls.ImpalaTestMatrix.add_dimension(ImpalaTestDimension('orc_schema_resolution', 0, 1))
    cls.ImpalaTestMatrix.add_constraint(orc_schema_resolution_constraint)

  def test_scanner_basic(self, vector):
    """Queries that do not materialize arrays."""
    vector = deepcopy(vector)
    vector.get_value('exec_option')['mt_dop'] = vector.get_value('mt_dop')
    self.run_test_case('QueryTest/nested-types-scanner-basic', vector)

  def test_scanner_array_materialization(self, vector):
    """Queries that materialize arrays."""
    vector = deepcopy(vector)
    vector.get_value('exec_option')['mt_dop'] = vector.get_value('mt_dop')
    self.run_test_case('QueryTest/nested-types-scanner-array-materialization', vector)

  def test_scanner_multiple_materialization(self, vector):
    """Queries that materialize the same array multiple times."""
    vector = deepcopy(vector)
    vector.get_value('exec_option')['mt_dop'] = vector.get_value('mt_dop')
    self.run_test_case('QueryTest/nested-types-scanner-multiple-materialization', vector)

  def test_scanner_position(self, vector):
    """Queries that materialize the artifical position element."""
    vector = deepcopy(vector)
    vector.get_value('exec_option')['mt_dop'] = vector.get_value('mt_dop')
    self.run_test_case('QueryTest/nested-types-scanner-position', vector)

  def test_scanner_map(self, vector):
    """Queries that materialize maps. (Maps looks like arrays of key/value structs, so
    most map functionality is already tested by the array tests.)"""
    vector = deepcopy(vector)
    vector.get_value('exec_option')['mt_dop'] = vector.get_value('mt_dop')
    self.run_test_case('QueryTest/nested-types-scanner-maps', vector)

  def test_runtime(self, vector):
    """Queries that send collections through the execution runtime."""
    vector = deepcopy(vector)
    vector.get_value('exec_option')['mt_dop'] = vector.get_value('mt_dop')
    self.run_test_case('QueryTest/nested-types-runtime', vector)

  def test_subplan(self, vector):
    """Test subplans with various exec nodes inside it."""
    vector = deepcopy(vector)
    vector.get_value('exec_option')['mt_dop'] = vector.get_value('mt_dop')
    db_suffix = vector.get_value('table_format').db_suffix()
    self.run_test_case('QueryTest/nested-types-subplan', vector,
                       use_db='tpch_nested' + db_suffix)

  def test_subplan_single_node(self, vector):
    """Test subplans with various exec nodes inside it and num_nodes=1."""
    vector = deepcopy(vector)
    vector.get_value('exec_option')['mt_dop'] = vector.get_value('mt_dop')
    new_vector = deepcopy(vector)
    new_vector.get_value('exec_option')['num_nodes'] = 1
    self.run_test_case('QueryTest/nested-types-subplan-single-node', new_vector)

  def test_with_clause(self, vector):
    """Queries using nested types and with WITH clause."""
    db_suffix = vector.get_value('table_format').db_suffix()
    self.run_test_case('QueryTest/nested-types-with-clause', vector,
                       use_db='tpch_nested' + db_suffix)


class TestNestedStructsInSelectList(ImpalaTestSuite):
  """Functional tests for nested structs provided in the select list."""
  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestNestedStructsInSelectList, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_constraint(lambda v:
        v.get_value('table_format').file_format in ['parquet', 'orc'])
    cls.ImpalaTestMatrix.add_dimension(
        ImpalaTestDimension('mt_dop', 0, 2))
    cls.ImpalaTestMatrix.add_dimension(
        create_exec_option_dimension_from_dict({
            # Putting 'True' first because this way in non-exhaustive runs there are more
            # test cases with codegen enabled.
            'disable_codegen': ['True', 'False'],
            # The below two options are set to prevent the planner from disabling codegen
            # because of the small data size even when 'disable_codegen' is False.
            'disable_codegen_rows_threshold': [0],
            'exec_single_node_rows_threshold': [0]}))
    cls.ImpalaTestMatrix.add_dimension(create_client_protocol_dimension())
    cls.ImpalaTestMatrix.add_dimension(ImpalaTestDimension('orc_schema_resolution', 0, 1))
    cls.ImpalaTestMatrix.add_constraint(orc_schema_resolution_constraint)

  def test_struct_in_select_list(self, vector):
    """Queries where a struct column is in the select list"""
    new_vector = deepcopy(vector)
    new_vector.get_value('exec_option')['convert_legacy_hive_parquet_utc_timestamps'] = 1
    new_vector.get_value('exec_option')['TIMEZONE'] = '"Europe/Budapest"'
    self.run_test_case('QueryTest/struct-in-select-list', new_vector)

  @SkipIfFS.hbase
  def test_struct_in_select_list_hbase(self, vector):
    """Verify error cases that are not supported on HBase tables"""
    self.run_test_case('QueryTest/struct-in-select-list-hbase', vector)

  def test_nested_struct_in_select_list(self, vector):
    """Queries where a nested struct column is in the select list"""
    new_vector = deepcopy(vector)
    new_vector.get_value('exec_option')['convert_legacy_hive_parquet_utc_timestamps'] = 1
    self.run_test_case('QueryTest/nested-struct-in-select-list', new_vector)

  def test_struct_positions(self, vector):
    """Queries where structs and (file/collection) positions are used together"""
    self.run_test_case('QueryTest/struct-positions', vector)

  def test_tpch_nested(self, vector):
    """Add some tests from tpch nested that have a very complex column and large number
       of records, and deeply nested small/long strings."""
    if vector.get_value('table_format').file_format == 'parquet':
      db = 'tpch_nested_parquet'
    else:
      db = 'tpch_nested_orc_def'
    self.run_test_case('QueryTest/complex-tpch-nested', vector, use_db=db)


class TestNestedCollectionsInSelectList(ImpalaTestSuite):
  """Functional tests for nested arrays provided in the select list."""
  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestNestedCollectionsInSelectList, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_constraint(lambda v:
        v.get_value('table_format').file_format in ['parquet', 'orc'])
    cls.ImpalaTestMatrix.add_dimension(
        ImpalaTestDimension('mt_dop', 0, 2))
    cls.ImpalaTestMatrix.add_dimension(
        create_exec_option_dimension_from_dict({
            'disable_codegen': ['False', 'True'],
            # The below two options are set to prevent the planner from disabling codegen
            # because of the small data size even when 'disable_codegen' is False.
            'disable_codegen_rows_threshold': [0],
            'exec_single_node_rows_threshold': [0]}))
    cls.ImpalaTestMatrix.add_dimension(create_client_protocol_dimension())
    cls.ImpalaTestMatrix.add_dimension(ImpalaTestDimension('orc_schema_resolution', 0, 1))
    cls.ImpalaTestMatrix.add_constraint(orc_schema_resolution_constraint)

  def test_array_in_select_list(self, vector, unique_database):
    """Queries where an array column is in the select list"""
    self.run_test_case('QueryTest/nested-array-in-select-list', vector)

  def test_map_in_select_list(self, vector, unique_database):
    """Queries where a map column is in the select list"""
    self.run_test_case('QueryTest/nested-map-in-select-list', vector)

  def test_map_null_keys(self, vector, unique_database):
    """Queries where a map has null keys. Is only possible in ORC, not Parquet."""
    if vector.get_value('table_format').file_format == 'parquet':
      pytest.skip()
    self.run_test_case('QueryTest/map_null_keys', vector)


class TestMixedCollectionsAndStructsInSelectList(ImpalaTestSuite):
  """Functional tests for the case where collections and structs are embedded into one
  another and they are provided in the select list."""
  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestMixedCollectionsAndStructsInSelectList, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_constraint(lambda v:
        v.get_value('table_format').file_format in ['parquet', 'orc'])
    cls.ImpalaTestMatrix.add_dimension(
        ImpalaTestDimension('mt_dop', 0, 2))
    cls.ImpalaTestMatrix.add_dimension(
        create_exec_option_dimension_from_dict({
            'disable_codegen': ['False', 'True'],
            # The below two options are set to prevent the planner from disabling codegen
            # because of the small data size even when 'disable_codegen' is False.
            'disable_codegen_rows_threshold': [0],
            'exec_single_node_rows_threshold': [0]}))
    cls.ImpalaTestMatrix.add_dimension(create_client_protocol_dimension())
    cls.ImpalaTestMatrix.add_dimension(ImpalaTestDimension('orc_schema_resolution', 0, 1))
    cls.ImpalaTestMatrix.add_constraint(orc_schema_resolution_constraint)

  def test_mixed_complex_types_in_select_list(self, vector, unique_database):
    """Queries where structs and collections are embedded into one another."""
    self.run_test_case('QueryTest/mixed-collections-and-structs', vector)


class TestComputeStatsWithNestedTypes(ImpalaTestSuite):
  """Functional tests for running compute stats on tables that have nested types in the
  columns."""
  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestComputeStatsWithNestedTypes, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_constraint(lambda v:
        v.get_value('table_format').file_format in ['parquet', 'orc'])
    cls.ImpalaTestMatrix.add_dimension(ImpalaTestDimension('orc_schema_resolution', 0, 1))
    cls.ImpalaTestMatrix.add_constraint(orc_schema_resolution_constraint)

  def test_compute_stats_with_structs(self, vector):
    """COMPUTE STATS and SHOW COLUMN STATS for tables with structs"""
    self.run_test_case('QueryTest/compute-stats-with-structs', vector)


class TestZippingUnnest(ImpalaTestSuite):
  """Functional tests for zipping unnest functionality."""
  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestZippingUnnest, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_constraint(lambda v:
        v.get_value('table_format').file_format in ['parquet', 'orc'])
    cls.ImpalaTestMatrix.add_dimension(ImpalaTestDimension('orc_schema_resolution', 0, 1))
    cls.ImpalaTestMatrix.add_constraint(orc_schema_resolution_constraint)

  def test_zipping_unnest_in_from_clause(self, vector):
    """Queries where zipping unnest is executed by providing UNNEST() in the from clause.
    """
    self.run_test_case('QueryTest/zipping-unnest-in-from-clause', vector)

  def test_zipping_unnest_in_select_list(self, vector):
    """Queries where zipping unnest is executed by providing UNNEST() in the select list.
    """
    self.run_test_case('QueryTest/zipping-unnest-in-select-list', vector)


class TestZippingUnnestFromView(ImpalaTestSuite):
  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestZippingUnnestFromView, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_constraint(lambda v:
        v.get_value('table_format').file_format == 'parquet')

  def test_zipping_unnest_from_view(self, vector, unique_database):
    """Zipping unnest queries where views are involved."""
    self.run_test_case('QueryTest/zipping-unnest-from-view', vector,
        use_db=unique_database)


class TestNestedTypesNoMtDop(ImpalaTestSuite):
  """Functional tests for nested types that do not need to be run with mt_dop > 0."""
  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestNestedTypesNoMtDop, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_constraint(lambda v:
        v.get_value('table_format').file_format in ['parquet', 'orc'])
    cls.ImpalaTestMatrix.add_dimension(ImpalaTestDimension('orc_schema_resolution', 0, 1))
    cls.ImpalaTestMatrix.add_constraint(orc_schema_resolution_constraint)

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

  def test_tpch_errors(self, vector):
    """Queries that test error handling on the TPC-H nested data set."""
    db_suffix = vector.get_value('table_format').db_suffix()
    self.run_test_case('QueryTest/nested-types-tpch-errors',
                       vector, use_db='tpch_nested' + db_suffix)

  def test_parquet_stats(self, vector):
    """Queries that test evaluation of Parquet row group statistics."""
    if vector.get_value('table_format').file_format == 'orc':
      pytest.skip('This test is specific to Parquet')
    self.run_test_case('QueryTest/nested-types-parquet-stats', vector)

  @SkipIfFS.hive
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
    if file_format == 'parquet':
      base_table = "functional%s.complextypestbl" % db_suffix
    else:
      assert file_format == 'orc'
      base_table = "functional%s.complextypestbl_non_transactional" % db_suffix
    for partition in [1, 2]:
      self.client.execute("ALTER TABLE {0} ADD PARTITION(part={1}) LOCATION '{2}'".format(
          db_table, partition,
          self._get_table_location(base_table, vector)))
    self.run_test_case('QueryTest/nested-types-basic-partitioned', vector,
        unique_database)

  # Skip this test on non-HDFS filesystems, because the test contains Hive
  # queries that hang in some cases due to IMPALA-9365.
  @SkipIfFS.hive
  @SkipIfHive2.acid
  def test_partitioned_table_acid(self, vector, unique_database):
    """IMPALA-6370: Test that a partitioned table with nested types can be scanned."""
    table = "complextypes_partitioned"
    db_table = "{0}.{1}".format(unique_database, table)
    table_format_info = vector.get_value('table_format')  # type: TableFormatInfo
    file_format = table_format_info.file_format
    if file_format != "orc":
      pytest.skip('Full ACID tables are only supported in ORC format.')

    self.client.execute("""
        CREATE TABLE {0} (
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
        STORED AS ORC
        TBLPROPERTIES('transactional'='true')""".format(db_table))
    # Add multiple partitions with the complextypestbl data.
    base_table = "functional_orc_def.complextypestbl"
    for partition in [1, 2]:
      self.run_stmt_in_hive("INSERT INTO TABLE {0} PARTITION(part={1}) "
          "SELECT * FROM {2}".format(db_table, partition, base_table))
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
      except Exception as e:
        assert expected_err in str(e)
      try:
        self.execute_query("select cnt from %s t, (select count(*) cnt from t.col1) v"\
          % full_name, qopts)
      except Exception as e:
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
      pytest.skip("Test only run with three_level")

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
    self.filesystem_client.copy_from_local(local_path, location)

class TestMaxNestingDepth(ImpalaTestSuite):
  # Should be kept in sync with the FE's Type.MAX_NESTING_DEPTH
  MAX_NESTING_DEPTH = 100
  TABLES = ['struct', 'int_array', 'struct_array', 'int_map', 'struct_map']

  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestMaxNestingDepth, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_constraint(lambda v:
        v.get_value('table_format').file_format in ['parquet', 'orc'])
    cls.ImpalaTestMatrix.add_dimension(ImpalaTestDimension('orc_schema_resolution', 0, 1))
    cls.ImpalaTestMatrix.add_constraint(orc_schema_resolution_constraint)

  def test_max_nesting_depth(self, vector, unique_database):
    """Tests that Impala can scan Parquet and ORC files having complex types of
    the maximum nesting depth."""
    file_format = vector.get_value('table_format').file_format
    if file_format == 'parquet':
      self.__create_parquet_tables(unique_database)
    elif file_format == 'orc':
      self.__create_orc_tables(unique_database)
    self.run_test_case('QueryTest/max-nesting-depth', vector, unique_database)

  def __create_parquet_tables(self, unique_database):
    """Create Parquet tables from files."""
    self.filesystem_client.copy_from_local(
      "%s/testdata/max_nesting_depth/parquet" % os.environ['IMPALA_HOME'],
      "%s/%s.db/" % (WAREHOUSE, unique_database))
    for tbl in self.TABLES:
      tbl_name = "%s.%s_tbl" % (unique_database, tbl)
      tbl_location = "%s/%s.db/parquet/%s/" % (WAREHOUSE, unique_database, tbl)
      create_table = "CREATE EXTERNAL TABLE %s LIKE PARQUET '%s' STORED AS PARQUET" \
          " location '%s'" % (tbl_name, tbl_location + 'file.parq', tbl_location)
      self.client.execute(create_table)

  def __create_orc_tables(self, unique_database):
    """Create ORC tables from files."""
    self.filesystem_client.copy_from_local(
      "%s/testdata/max_nesting_depth/orc" % os.environ['IMPALA_HOME'],
      "%s/%s.db/" % (WAREHOUSE, unique_database))
    for tbl in self.TABLES:
      tbl_name = "%s.%s_tbl" % (unique_database, tbl)
      tbl_location = "%s/%s.db/orc/%s/" % (WAREHOUSE, unique_database, tbl)
      create_table = "CREATE EXTERNAL TABLE %s LIKE ORC '%s' STORED AS ORC" \
          " location '%s'" % (tbl_name, tbl_location + 'file.orc', tbl_location)
      self.client.execute(create_table)

  @SkipIfFS.hive
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
    except ImpalaBeeswaxException as e:
      assert "Type exceeds the maximum nesting depth" in str(e)


class TestNestedTypesStarExpansion(ImpalaTestSuite):
  """Functional tests for nested types when star expansion query
  option (EXPAND_COMPLEX_TYPES) is enabled/disabled, run for all file formats that
  support nested types."""

  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestNestedTypesStarExpansion, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(create_client_protocol_dimension())
    cls.ImpalaTestMatrix.add_constraint(lambda v:
        v.get_value('table_format').file_format in ['parquet', 'orc'])
    cls.ImpalaTestMatrix.add_constraint(lambda v:
        v.get_value('protocol') == 'hs2')
    cls.ImpalaTestMatrix.add_dimension(
        create_exec_option_dimension_from_dict({
            'disable_codegen': ['False', 'True']}))
    cls.ImpalaTestMatrix.add_mandatory_exec_option(
            'convert_legacy_hive_parquet_utc_timestamps', 'true')
    cls.ImpalaTestMatrix.add_mandatory_exec_option('TIMEZONE', '"Europe/Budapest"')

  def test_star_expansion(self, vector):
    # Queries with star (*) expression on tables with array, map
    # and struct complex type, through views, with query option
    # EXPAND_COMPLEX_TYPES enabled and disabled.
    self.run_test_case('QueryTest/nested-types-star-expansion', vector)
