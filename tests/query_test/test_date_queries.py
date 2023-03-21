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

# Targeted tests for date type.

from __future__ import absolute_import, division, print_function
import pytest
from tests.common.file_utils import create_table_and_copy_files
from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.skip import SkipIfFS
from tests.common.test_dimensions import (create_exec_option_dimension_from_dict,
    create_client_protocol_dimension, hs2_parquet_constraint)
from tests.shell.util import create_impala_shell_executable_dimension


class TestDateQueries(ImpalaTestSuite):
  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestDateQueries, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(
      create_exec_option_dimension_from_dict({
        'batch_size': [0, 1],
        'disable_codegen': ['false', 'true'],
        'disable_codegen_rows_threshold': [0]}))
    # DATE type is only supported for text, parquet, avro, orc and json fileformat on HDFS
    # and HBASE.
    cls.ImpalaTestMatrix.add_constraint(lambda v:
        v.get_value('table_format').file_format in ('text', 'hbase', 'parquet', 'json')
        or (v.get_value('table_format').file_format == 'avro'
            and v.get_value('table_format').compression_codec == 'snap'))

    # Run these queries through both beeswax and HS2 to get coverage of date returned
    # via both protocols.
    cls.ImpalaTestMatrix.add_dimension(create_client_protocol_dimension())
    cls.ImpalaTestMatrix.add_constraint(hs2_parquet_constraint)
    cls.ImpalaTestMatrix.add_dimension(create_impala_shell_executable_dimension())

  def test_queries(self, vector):
    if vector.get_value('table_format').file_format == 'avro':
      # Avro date test queries are in a separate test file.
      #  - Hive2 uses Julian Calendar for writing dates before 1582-10-15, whereas Impala
      #    uses proleptic Gregorian Calendar. This affects the results Impala gets when
      #    querying avro tables written by Hive2.
      #  - Hive3 on the other hand uses proleptic Gregorian Calendar to write dates.
      self.run_test_case('QueryTest/avro_date', vector)
    else:
      self.run_test_case('QueryTest/date', vector)

  def test_partitioning(self, vector, unique_database):
    """ Test partitioning by DATE. """
    # This test specifies databases explicitly. No need to execute it for anything other
    # than text fileformat.
    if vector.get_value('table_format').file_format != 'text':
      pytest.skip()
    self.run_test_case('QueryTest/date-partitioning', vector, use_db=unique_database)

  @SkipIfFS.qualified_path
  def test_fileformat_support(self, vector, unique_database):
    """ Test that scanning and writing DATE is supported for text and parquet tables.
        Test that scanning DATE is supported for avro tables as well.
    """
    # This test specifies databases and locations explicitly. No need to execute it for
    # anything other than text fileformat on HDFS.
    if vector.get_value('table_format').file_format != 'text':
      pytest.skip()

    # Parquet table with date column.
    TABLE_NAME = "parquet_date_tbl"
    CREATE_SQL = "CREATE TABLE {0}.{1} (date_col DATE) STORED AS PARQUET".format(
        unique_database, TABLE_NAME)
    create_table_and_copy_files(self.client, CREATE_SQL, unique_database, TABLE_NAME,
        ["/testdata/data/date_tbl.parquet"])
    # Avro table with date column.
    TABLE_NAME = "avro_date_tbl"
    CREATE_SQL = "CREATE TABLE {0}.{1} (date_col DATE) STORED AS AVRO".format(
        unique_database, TABLE_NAME)
    create_table_and_copy_files(self.client, CREATE_SQL, unique_database, TABLE_NAME,
        ["/testdata/data/date_tbl.avro"])
    # Orc table with date column.
    TABLE_NAME = "orc_date_tbl"
    CREATE_SQL = "CREATE TABLE {0}.{1} (date_col DATE) STORED AS ORC".format(
        unique_database, TABLE_NAME)
    create_table_and_copy_files(self.client, CREATE_SQL, unique_database, TABLE_NAME,
        ["/testdata/data/date_tbl.orc"])

    # Partitioned table with parquet and avro partitions.
    TABLE_NAME = "date_tbl"
    CREATE_SQL = """CREATE TABLE {0}.{1} (date_col DATE)
        PARTITIONED BY (date_part DATE)""".format(unique_database, TABLE_NAME)
    self.client.execute(CREATE_SQL)
    # Add partitions.
    ADD_PART_SQL = """ALTER TABLE {0}.{1} ADD PARTITION (date_part='1899-12-31')
        LOCATION '/test-warehouse/{0}.db/parquet_date_tbl'
        PARTITION (date_part='1999-12-31')
        LOCATION '/test-warehouse/{0}.db/avro_date_tbl'
        """.format(unique_database, TABLE_NAME)
    self.client.execute(ADD_PART_SQL)
    # Parquet fileformat.
    SET_PART_FF_SQL = """ALTER TABLE {0}.{1} PARTITION (date_part='1899-12-31')
        SET FILEFORMAT PARQUET""".format(unique_database, TABLE_NAME)
    self.client.execute(SET_PART_FF_SQL)
    # Avro fileformat.
    SET_PART_FF_SQL = """ALTER TABLE {0}.{1} PARTITION (date_part='1999-12-31')
        SET FILEFORMAT AVRO""".format(unique_database, TABLE_NAME)
    self.client.execute(SET_PART_FF_SQL)
    # After adding the avro partition, metadata has to be invalidated, otherwise querying
    # the table will fail with stale metadata error.
    self.client.execute("INVALIDATE METADATA {0}.{1}".format(unique_database, TABLE_NAME))

    # Test scanning/writing tables with different fileformats.
    self.run_test_case('QueryTest/date-fileformat-support', vector,
        use_db=unique_database)
