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
import random

from subprocess import check_call
from parquet.ttypes import ConvertedType

from tests.common.impala_test_suite import ImpalaTestSuite, LOG
from tests.common.skip import SkipIf

from tests.util.filesystem_utils import get_fs_path
from tests.util.get_parquet_metadata import get_parquet_metadata

class TestIcebergTable(ImpalaTestSuite):
  """Tests related to Iceberg tables."""

  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestIcebergTable, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_constraint(
      lambda v: v.get_value('table_format').file_format == 'parquet')

  def test_iceberg_negative(self, vector, unique_database):
    self.run_test_case('QueryTest/iceberg-negative', vector, use_db=unique_database)

  def test_create_iceberg_tables(self, vector, unique_database):
    self.run_test_case('QueryTest/iceberg-create', vector, use_db=unique_database)

  def test_alter_iceberg_tables(self, vector, unique_database):
    self.run_test_case('QueryTest/iceberg-alter', vector, use_db=unique_database)

  @SkipIf.not_hdfs
  def test_drop_incomplete_table(self, vector, unique_database):
    """Test DROP TABLE when the underlying directory is deleted. In that case table
    loading fails, but we should be still able to drop the table from Impala."""
    tbl_name = unique_database + ".synchronized_iceberg_tbl"
    cat_location = "/test-warehouse/" + unique_database
    self.client.execute("""create table {0} (i int) stored as iceberg
        tblproperties('iceberg.catalog'='hadoop.catalog',
                      'iceberg.catalog_location'='{1}')""".format(tbl_name, cat_location))
    self.hdfs_client.delete_file_dir(cat_location, True)
    self.execute_query_expect_success(self.client, """drop table {0}""".format(tbl_name))

  @SkipIf.not_hdfs
  def test_insert(self, vector, unique_database):
    self.run_test_case('QueryTest/iceberg-insert', vector, use_db=unique_database)

  def test_partitioned_insert(self, vector, unique_database):
    self.run_test_case('QueryTest/iceberg-partitioned-insert', vector,
        use_db=unique_database)

  def test_partition_transform_insert(self, vector, unique_database):
    self.run_test_case('QueryTest/iceberg-partition-transform-insert', vector,
        use_db=unique_database)

  def test_describe_history(self, vector, unique_database):
    self.run_test_case('QueryTest/iceberg-table-history', vector, use_db=unique_database)

    # Create a table with multiple snapshots and verify the table history.
    tbl_name = unique_database + ".iceberg_multi_snapshots"
    self.client.execute("""create table {0} (i int) stored as iceberg
        tblproperties('iceberg.catalog'='hadoop.tables')""".format(tbl_name))
    result = self.client.execute("INSERT INTO {0} VALUES (1)".format(tbl_name))
    result = self.client.execute("INSERT INTO {0} VALUES (2)".format(tbl_name))
    result = self.client.execute("DESCRIBE HISTORY {0}".format(tbl_name))
    assert(len(result.data) == 2)
    first_snapshot = result.data[0].split("\t")
    second_snapshot = result.data[1].split("\t")
    # Check that first snapshot is older than the second snapshot.
    assert(first_snapshot[0] < second_snapshot[0])
    # Check that second snapshot's parent ID is the snapshot ID of the first snapshot.
    assert(first_snapshot[1] == second_snapshot[2])
    # The first snapshot has no parent snapshot ID.
    assert(first_snapshot[2] == "NULL")
    # Check "is_current_ancestor" column.
    assert(first_snapshot[3] == "TRUE" and second_snapshot[3] == "TRUE")

  @SkipIf.not_hdfs
  def test_strings_utf8(self, vector, unique_database):
    # Create table
    table_name = "ice_str_utf8"
    qualified_table_name = "%s.%s" % (unique_database, table_name)
    query = 'create table %s (a string) stored as iceberg' % qualified_table_name
    self.client.execute(query)

    # Inserted string data should have UTF8 annotation regardless of query options.
    query = 'insert into %s values ("impala")' % qualified_table_name
    self.execute_query(query, {'parquet_annotate_strings_utf8': False})

    # Copy the created file to the local filesystem and parse metadata
    local_file = '/tmp/iceberg_utf8_test_%s.parq' % random.randint(0, 10000)
    LOG.info("test_strings_utf8 local file name: " + local_file)
    hdfs_file = get_fs_path('/test-warehouse/%s.db/%s/data/*.parq'
        % (unique_database, table_name))
    check_call(['hadoop', 'fs', '-copyToLocal', hdfs_file, local_file])
    metadata = get_parquet_metadata(local_file)

    # Extract SchemaElements corresponding to the table column
    a_schema_element = metadata.schema[1]
    assert a_schema_element.name == 'a'

    # Check that the schema uses the UTF8 annotation
    assert a_schema_element.converted_type == ConvertedType.UTF8

    os.remove(local_file)
