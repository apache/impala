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

from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.skip import SkipIf

class TestCreatingIcebergTable(ImpalaTestSuite):
  """Test creating iceberg managed and external table"""

  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestCreatingIcebergTable, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_constraint(
      lambda v: v.get_value('table_format').file_format == 'parquet')

  def test_create_iceberg_tables(self, vector, unique_database):
    self.run_test_case('QueryTest/iceberg-create', vector, use_db=unique_database)

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
