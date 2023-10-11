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

from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.skip import SkipIfCatalogV2, SkipIf
from tests.common.test_dimensions import create_uncompressed_text_dimension


class TestExtDataSources(ImpalaTestSuite):
  """Impala query tests for external data sources."""

  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestExtDataSources, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(
        create_uncompressed_text_dimension(cls.get_workload()))

  def _get_tbl_properties(self, table_name):
    """Extracts the table properties mapping from the output of DESCRIBE FORMATTED"""
    return self._get_properties('Table Parameters:', table_name)

  def _get_properties(self, section_name, name, is_db=False):
    """Extracts the db/table properties mapping from the output of DESCRIBE FORMATTED"""
    result = self.client.execute("describe {0} formatted {1}".format(
      "database" if is_db else "", name))
    match = False
    properties = dict()
    for row in result.data:
      fields = row.split("\t")
      if fields[0] != '':
        # Start of new section.
        if match:
          # Finished processing matching section.
          break
        match = section_name in fields[0]
      elif match:
        if fields[1] == 'NULL':
          break
        properties[fields[1].rstrip()] = fields[2].rstrip()
    return properties

  @SkipIfCatalogV2.data_sources_unsupported()
  @SkipIf.not_hdfs
  def test_verify_jdbc_table_properties(self, vector):
    jdbc_tbl_name = "functional.alltypes_jdbc_datasource"
    properties = self._get_tbl_properties(jdbc_tbl_name)
    # Verify data source related table properties
    assert properties['__IMPALA_DATA_SOURCE_NAME'] == 'jdbcdatasource'
    assert properties['__IMPALA_DATA_SOURCE_LOCATION'] == \
        'hdfs://localhost:20500/test-warehouse/data-sources/jdbc-data-source.jar'
    assert properties['__IMPALA_DATA_SOURCE_CLASS'] == \
        'org.apache.impala.extdatasource.jdbc.JdbcDataSource'
    assert properties['__IMPALA_DATA_SOURCE_API_VERSION'] == 'V1'
    assert 'database.type\\":\\"POSTGRES' \
        in properties['__IMPALA_DATA_SOURCE_INIT_STRING']
    assert 'table\\":\\"alltypes' \
        in properties['__IMPALA_DATA_SOURCE_INIT_STRING']

  @SkipIfCatalogV2.data_sources_unsupported()
  def test_data_source_tables(self, vector):
    self.run_test_case('QueryTest/data-source-tables', vector)

  @SkipIfCatalogV2.data_sources_unsupported()
  @SkipIf.not_hdfs
  def test_jdbc_data_source(self, vector):
    self.run_test_case('QueryTest/jdbc-data-source', vector)
