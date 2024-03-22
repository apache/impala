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

import re

from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.test_dimensions import (create_uncompressed_text_dimension,
    extend_exec_option_dimension)


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
    extend_exec_option_dimension(cls, "exec_single_node_rows_threshold", "100")

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

  def test_verify_jdbc_table_properties(self, vector):
    jdbc_tbl_name = "functional.alltypes_jdbc_datasource"
    properties = self._get_tbl_properties(jdbc_tbl_name)
    # Verify table properties specific for external JDBC table
    assert properties['__IMPALA_DATA_SOURCE_NAME'] == 'impalajdbcdatasource'
    assert properties['database.type'] == 'POSTGRES'
    assert properties['jdbc.driver'] == 'org.postgresql.Driver'
    assert properties['dbcp.username'] == 'hiveuser'
    assert properties['table'] == 'alltypes'
    # Verify dbcp.password is masked in the output of DESCRIBE FORMATTED command
    assert properties['dbcp.password'] == '******'

    # Verify dbcp.password is masked in the output of SHOW CREATE TABLE command
    result = self.client.execute("SHOW CREATE TABLE {0}".format(jdbc_tbl_name))
    match = False
    for row in result.data:
      if "'dbcp.password'='******'" in row:
        match = True
        break
    assert match, result.data

  def test_data_source_tables(self, vector, unique_database):
    self.run_test_case('QueryTest/data-source-tables', vector, use_db=unique_database)

  def test_jdbc_data_source(self, vector, unique_database):
    self.run_test_case('QueryTest/jdbc-data-source', vector, use_db=unique_database)

  def test_jdbc_data_source_with_keystore(self, vector, unique_database):
    # Impala query tests for external data sources with keystore.
    self.run_test_case('QueryTest/jdbc-data-source-with-keystore', vector,
        use_db=unique_database)
