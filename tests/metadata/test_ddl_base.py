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
#
from __future__ import absolute_import, division, print_function
from tests.common.test_dimensions import (
    ALL_NODES_ONLY,
    create_exec_option_dimension,
    create_uncompressed_text_dimension)
from tests.common.impala_test_suite import ImpalaTestSuite
from tests.util.filesystem_utils import WAREHOUSE

# Base class that most DDL tests inherit from. The tests have a few common functions,
# as well as test dimensions and setup/teardown.
class TestDdlBase(ImpalaTestSuite):
  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestDdlBase, cls).add_test_dimensions()
    sync_ddl_opts = [0, 1]
    if cls.exploration_strategy() != 'exhaustive':
      # Only run with sync_ddl on exhaustive since it increases test runtime.
      sync_ddl_opts = [0]

    cls.ImpalaTestMatrix.add_dimension(create_exec_option_dimension(
        cluster_sizes=ALL_NODES_ONLY,
        disable_codegen_options=[False],
        batch_sizes=[0],
        sync_ddl=sync_ddl_opts))

    # There is no reason to run these tests using all dimensions.
    cls.ImpalaTestMatrix.add_dimension(
        create_uncompressed_text_dimension(cls.get_workload()))

  @classmethod
  def _use_multiple_impalad(cls, vector):
    return vector.get_value('exec_option')['sync_ddl'] == 1

  def _create_db(self, db_name, sync=False, comment=None):
    """Creates a database using synchronized DDL to ensure all nodes have the test
    database available for use before executing the .test file(s).
    """
    impala_client = self.create_impala_client()
    sync and impala_client.set_configuration({'sync_ddl': 1})
    if comment is None:
      ddl = "create database {0} location '{1}/{0}.db'".format(db_name, WAREHOUSE)
    else:
      ddl = "create database {0} comment '{1}' location '{2}/{0}.db'".format(
        db_name, comment, WAREHOUSE)
    impala_client.execute(ddl)
    impala_client.close()

  def _get_tbl_properties(self, table_name):
    """Extracts the table properties mapping from the output of DESCRIBE FORMATTED"""
    return self._get_properties('Table Parameters:', table_name)

  def _get_serde_properties(self, table_name):
    """Extracts the serde properties mapping from the output of DESCRIBE FORMATTED"""
    return self._get_properties('Storage Desc Params:', table_name)

  def _get_db_owner_properties(self, db_name):
    """Extracts the DB properties mapping from the output of DESCRIBE FORMATTED"""
    return self._get_properties("Owner:", db_name, True)

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

  def _get_property(self, property_name, name, is_db=False):
    """Extracts a db/table property value from the output of DESCRIBE FORMATTED."""
    result = self.client.execute("describe {0} formatted {1}".format(
      "database" if is_db else "", name))
    for row in result.data:
      if property_name in row:
        row = row.split('\t')
        if row[1] == 'NULL':
          break
        return row[1].rstrip()
    return None

  def _get_db_comment(self, db_name):
    """Extracts the DB comment from the output of DESCRIBE DATABASE"""
    result = self.client.execute("describe database {0}".format(db_name))
    return result.data[0].split('\t')[2]

  def _get_table_or_view_comment(self, table_name):
    props = self._get_tbl_properties(table_name)
    return props["comment"] if "comment" in props else None

  def _get_column_comment(self, table_or_view_name, col_name):
    result = self.client.execute("describe {0}".format(table_or_view_name))
    comments = dict()
    for row in result.data:
      cols = row.split('\t')
      if len(cols) <= 10:
        comments[cols[0].rstrip()] = cols[2].rstrip()
    return comments.get(col_name)


  def _get_table_or_view_owner(self, table_name):
    """Returns a tuple(owner, owner_type) for a given table name"""
    owner_name = self._get_property("Owner:", table_name)
    owner_type = self._get_property("OwnerType:", table_name)
    return (owner_name, owner_type)
