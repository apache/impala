# Copyright (c) 2016 Cloudera, Inc. All rights reserved.
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

    cls.TestMatrix.add_dimension(create_exec_option_dimension(
        cluster_sizes=ALL_NODES_ONLY,
        disable_codegen_options=[False],
        batch_sizes=[0],
        sync_ddl=sync_ddl_opts))

    # There is no reason to run these tests using all dimensions.
    cls.TestMatrix.add_dimension(create_uncompressed_text_dimension(cls.get_workload()))

  def create_drop_ddl(self, vector, db_name, create_stmts, drop_stmts, select_stmt,
      num_iterations=3):
    """Helper method to run CREATE/DROP DDL commands repeatedly and exercise the lib
    cache. create_stmts is the list of CREATE statements to be executed in order
    drop_stmts is the list of DROP statements to be executed in order. Each statement
    should have a '%s' placeholder to insert "IF EXISTS" or "". The select_stmt is just a
    single statement to test after executing the CREATE statements.
    TODO: it's hard to tell that the cache is working (i.e. if it did nothing to drop
    the cache, these tests would still pass). Testing that is a bit harder and requires
    us to update the udf binary in the middle.
    """
    # The db may already exist, clean it up.
    self.cleanup_db(db_name)
    self._create_db(db_name, sync=True)
    self.client.set_configuration(vector.get_value('exec_option'))
    self.client.execute("use %s" % (db_name,))
    for drop_stmt in drop_stmts: self.client.execute(drop_stmt % ("if exists"))
    for i in xrange(0, num_iterations):
      for create_stmt in create_stmts: self.client.execute(create_stmt)
      self.client.execute(select_stmt)
      for drop_stmt in drop_stmts: self.client.execute(drop_stmt % (""))

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

  def _get_properties(self, section_name, table_name):
    """Extracts the table properties mapping from the output of DESCRIBE FORMATTED"""
    result = self.client.execute("describe formatted " + table_name)
    match = False
    properties = dict();
    for row in result.data:
      if section_name in row:
        match = True
      elif match:
        row = row.split('\t')
        if (row[1] == 'NULL'):
          break
        properties[row[1].rstrip()] = row[2].rstrip()
    return properties
