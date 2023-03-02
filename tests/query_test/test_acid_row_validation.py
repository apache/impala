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

# Functional tests for ACID integration with Hive.

from __future__ import absolute_import, division, print_function
import os
import pytest

from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.skip import SkipIfLocal
from tests.util.acid_txn import AcidTxn
from tests.util.filesystem_utils import IS_HDFS

# Tests that Impala validates rows against a validWriteIdList correctly.
class TestAcidRowValidation(ImpalaTestSuite):
  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def setup_class(cls):
    super(TestAcidRowValidation, cls).setup_class()
    cls.acid = AcidTxn(cls.hive_client)

  @classmethod
  def add_test_dimensions(cls):
    super(TestAcidRowValidation, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_constraint(lambda v:
        v.get_value('table_format').file_format in ['orc'])

  def _create_test_table(self, vector, unique_database, tbl_name):
    fq_tbl_name = "{0}.{1}".format(unique_database, tbl_name)
    create_stmt = """CREATE TABLE {0} (a string, b string) STORED AS ORC
        TBLPROPERTIES('transactional'='true')""".format(fq_tbl_name)
    self.client.execute(create_stmt)
    table_uri = self._get_table_location(fq_tbl_name, vector)
    table_path = table_uri[table_uri.index("test-warehouse"):]
    delta_dir = table_path + "/delta_1_2"
    self.hdfs_client.make_dir(delta_dir)
    streaming_orc_file = os.environ['IMPALA_HOME'] + "/testdata/data/streaming.orc"
    self.hdfs_client.copy_from_local(streaming_orc_file, "/" + delta_dir)

  def _commit_one(self, unique_database, table_name):
    txn_id = self.acid.open_txns()
    self.acid.allocate_table_write_ids(txn_id, unique_database, table_name)
    self.acid.commit_txn(txn_id)

  @SkipIfLocal.hdfs_client
  def test_row_validation(self, vector, unique_database):
    """Tests reading from a file written by Hive Streaming Ingestion. In the first no rows
    are valid. Then we commit the first transaction and read the table. Then we commit the
    last transaction and read the table."""
    # This test only makes sense on a filesystem that supports the file append operation
    # (e.g. S3 doesn't) because it simulates Hive Streaming V2. So let's run it only on
    # HDFS.
    if not IS_HDFS: pytest.skip()
    tbl_name = "streaming"
    self._create_test_table(vector, unique_database, tbl_name)
    self.run_test_case('QueryTest/acid-row-validation-0', vector, use_db=unique_database)
    self._commit_one(unique_database, tbl_name)
    self.run_test_case('QueryTest/acid-row-validation-1', vector, use_db=unique_database)
    self._commit_one(unique_database, tbl_name)
    self.run_test_case('QueryTest/acid-row-validation-2', vector, use_db=unique_database)
