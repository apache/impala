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

# Impala tests for DDL statements
import time

from tests.common.impala_test_suite import ImpalaTestSuite
from tests.util.filesystem_utils import WAREHOUSE, IS_S3

# Checks that ALTER and INSERT statements update the last DDL time of the modified table.
class TestLastDdlTimeUpdate(ImpalaTestSuite):

  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestLastDdlTimeUpdate, cls).add_test_dimensions()
    # There is no reason to run these tests using all dimensions.
    cls.ImpalaTestMatrix.add_constraint(lambda v:\
        v.get_value('table_format').file_format == 'text' and\
        v.get_value('table_format').compression_codec == 'none')

    if cls.exploration_strategy() == 'core' and not IS_S3:
      # Don't run on core.  This test is very slow and we are unlikely
      # to regress here.
      cls.ImpalaTestMatrix.add_constraint(lambda v: False)

  def test_alter(self, vector, unique_database):
    TBL_NAME = "alter_test_tbl"
    FQ_TBL_NAME = unique_database + "." + TBL_NAME
    self.execute_query("create external table %s (i int) "
                       "partitioned by (j int, s string)" % FQ_TBL_NAME)

    # add/drop partitions
    self.run_test("alter table %s add partition (j=1, s='2012')" % FQ_TBL_NAME,
                  unique_database, TBL_NAME, False)
    self.run_test("alter table %s add if not exists "
                  "partition (j=1, s='2012')" % FQ_TBL_NAME,
                  unique_database, TBL_NAME, False)
    self.run_test("alter table %s drop partition (j=1, s='2012')" % FQ_TBL_NAME,
                  unique_database, TBL_NAME, False)
    self.run_test("alter table %s drop if exists "
                  "partition (j=2, s='2012')" % FQ_TBL_NAME,
                  unique_database, TBL_NAME, False)
    # rename columns
    self.run_test("alter table %s change column i k int" % FQ_TBL_NAME,
                  unique_database, TBL_NAME, True)
    self.run_test("alter table %s change column k i int" % FQ_TBL_NAME,
                  unique_database, TBL_NAME, True)
    # change location of table
    self.run_test("alter table %s set location "
                  "'%s'" % (FQ_TBL_NAME, WAREHOUSE),
                  unique_database, TBL_NAME, True)
    # change format of table
    self.run_test("alter table %s set fileformat textfile" % FQ_TBL_NAME,
                  unique_database, TBL_NAME, True)

  def test_insert(self, vector, unique_database):
    TBL_NAME = "insert_test_tbl"
    FQ_TBL_NAME = unique_database + "." + TBL_NAME
    self.execute_query("create external table %s (i int) "
                       "partitioned by (j int, s string)" % FQ_TBL_NAME)
    # static partition insert
    self.run_test("insert into %s partition(j=1, s='2012') "
                  "select 10" % FQ_TBL_NAME, unique_database, TBL_NAME, False)
    # dynamic partition insert
    self.run_test("insert into %s partition(j, s) "
                  "select 10, 2, '2013'" % FQ_TBL_NAME, unique_database, TBL_NAME, False)
    # dynamic partition insert changing no partitions (empty input)
    self.run_test("insert into %s partition(j, s) "
                  "select * from (select 10 as i, 2 as j, '2013' as s) as t "
                  "where t.i < 10" % FQ_TBL_NAME, unique_database, TBL_NAME, False)
    # dynamic partition insert modifying an existing partition
    self.run_test("insert into %s partition(j, s) "
                  "select 20, 1, '2012'" % FQ_TBL_NAME, unique_database, TBL_NAME, False)

  def run_test(self, query, db_name, table_name, expect_changed):
    """
    Runs the given query (expected to be an ALTER or INSERT statement)
    and compares the last ddl time before and after executing the query.
    If expect_change is true then we expect the last ddl time to increase,
    otherwise we expect no change.
    """

    HIVE_LAST_DDL_TIME_PARAM_KEY = "transient_lastDdlTime"

    # Get last DDL time before executing query.
    table = self.hive_client.get_table(db_name, table_name)
    assert table is not None
    beforeDdlTime = table.parameters[HIVE_LAST_DDL_TIME_PARAM_KEY]

    # Sleep for 2s to make sure the new ddlTime is strictly greater than the old one.
    # Hive uses a seconds granularity on the last ddl time.
    time.sleep (2)

    self.execute_query(query)

    # Get last ddl time after executing query .
    table = self.hive_client.get_table(db_name, table_name)
    afterDdlTime = table.parameters[HIVE_LAST_DDL_TIME_PARAM_KEY]

    if expect_changed:
      # check that the new ddlTime is strictly greater than the old one.
      assert long(afterDdlTime) > long(beforeDdlTime)
    else:
      assert long(afterDdlTime) == long(beforeDdlTime)
