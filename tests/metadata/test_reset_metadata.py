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

from test_ddl_base import TestDdlBase
from tests.common.skip import (SkipIfS3, SkipIfABFS, SkipIfADLS, SkipIfGCS,
                               SkipIfIsilon, SkipIfLocal)


class TestResetMetadata(TestDdlBase):
  def test_reset_metadata_case_sensitivity(self, unique_database):
    # IMPALA-6719: Issue with database name case sensitivity in reset metadata.
    table = 'newtable'
    self.client.execute('create table %s.%s (i int)' % (unique_database, table))

    self.client.execute('refresh %s.%s' % (unique_database, table))
    self.client.execute('refresh %s.%s' % (unique_database.upper(), table.upper()))

    self.client.execute('invalidate metadata %s.%s' % (unique_database, table))
    self.client.execute('invalidate metadata %s.%s' % (unique_database.upper(),
                                                       table.upper()))

    self.client.execute('refresh functions %s' % unique_database)
    self.client.execute('refresh functions %s' % unique_database.upper())

  @SkipIfS3.hive
  @SkipIfGCS.hive
  @SkipIfABFS.hive
  @SkipIfADLS.hive
  @SkipIfIsilon.hive
  @SkipIfLocal.hive
  def test_refresh_updated_partitions(self, unique_database):
    """
    Test to exercise and confirm the query option REFRESH_UPDATED_HMS_PARTITIONS
    works as expected (IMPALA-4364).
    """
    tbl = unique_database + "." + "test"
    self.client.execute(
      "create table {0} (c1 int) partitioned by (year int, month int)".format(tbl))
    # create 3 partitions and load data in them.
    self.client.execute("insert into table {0} partition (year, month)"
      "values (100, 2009, 1), (200, 2009, 2), (300, 2009, 3)".format(tbl))
    # add a new partition from hive
    self.run_stmt_in_hive(
      "alter table {0} add partition (year=2020, month=8)".format(tbl))
    self.client.execute("refresh {0}".format(tbl))

    # case 1: update the partition location
    self.run_stmt_in_hive(
      "alter table {0} partition (year=2020, month=8) "
      "set location 'hdfs:///tmp/year=2020/month=8'".format(tbl))
    # first try refresh without setting the query option
    self.execute_query("refresh {0}".format(tbl))
    result = self.execute_query("show partitions {0}".format(tbl))
    assert "/tmp/year=2020/month=8" not in result.get_data()
    self.execute_query("refresh {0}".format(tbl),
      query_options={"REFRESH_UPDATED_HMS_PARTITIONS": 0})
    result = self.execute_query("show partitions {0}".format(tbl))
    assert "/tmp/year=2020/month=8" not in result.get_data()
    self.execute_query("refresh {0}".format(tbl),
      query_options={"REFRESH_UPDATED_HMS_PARTITIONS": "False"})
    result = self.execute_query("show partitions {0}".format(tbl))
    assert "/tmp/year=2020/month=8" not in result.get_data()

    # now issue a refresh with the query option set
    self.execute_query("refresh {0}".format(tbl),
      query_options={"REFRESH_UPDATED_HMS_PARTITIONS": 1})
    result = self.execute_query("show partitions {0}".format(tbl))
    assert "/tmp/year=2020/month=8" in result.get_data()

    # change the location back to original and test using the query option
    # set as true
    new_loc = "/test-warehouse/{0}.db/{1}/year=2020/month=8".format(
      unique_database, "test")
    self.run_stmt_in_hive("alter table {0} partition (year=2020, month=8) "
      "set location 'hdfs://{1}'".format(tbl, new_loc))
    self.execute_query("refresh {0}".format(tbl),
      query_options={"REFRESH_UPDATED_HMS_PARTITIONS": "true"})
    result = self.execute_query("show partitions {0}".format(tbl))
    assert new_loc in result.get_data()
    result = self.get_impala_partition_info(unique_database + ".test", "year", "month")
    assert len(result) == 4

    # case2: change the partition to a different file-format, note that the table's
    # file-format is text.
    # add another test partition. It should use the default file-format from the table.
    self.execute_query("alter table {0} add partition (year=2020, month=9)".format(tbl))
    # change the partition file-format from hive
    self.run_stmt_in_hive("alter table {0} partition (year=2020, month=9) "
                          "set fileformat parquet".format(tbl))
    # make sure that refresh without the query option does not update the partition
    self.execute_query("refresh {0}".format(tbl))
    self.execute_query("insert into {0} partition (year=2020, month=9) "
                       "select c1 from {0} where year=2009 and month=1".format(tbl))
    result = self.execute_query(
      "show files in {0} partition (year=2020, month=8)".format(tbl))
    assert ".parq" not in result.get_data()
    # change the file-format for another partition from hive
    self.run_stmt_in_hive("alter table {0} partition (year=2020, month=8) "
    "set fileformat parquet".format(tbl))
    # now try refresh with the query option set
    self.execute_query("refresh {0}".format(tbl),
      query_options={"REFRESH_UPDATED_HMS_PARTITIONS": 1})
    self.execute_query("insert into {0} partition (year=2020, month=8) "
      "select c1 from {0} where year=2009 and month=1".format(tbl))
    # make sure the partition year=2020/month=8 is parquet fileformat
    result = self.execute_query(
      "show files in {0} partition (year=2020, month=8)".format(tbl))
    assert ".parq" in result.get_data()
    result = self.get_impala_partition_info(unique_database + ".test", "year", "month")
    assert len(result) == 5
    # make sure that the other partitions are still in text format new as well as old
    self.execute_query("insert into {0} partition (year=2020, month=1) "
      "select c1 from {0} where year=2009 and month=1".format(tbl))
    result = self.execute_query(
      "show files in {0} partition (year=2020, month=1)".format(tbl))
    assert ".txt" in result.get_data()
    result = self.get_impala_partition_info(unique_database + ".test", "year", "month")
    assert len(result) == 6
    self.execute_query("insert into {0} partition (year=2009, month=3) "
                       "select c1 from {0} where year=2009 and month=1".format(tbl))
    result = self.execute_query(
      "show files in {0} partition (year=2009, month=3)".format(tbl))
    assert ".txt" in result.get_data()
    result = self.get_impala_partition_info(unique_database + ".test", "year", "month")
    assert len(result) == 6
