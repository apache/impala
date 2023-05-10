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
from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.skip import SkipIfFS


@SkipIfFS.hive
class TestMetadataNoEventsProcessing(CustomClusterTestSuite):

  @CustomClusterTestSuite.with_args(catalogd_args="--hms_event_polling_interval_s=0")
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

  @CustomClusterTestSuite.with_args(catalogd_args="--hms_event_polling_interval_s=0")
  def test_add_overlapping_partitions(self, vector, unique_database):
    """
    IMPALA-1670, IMPALA-4141: Test interoperability with Hive when adding overlapping
    partitions to a table
    """
    # Create a table in Impala.
    db_name = unique_database
    tbl_name = "test_overlapping_parts"
    table_name = db_name + "." + tbl_name
    self.execute_query("create table {0}.{1} (a int) partitioned by (x int)"
      .format(db_name, tbl_name))
    # Trigger metadata load. No partitions exist yet in Impala.
    assert [] == self.get_impala_partition_info(table_name, 'x')

    # Add partition in Hive.
    self.run_stmt_in_hive("alter table %s add partition (x=2)" % table_name)
    # Impala is not aware of the new partition.
    assert [] == self.get_impala_partition_info(table_name, 'x')

    # Try to add partitions with caching in Impala, one of them (x=2) exists in HMS.
    exception = None
    try:
      self.client.execute(
        "alter table %s add partition (x=1) uncached "
        "partition (x=2) cached in 'testPool' with replication=2 "
        "partition (x=3) cached in 'testPool' with replication=3" % table_name)
    except Exception as e:
      exception = e
      assert "Partition already exists" in str(e)
    assert exception is not None, "should have triggered an error"

    # No partitions were added in Impala.
    assert [] == self.get_impala_partition_info(table_name, 'x')

    # It should succeed with IF NOT EXISTS.
    self.client.execute("alter table %s add if not exists partition (x=1) uncached "
        "partition (x=2) cached in 'testPool' with replication=2 "
        "partition (x=3) cached in 'testPool' with replication=3" % table_name)

    # Hive sees all the partitions.
    assert ['x=1', 'x=2', 'x=3'] == self.hive_partition_names(table_name)

    # Impala sees the partition that has already existed in HMS (x=2) and the newly
    # added partitions (x=1) and (x=3).
    # Caching has been applied only to newly added partitions (x=1) and (x=3), the
    # preexisting partition (x=2) was not modified.
    partitions = self.get_impala_partition_info(table_name, 'x', 'Bytes Cached',
        'Cache Replication')
    assert [('1', 'NOT CACHED', 'NOT CACHED'),
        ('2', 'NOT CACHED', 'NOT CACHED'),
        ('3', '0B', '3')] == partitions

    # Try to add location to a partition that is already in catalog cache (x=1).
    self.client.execute("alter table %s add if not exists "
        "partition (x=1) location '/_X_1'" % table_name)
    # (x=1) partition's location hasn't changed
    (x1_value, x1_location) = self.get_impala_partition_info(table_name, 'x',
        'Location')[0]
    assert '1' == x1_value
    assert x1_location.endswith("/x=1")

  @CustomClusterTestSuite.with_args(catalogd_args="--hms_event_polling_interval_s=0")
  def test_add_preexisting_partitions_with_data(self, unique_database, vector):
    """
    IMPALA-1670, IMPALA-4141: After addding partitions that already exist in HMS, Impala
    can access the partition data.
    """
    # Create a table in Impala.
    table_name = "{0}.test_tbl".format(unique_database)
    self.client.execute("drop table if exists {0}".format(table_name))
    self.client.execute(
      "create table {0} (a int) partitioned by (x int)".format(table_name))
    # Trigger metadata load. No partitions exist yet in Impala.
    assert [] == self.get_impala_partition_info(table_name, 'x')

    # Add partitions in Hive.
    self.run_stmt_in_hive("alter table %s add partition (x=1) "
        "partition (x=2) "
        "partition (x=3)" % table_name)
    # Insert rows in Hive
    self.run_stmt_in_hive("insert into %s partition(x=1) values (1), (2), (3)"
        % table_name)
    self.run_stmt_in_hive("insert into %s partition(x=2) values (1), (2), (3), (4)"
        % table_name)
    self.run_stmt_in_hive("insert into %s partition(x=3) values (1)"
        % table_name)
    # No partitions exist yet in Impala.
    assert [] == self.get_impala_partition_info(table_name, 'x')

    # Add the same partitions in Impala with IF NOT EXISTS.
    self.client.execute("alter table %s add if not exists partition (x=1) "
        "partition (x=2) partition (x=3)" % table_name)
    # Impala sees the partitions
    assert [('1',), ('2',), ('3',)] == self.get_impala_partition_info(table_name, 'x')
    # Data exists in Impala
    assert ['1\t1', '1\t2', '1\t3',
        '2\t1', '2\t2', '2\t3', '2\t4',
        '3\t1'] == \
            self.client.execute(
                'select x, a from %s order by x, a' % table_name).get_data().split('\n')

  @CustomClusterTestSuite.with_args(catalogd_args="--hms_event_polling_interval_s=0")
  def test_refresh_invalid_partition(self, vector, unique_database):
    """
    Trying to refresh a partition that does not exist does not modify anything
    either in impala or hive.
    """
    table_name = unique_database + '.' + "partition_test_table"
    self.client.execute(
        'create table %s (x int) partitioned by (y int, z int)' %
        table_name)
    self.client.execute(
        'alter table %s add partition (y=333, z=5309)' % table_name)
    assert [('333', '5309')] == self.get_impala_partition_info(table_name, 'y', 'z')
    assert ['y=333/z=5309'] == self.hive_partition_names(table_name)
    self.client.execute('refresh %s partition (y=71, z=8857)' % table_name)
    assert [('333', '5309')] == self.get_impala_partition_info(table_name, 'y', 'z')
    assert ['y=333/z=5309'] == self.hive_partition_names(table_name)

  @CustomClusterTestSuite.with_args(catalogd_args="--hms_event_polling_interval_s=0")
  def test_add_data_and_refresh(self, vector, unique_database):
    """
    Data added through hive is visible in impala after refresh of partition.
    """
    table_name = unique_database + '.' + "partition_test_table"
    self.client.execute(
        'create table %s (x int) partitioned by (y int, z int)' %
        table_name)
    self.client.execute(
        'alter table %s add partition (y=333, z=5309)' % table_name)
    result = self.client.execute("select count(*) from %s" % table_name)
    assert result.data == [str('0')]
    self.run_stmt_in_hive(
        'insert into table %s partition (y=333, z=5309) values (2)'
        % table_name)
    # Make sure its still shows the same result before refreshing
    result = self.client.execute("select count(*) from %s" % table_name)
    valid_counts = [0]
    assert int(result.data[0]) in valid_counts

    self.client.execute('refresh %s partition (y=333, z=5309)' % table_name)
    assert '2\t333\t5309' == self.client.execute(
        'select * from %s' % table_name).get_data()

  @CustomClusterTestSuite.with_args(catalogd_args="--hms_event_polling_interval_s=0")
  def test_refresh_partition_num_rows(self, vector, unique_database):
    """Refreshing a partition should not change it's numRows stat."""
    # Create a partitioned table and add data to it.
    tbl = unique_database + ".t1"
    self.client.execute("create table %s(a int) partitioned by (b int)" % tbl)
    self.client.execute("insert into %s partition(b=1) values (1)" % tbl)
    # Compute stats on tbl. It should populate the partition num rows.
    self.client.execute("compute stats %s" % tbl)
    result = self.client.execute("show partitions %s" % tbl)
    # Format: partition/#Rows/#Files (first 3 entries)
    assert result.get_data().startswith("1\t1\t1"),\
        "Incorrect partition stats %s" % result.get_data()
    # Add another file to the same partition using hive.
    self.run_stmt_in_hive("insert into table %s partition (b=1) values (2)" % tbl)
    # Make sure Impala still sees a single row.
    assert "1" == self.client.execute("select count(*) from %s" % tbl).get_data()

    # refresh the partition and make sure the new row is visible
    self.client.execute("refresh %s partition (b=1)" % tbl)
    assert "2" == self.client.execute("select count(*) from %s" % tbl).get_data()
    # Make sure the partition num rows are unchanged and still 1 but the
    # #files is updated.
    result = self.client.execute("show partitions %s" % tbl)
    assert result.get_data().startswith("1\t1\t2"),\
        "Incorrect partition stats %s" % result.get_data()
    # Do a full table refresh and it should still remain the same.
    self.client.execute("refresh %s" % tbl)
    result = self.client.execute("show partitions %s" % tbl)
    assert result.get_data().startswith("1\t1\t2"),\
        "Incorrect partition stats %s" % result.get_data()

  @CustomClusterTestSuite.with_args(catalogd_args="--hms_event_polling_interval_s=0")
  def test_invalidate_metadata(self, unique_name):
    """Verify invalidate metadata on tables under unloaded db won't fail"""
    db = unique_name + "_db"
    tbl = db + "." + unique_name + "_tbl"
    try:
      self.run_stmt_in_hive("create database " + db)
      self.run_stmt_in_hive("create table %s (i int)" % tbl)
      self.client.execute("invalidate metadata %s" % tbl)
      res = self.client.execute("describe %s" % tbl)
      assert res.data == ["i\tint\t"]
    finally:
      self.run_stmt_in_hive("drop database %s cascade" % db)

  @CustomClusterTestSuite.with_args(
    catalogd_args="--catalog_topic_mode=minimal --hms_event_polling_interval_s=0",
    impalad_args="--use_local_catalog=true")
  def test_invalidate_metadata_v2(self, unique_name):
    """Verify invalidate metadata on tables under unloaded db won't fail"""
    db = unique_name + "_db"
    tbl = db + "." + unique_name + "_tbl"
    try:
      self.run_stmt_in_hive("create database " + db)
      self.run_stmt_in_hive("create table %s (i int)" % tbl)
      self.client.execute("invalidate metadata %s" % tbl)
      res = self.client.execute("describe %s" % tbl)
      assert res.data == ["i\tint\t"]
    finally:
      self.run_stmt_in_hive("drop database %s cascade" % db)
