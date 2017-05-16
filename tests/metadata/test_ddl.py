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

import getpass
import pytest
import time

from test_ddl_base import TestDdlBase
from tests.common.impala_test_suite import LOG
from tests.common.parametrize import UniqueDatabase
from tests.common.skip import SkipIf, SkipIfLocal, SkipIfOldAggsJoins
from tests.common.test_dimensions import create_single_exec_option_dimension
from tests.util.filesystem_utils import WAREHOUSE, IS_HDFS, IS_LOCAL, IS_S3

# Validates DDL statements (create, drop)
class TestDdlStatements(TestDdlBase):
  @SkipIfLocal.hdfs_client
  def test_drop_table_with_purge(self, unique_database):
    """This test checks if the table data is permamently deleted in
    DROP TABLE <tbl> PURGE queries"""
    self.client.execute("create table {0}.t1(i int)".format(unique_database))
    self.client.execute("create table {0}.t2(i int)".format(unique_database))
    # Create sample test data files under the table directories
    self.filesystem_client.create_file("test-warehouse/{0}.db/t1/t1.txt".\
        format(unique_database), file_data='t1')
    self.filesystem_client.create_file("test-warehouse/{0}.db/t2/t2.txt".\
        format(unique_database), file_data='t2')
    # Drop the table (without purge) and make sure it exists in trash
    self.client.execute("drop table {0}.t1".format(unique_database))
    assert not self.filesystem_client.exists("test-warehouse/{0}.db/t1/t1.txt".\
        format(unique_database))
    assert not self.filesystem_client.exists("test-warehouse/{0}.db/t1/".\
        format(unique_database))
    assert self.filesystem_client.exists(\
        "user/{0}/.Trash/Current/test-warehouse/{1}.db/t1/t1.txt".\
        format(getpass.getuser(), unique_database))
    assert self.filesystem_client.exists(\
        "user/{0}/.Trash/Current/test-warehouse/{1}.db/t1".\
        format(getpass.getuser(), unique_database))
    # Drop the table (with purge) and make sure it doesn't exist in trash
    self.client.execute("drop table {0}.t2 purge".format(unique_database))
    if not IS_S3:
      # In S3, deletes are eventual. So even though we dropped the table, the files
      # belonging to this table will still be visible for some unbounded time. This
      # happens only with PURGE. A regular DROP TABLE is just a copy of files which is
      # consistent.
      assert not self.filesystem_client.exists("test-warehouse/{0}.db/t2/".\
          format(unique_database))
      assert not self.filesystem_client.exists("test-warehouse/{0}.db/t2/t2.txt".\
          format(unique_database))
    assert not self.filesystem_client.exists(\
        "user/{0}/.Trash/Current/test-warehouse/{1}.db/t2/t2.txt".\
        format(getpass.getuser(), unique_database))
    assert not self.filesystem_client.exists(\
        "user/{0}/.Trash/Current/test-warehouse/{1}.db/t2".\
        format(getpass.getuser(), unique_database))
    # Create an external table t3 and run the same test as above. Make
    # sure the data is not deleted
    self.filesystem_client.make_dir(
        "test-warehouse/{0}.db/data_t3/".format(unique_database), permission=777)
    self.filesystem_client.create_file(
        "test-warehouse/{0}.db/data_t3/data.txt".format(unique_database), file_data='100')
    self.client.execute("create external table {0}.t3(i int) stored as "
      "textfile location \'/test-warehouse/{0}.db/data_t3\'" .format(unique_database))
    self.client.execute("drop table {0}.t3 purge".format(unique_database))
    assert self.filesystem_client.exists(
        "test-warehouse/{0}.db/data_t3/data.txt".format(unique_database))
    self.filesystem_client.delete_file_dir(
        "test-warehouse/{0}.db/data_t3".format(unique_database), recursive=True)

  @SkipIfLocal.hdfs_client
  def test_drop_cleans_hdfs_dirs(self, unique_database):
    self.client.execute('use default')
    # Verify the db directory exists
    assert self.filesystem_client.exists(
        "test-warehouse/{0}.db/".format(unique_database))

    self.client.execute("create table {0}.t1(i int)".format(unique_database))
    # Verify the table directory exists
    assert self.filesystem_client.exists(
        "test-warehouse/{0}.db/t1/".format(unique_database))

    # Dropping the table removes the table's directory and preserves the db's directory
    self.client.execute("drop table {0}.t1".format(unique_database))
    assert not self.filesystem_client.exists(
        "test-warehouse/{0}.db/t1/".format(unique_database))
    assert self.filesystem_client.exists(
        "test-warehouse/{0}.db/".format(unique_database))

    # Dropping the db removes the db's directory
    self.client.execute("drop database {0}".format(unique_database))
    assert not self.filesystem_client.exists(
        "test-warehouse/{0}.db/".format(unique_database))

    # Dropping the db using "cascade" removes all tables' and db's directories
    # but keeps the external tables' directory
    self._create_db(unique_database)
    self.client.execute("create table {0}.t1(i int)".format(unique_database))
    self.client.execute("create table {0}.t2(i int)".format(unique_database))
    result = self.client.execute("create external table {0}.t3(i int) "
        "location '{1}/{0}/t3/'".format(unique_database, WAREHOUSE))
    self.client.execute("drop database {0} cascade".format(unique_database))
    assert not self.filesystem_client.exists(
        "test-warehouse/{0}.db/".format(unique_database))
    assert not self.filesystem_client.exists(
        "test-warehouse/{0}.db/t1/".format(unique_database))
    assert not self.filesystem_client.exists(
        "test-warehouse/{0}.db/t2/".format(unique_database))
    assert self.filesystem_client.exists(
        "test-warehouse/{0}/t3/".format(unique_database))
    self.filesystem_client.delete_file_dir(
        "test-warehouse/{0}/t3/".format(unique_database), recursive=True)
    assert not self.filesystem_client.exists(
        "test-warehouse/{0}/t3/".format(unique_database))
    # Re-create database to make unique_database teardown succeed.
    self._create_db(unique_database)

  @SkipIfLocal.hdfs_client
  def test_truncate_cleans_hdfs_files(self, unique_database):
    # Verify the db directory exists
    assert self.filesystem_client.exists(
        "test-warehouse/{0}.db/".format(unique_database))

    self.client.execute("create table {0}.t1(i int)".format(unique_database))
    # Verify the table directory exists
    assert self.filesystem_client.exists(
        "test-warehouse/{0}.db/t1/".format(unique_database))

    try:
      # If we're testing S3, we want the staging directory to be created.
      self.client.execute("set s3_skip_insert_staging=false")
      # Should have created one file in the table's dir
      self.client.execute("insert into {0}.t1 values (1)".format(unique_database))
      assert len(self.filesystem_client.ls(
          "test-warehouse/{0}.db/t1/".format(unique_database))) == 2

      # Truncating the table removes the data files and preserves the table's directory
      self.client.execute("truncate table {0}.t1".format(unique_database))
      assert len(self.filesystem_client.ls(
          "test-warehouse/{0}.db/t1/".format(unique_database))) == 1

      self.client.execute(
          "create table {0}.t2(i int) partitioned by (p int)".format(unique_database))
      # Verify the table directory exists
      assert self.filesystem_client.exists(
          "test-warehouse/{0}.db/t2/".format(unique_database))

      # Should have created the partition dir, which should contain exactly one file
      self.client.execute(
          "insert into {0}.t2 partition(p=1) values (1)".format(unique_database))
      assert len(self.filesystem_client.ls(
          "test-warehouse/{0}.db/t2/p=1".format(unique_database))) == 1

      # Truncating the table removes the data files and preserves the partition's directory
      self.client.execute("truncate table {0}.t2".format(unique_database))
      assert self.filesystem_client.exists(
          "test-warehouse/{0}.db/t2/p=1".format(unique_database))
      assert len(self.filesystem_client.ls(
          "test-warehouse/{0}.db/t2/p=1".format(unique_database))) == 0
    finally:
      # Reset to its default value.
      self.client.execute("set s3_skip_insert_staging=true")

  @UniqueDatabase.parametrize(sync_ddl=True)
  def test_truncate_table(self, vector, unique_database):
    vector.get_value('exec_option')['abort_on_error'] = False
    self.run_test_case('QueryTest/truncate-table', vector, use_db=unique_database,
        multiple_impalad=self._use_multiple_impalad(vector))

  @UniqueDatabase.parametrize(sync_ddl=True)
  def test_create_database(self, vector, unique_database):
    # The unique_database provides the .test a unique database name which allows
    # us to run this test in parallel with others.
    self.run_test_case('QueryTest/create-database', vector, use_db=unique_database,
        multiple_impalad=self._use_multiple_impalad(vector))

  # There is a query in QueryTest/create-table that references nested types, which is not
  # supported if old joins and aggs are enabled. Since we do not get any meaningful
  # additional coverage by running a DDL test under the old aggs and joins, it can be
  # skipped.
  @SkipIfOldAggsJoins.nested_types
  @UniqueDatabase.parametrize(sync_ddl=True)
  def test_create_table(self, vector, unique_database):
    vector.get_value('exec_option')['abort_on_error'] = False
    self.run_test_case('QueryTest/create-table', vector, use_db=unique_database,
        multiple_impalad=self._use_multiple_impalad(vector))

  @UniqueDatabase.parametrize(sync_ddl=True)
  def test_create_table_like_table(self, vector, unique_database):
    vector.get_value('exec_option')['abort_on_error'] = False
    self.run_test_case('QueryTest/create-table-like-table', vector,
        use_db=unique_database, multiple_impalad=self._use_multiple_impalad(vector))

  @UniqueDatabase.parametrize(sync_ddl=True)
  def test_create_table_like_file(self, vector, unique_database):
    vector.get_value('exec_option')['abort_on_error'] = False
    self.run_test_case('QueryTest/create-table-like-file', vector,
        use_db=unique_database, multiple_impalad=self._use_multiple_impalad(vector))

  @UniqueDatabase.parametrize(sync_ddl=True)
  def test_create_table_as_select(self, vector, unique_database):
    vector.get_value('exec_option')['abort_on_error'] = False
    self.run_test_case('QueryTest/create-table-as-select', vector,
        use_db=unique_database, multiple_impalad=self._use_multiple_impalad(vector))

  @SkipIf.kudu_not_supported
  @UniqueDatabase.parametrize(sync_ddl=True)
  def test_create_kudu(self, vector, unique_database):
    vector.get_value('exec_option')['abort_on_error'] = False
    self.run_test_case('QueryTest/kudu_create', vector, use_db=unique_database,
        multiple_impalad=self._use_multiple_impalad(vector))

  @UniqueDatabase.parametrize(sync_ddl=True)
  def test_sync_ddl_drop(self, vector, unique_database):
    """Verifies the catalog gets updated properly when dropping objects with sync_ddl
    enabled"""
    self.client.set_configuration({'sync_ddl': 1})
    # Drop the database immediately after creation (within a statestore heartbeat) and
    # verify the catalog gets updated properly.
    self.client.execute("drop database {0}".format(unique_database))
    assert unique_database not in self.all_db_names()
    # Re-create database to make unique_database teardown succeed.
    self._create_db(unique_database)

  # TODO: don't use hdfs_client
  @SkipIfLocal.hdfs_client
  @UniqueDatabase.parametrize(sync_ddl=True, num_dbs=2)
  def test_alter_table(self, vector, unique_database):
    vector.get_value('exec_option')['abort_on_error'] = False

    # Create an unpartitioned table to get a filesystem directory that does not
    # use the (key=value) format. The directory is automatically cleanup up
    # by the unique_database fixture.
    self.client.execute("create table {0}.part_data (i int)".format(unique_database))
    assert self.filesystem_client.exists(
        "test-warehouse/{0}.db/part_data".format(unique_database))
    self.filesystem_client.create_file(
        "test-warehouse/{0}.db/part_data/data.txt".format(unique_database),
        file_data='1984')
    self.run_test_case('QueryTest/alter-table', vector, use_db=unique_database,
        multiple_impalad=self._use_multiple_impalad(vector))
    # The following tests require HDFS caching which is supported only in the HDFS
    # filesystem.
    if IS_HDFS:
      self.run_test_case('QueryTest/alter-table-hdfs-caching', vector,
          use_db=unique_database, multiple_impalad=self._use_multiple_impalad(vector))

  @UniqueDatabase.parametrize(sync_ddl=True)
  def test_alter_set_column_stats(self, vector, unique_database):
    self.run_test_case('QueryTest/alter-table-set-column-stats', vector,
        use_db=unique_database, multiple_impalad=self._use_multiple_impalad(vector))

  @SkipIfLocal.hdfs_client
  def test_drop_partition_with_purge(self, vector, unique_database):
    """Verfies whether alter <tbl> drop partition purge actually skips trash"""
    self.client.execute(
        "create table {0}.t1(i int) partitioned by (j int)".format(unique_database))
    # Add two partitions (j=1) and (j=2) to table t1
    self.client.execute("alter table {0}.t1 add partition(j=1)".format(unique_database))
    self.client.execute("alter table {0}.t1 add partition(j=2)".format(unique_database))
    self.filesystem_client.create_file(\
        "test-warehouse/{0}.db/t1/j=1/j1.txt".format(unique_database), file_data='j1')
    self.filesystem_client.create_file(\
        "test-warehouse/{0}.db/t1/j=2/j2.txt".format(unique_database), file_data='j2')
    # Drop the partition (j=1) without purge and make sure it exists in trash
    self.client.execute("alter table {0}.t1 drop partition(j=1)".format(unique_database))
    assert not self.filesystem_client.exists("test-warehouse/{0}.db/t1/j=1/j1.txt".\
        format(unique_database))
    assert not self.filesystem_client.exists("test-warehouse/{0}.db/t1/j=1".\
        format(unique_database))
    assert self.filesystem_client.exists(\
        "user/{0}/.Trash/Current/test-warehouse/{1}.db/t1/j=1/j1.txt".\
        format(getpass.getuser(), unique_database))
    assert self.filesystem_client.exists(\
        "user/{0}/.Trash/Current/test-warehouse/{1}.db/t1/j=1".\
        format(getpass.getuser(), unique_database))
    # Drop the partition (with purge) and make sure it doesn't exist in trash
    self.client.execute("alter table {0}.t1 drop partition(j=2) purge".\
        format(unique_database));
    if not IS_S3:
      # In S3, deletes are eventual. So even though we dropped the partition, the files
      # belonging to this partition will still be visible for some unbounded time. This
      # happens only with PURGE. A regular DROP TABLE is just a copy of files which is
      # consistent.
      assert not self.filesystem_client.exists("test-warehouse/{0}.db/t1/j=2/j2.txt".\
          format(unique_database))
      assert not self.filesystem_client.exists("test-warehouse/{0}.db/t1/j=2".\
          format(unique_database))
    assert not self.filesystem_client.exists(\
        "user/{0}/.Trash/Current/test-warehouse/{1}.db/t1/j=2".\
        format(getpass.getuser(), unique_database))
    assert not self.filesystem_client.exists(
        "user/{0}/.Trash/Current/test-warehouse/{1}.db/t1/j=2/j2.txt".\
        format(getpass.getuser(), unique_database))

  @UniqueDatabase.parametrize(sync_ddl=True)
  def test_views_ddl(self, vector, unique_database):
    vector.get_value('exec_option')['abort_on_error'] = False
    self.run_test_case('QueryTest/views-ddl', vector, use_db=unique_database,
        multiple_impalad=self._use_multiple_impalad(vector))

  @UniqueDatabase.parametrize(sync_ddl=True)
  def test_functions_ddl(self, vector, unique_database):
    self.run_test_case('QueryTest/functions-ddl', vector, use_db=unique_database,
        multiple_impalad=self._use_multiple_impalad(vector))

  @SkipIfLocal.hdfs_client
  def test_create_alter_bulk_partition(self, vector, unique_database):
    # Change the scale depending on the exploration strategy, with 50 partitions this
    # test runs a few minutes, with 10 partitions it takes ~50s for two configurations.
    num_parts = 50 if self.exploration_strategy() == 'exhaustive' else 10
    fq_tbl_name = unique_database + ".part_test_tbl"
    self.client.execute("create table {0}(i int) partitioned by(j int, s string) "
         "location '{1}/{0}'".format(fq_tbl_name, WAREHOUSE))

    # Add some partitions (first batch of two)
    for i in xrange(num_parts / 5):
      start = time.time()
      self.client.execute(
          "alter table {0} add partition(j={1}, s='{1}')".format(fq_tbl_name, i))
      LOG.info('ADD PARTITION #%d exec time: %s' % (i, time.time() - start))

    # Modify one of the partitions
    self.client.execute("alter table {0} partition(j=1, s='1')"
        " set fileformat parquetfile".format(fq_tbl_name))

    # Alter one partition to a non-existent location twice (IMPALA-741)
    self.filesystem_client.delete_file_dir("tmp/dont_exist1/", recursive=True)
    self.filesystem_client.delete_file_dir("tmp/dont_exist2/", recursive=True)

    self.execute_query_expect_success(self.client,
        "alter table {0} partition(j=1,s='1') set location '{1}/tmp/dont_exist1'"
        .format(fq_tbl_name, WAREHOUSE))
    self.execute_query_expect_success(self.client,
        "alter table {0} partition(j=1,s='1') set location '{1}/tmp/dont_exist2'"
        .format(fq_tbl_name, WAREHOUSE))

    # Add some more partitions
    for i in xrange(num_parts / 5, num_parts):
      start = time.time()
      self.client.execute(
          "alter table {0} add partition(j={1},s='{1}')".format(fq_tbl_name, i))
      LOG.info('ADD PARTITION #%d exec time: %s' % (i, time.time() - start))

    # Insert data and verify it shows up.
    self.client.execute(
        "insert into table {0} partition(j=1, s='1') select 1".format(fq_tbl_name))
    assert '1' == self.execute_scalar("select count(*) from {0}".format(fq_tbl_name))

  def test_create_alter_tbl_properties(self, vector, unique_database):
    fq_tbl_name = unique_database + ".test_alter_tbl"

    # Specify TBLPROPERTIES and SERDEPROPERTIES at CREATE time
    self.client.execute("""create table {0} (i int)
    with serdeproperties ('s1'='s2', 's3'='s4')
    tblproperties ('p1'='v0', 'p1'='v1')""".format(fq_tbl_name))
    properties = self._get_tbl_properties(fq_tbl_name)

    assert len(properties) == 2
    # The transient_lastDdlTime is variable, so don't verify the value.
    assert 'transient_lastDdlTime' in properties
    del properties['transient_lastDdlTime']
    assert {'p1': 'v1'} == properties

    properties = self._get_serde_properties(fq_tbl_name)
    assert {'s1': 's2', 's3': 's4'} == properties

    # Modify the SERDEPROPERTIES using ALTER TABLE SET.
    self.client.execute("alter table {0} set serdeproperties "
        "('s1'='new', 's5'='s6')".format(fq_tbl_name))
    properties = self._get_serde_properties(fq_tbl_name)
    assert {'s1': 'new', 's3': 's4', 's5': 's6'} == properties

    # Modify the TBLPROPERTIES using ALTER TABLE SET.
    self.client.execute("alter table {0} set tblproperties "
        "('prop1'='val1', 'p2'='val2', 'p2'='val3', ''='')".format(fq_tbl_name))
    properties = self._get_tbl_properties(fq_tbl_name)

    assert 'transient_lastDdlTime' in properties
    assert properties['p1'] == 'v1'
    assert properties['prop1'] == 'val1'
    assert properties['p2'] == 'val3'
    assert properties[''] == ''

  @UniqueDatabase.parametrize(sync_ddl=True)
  def test_partition_ddl_predicates(self, vector, unique_database):
    self.run_test_case('QueryTest/partition-ddl-predicates-all-fs', vector,
        use_db=unique_database, multiple_impalad=self._use_multiple_impalad(vector))
    if IS_HDFS:
      self.run_test_case('QueryTest/partition-ddl-predicates-hdfs-only', vector,
          use_db=unique_database, multiple_impalad=self._use_multiple_impalad(vector))

# IMPALA-2002: Tests repeated adding/dropping of .jar and .so in the lib cache.
class TestLibCache(TestDdlBase):
  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestLibCache, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(create_single_exec_option_dimension())

  def test_create_drop_function(self, vector, unique_database):
    """This will create, run, and drop the same function repeatedly, exercising the
    lib cache mechanism.
    """
    create_fn_stmt = ("create function {0}.f() returns int "
                      "location '{1}/libTestUdfs.so' symbol='NoArgs'"
                      .format(unique_database, WAREHOUSE))
    select_stmt = ("select {0}.f() from functional.alltypes limit 10"
                   .format(unique_database))
    drop_fn_stmt = "drop function %s {0}.f()".format(unique_database)
    self.create_drop_ddl(vector, [create_fn_stmt], [drop_fn_stmt], select_stmt)

  # Run serially because this test inspects global impalad metrics.
  # TODO: The metrics checks could be relaxed to enable running this test in
  # parallel, but that might need a more general wait_for_metric_value().
  @pytest.mark.execute_serially
  def test_create_drop_data_src(self, vector, unique_database):
    """This will create, run, and drop the same data source repeatedly, exercising
    the lib cache mechanism.
    """
    data_src_name = unique_database + "_datasrc"
    create_ds_stmt = ("CREATE DATA SOURCE {0} "
        "LOCATION '{1}/data-sources/test-data-source.jar' "
        "CLASS 'org.apache.impala.extdatasource.AllTypesDataSource' "
        "API_VERSION 'V1'".format(data_src_name, WAREHOUSE))
    create_tbl_stmt = ("CREATE TABLE {0}.data_src_tbl (x int) "
        "PRODUCED BY DATA SOURCE {1}('dummy_init_string')")\
        .format(unique_database, data_src_name)
    drop_ds_stmt = "drop data source %s {0}".format(data_src_name)
    drop_tbl_stmt = "drop table %s {0}.data_src_tbl".format(unique_database)
    select_stmt = "select * from {0}.data_src_tbl limit 1".format(unique_database)
    class_cache_hits_metric = "external-data-source.class-cache.hits"
    class_cache_misses_metric = "external-data-source.class-cache.misses"

    create_stmts = [create_ds_stmt, create_tbl_stmt]
    drop_stmts = [drop_tbl_stmt, drop_ds_stmt]

    # The ImpaladService is used to capture metrics
    service = self.impalad_test_service

    # Initial metric values
    class_cache_hits = service.get_metric_value(class_cache_hits_metric)
    class_cache_misses = service.get_metric_value(class_cache_misses_metric)
    # Test with 1 node so we can check the metrics on only the coordinator
    vector.get_value('exec_option')['num_nodes'] = 1
    num_iterations = 2
    self.create_drop_ddl(vector, create_stmts, drop_stmts, select_stmt, num_iterations)

    # Check class cache metrics. Shouldn't have any new cache hits, there should be
    # 2 cache misses for every iteration (jar is loaded by both the FE and BE).
    expected_cache_misses = class_cache_misses + (num_iterations * 2)
    service.wait_for_metric_value(class_cache_hits_metric, class_cache_hits)
    service.wait_for_metric_value(class_cache_misses_metric,
        expected_cache_misses)

    # Test with a table that caches the class
    create_tbl_stmt = ("CREATE TABLE {0}.data_src_tbl (x int) "
        "PRODUCED BY DATA SOURCE {1}('CACHE_CLASS::dummy_init_string')")\
        .format(unique_database, data_src_name)
    create_stmts = [create_ds_stmt, create_tbl_stmt]
    # Run once before capturing metrics because the class already may be cached from
    # a previous test run.
    # TODO: Provide a way to clear the cache
    self.create_drop_ddl(vector, create_stmts, drop_stmts, select_stmt, 1)

    # Capture metric values and run again, should hit the cache.
    class_cache_hits = service.get_metric_value(class_cache_hits_metric)
    class_cache_misses = service.get_metric_value(class_cache_misses_metric)
    self.create_drop_ddl(vector, create_stmts, drop_stmts, select_stmt, 1)
    service.wait_for_metric_value(class_cache_hits_metric, class_cache_hits + 2)
    service.wait_for_metric_value(class_cache_misses_metric, class_cache_misses)

  def create_drop_ddl(self, vector, create_stmts, drop_stmts, select_stmt,
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
    self.client.set_configuration(vector.get_value("exec_option"))
    for drop_stmt in drop_stmts: self.client.execute(drop_stmt % ("if exists"))
    for i in xrange(0, num_iterations):
      for create_stmt in create_stmts: self.client.execute(create_stmt)
      self.client.execute(select_stmt)
      for drop_stmt in drop_stmts: self.client.execute(drop_stmt % (""))
