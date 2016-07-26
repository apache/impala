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
from tests.common.skip import SkipIf, SkipIfLocal, SkipIfOldAggsJoins
from tests.util.filesystem_utils import WAREHOUSE, IS_LOCAL, IS_S3

# Validates DDL statements (create, drop)
class TestDdlStatements(TestDdlBase):
  TEST_DBS = ['ddl_test_db', 'ddl_purge_db', 'alter_table_test_db',
              'alter_table_test_db2', 'function_ddl_test', 'udf_test', 'data_src_test',
              'truncate_table_test_db', 'test_db', 'alter_purge_db', 'db_with_comment']

  def setup_method(self, method):
    self._cleanup()

  def teardown_method(self, method):
    self._cleanup()

  def _cleanup(self):
    map(self.cleanup_db, self.TEST_DBS)

  @SkipIfLocal.hdfs_client
  @pytest.mark.execute_serially
  def test_drop_table_with_purge(self):
    """This test checks if the table data is permamently deleted in
    DROP TABLE <tbl> PURGE queries"""
    DDL_PURGE_DB = "ddl_purge_db"
    # Create a sample database ddl_purge_db and tables t1,t2 under it
    self._create_db(DDL_PURGE_DB)
    self.client.execute("create table {0}.t1(i int)".format(DDL_PURGE_DB))
    self.client.execute("create table {0}.t2(i int)".format(DDL_PURGE_DB))
    # Create sample test data files under the table directories
    self.filesystem_client.create_file("test-warehouse/{0}.db/t1/t1.txt".\
        format(DDL_PURGE_DB), file_data='t1')
    self.filesystem_client.create_file("test-warehouse/{0}.db/t2/t2.txt".\
        format(DDL_PURGE_DB), file_data='t2')
    # Drop the table (without purge) and make sure it exists in trash
    self.client.execute("drop table {0}.t1".format(DDL_PURGE_DB))
    assert not self.filesystem_client.exists("test-warehouse/{0}.db/t1/t1.txt".\
        format(DDL_PURGE_DB))
    assert not self.filesystem_client.exists("test-warehouse/{0}.db/t1/".\
        format(DDL_PURGE_DB))
    assert self.filesystem_client.exists(\
        "user/{0}/.Trash/Current/test-warehouse/{1}.db/t1/t1.txt".\
        format(getpass.getuser(), DDL_PURGE_DB))
    assert self.filesystem_client.exists(\
        "user/{0}/.Trash/Current/test-warehouse/{1}.db/t1".\
        format(getpass.getuser(), DDL_PURGE_DB))
    # Drop the table (with purge) and make sure it doesn't exist in trash
    self.client.execute("drop table {0}.t2 purge".format(DDL_PURGE_DB))
    if not IS_S3:
      # In S3, deletes are eventual. So even though we dropped the table, the files
      # belonging to this table will still be visible for some unbounded time. This
      # happens only with PURGE. A regular DROP TABLE is just a copy of files which is
      # consistent.
      assert not self.filesystem_client.exists("test-warehouse/{0}.db/t2/".\
          format(DDL_PURGE_DB))
      assert not self.filesystem_client.exists("test-warehouse/{0}.db/t2/t2.txt".\
          format(DDL_PURGE_DB))
    assert not self.filesystem_client.exists(\
        "user/{0}/.Trash/Current/test-warehouse/{1}.db/t2/t2.txt".\
        format(getpass.getuser(), DDL_PURGE_DB))
    assert not self.filesystem_client.exists(\
        "user/{0}/.Trash/Current/test-warehouse/{1}.db/t2".\
        format(getpass.getuser(), DDL_PURGE_DB))
    # Create an external table t3 and run the same test as above. Make
    # sure the data is not deleted
    self.filesystem_client.make_dir("test-warehouse/data_t3/", permission=777)
    self.filesystem_client.create_file("test-warehouse/data_t3/data.txt", file_data='100')
    self.client.execute("create external table {0}.t3(i int) stored as \
      textfile location \'/test-warehouse/data_t3\'" .format(DDL_PURGE_DB))
    self.client.execute("drop table {0}.t3 purge".format(DDL_PURGE_DB))
    assert self.filesystem_client.exists("test-warehouse/data_t3/data.txt")
    self.filesystem_client.delete_file_dir("test-warehouse/data_t3", recursive=True)

  @SkipIfLocal.hdfs_client
  @pytest.mark.execute_serially
  def test_drop_cleans_hdfs_dirs(self):
    DDL_TEST_DB = "ddl_test_db"
    self.filesystem_client.delete_file_dir("test-warehouse/ddl_test_db.db/",
                                           recursive=True)
    assert not self.filesystem_client.exists("test-warehouse/ddl_test_db.db/")

    self.client.execute('use default')
    self._create_db(DDL_TEST_DB)
    # Verify the db directory exists
    assert self.filesystem_client.exists("test-warehouse/{0}.db/".format(DDL_TEST_DB))

    self.client.execute("create table {0}.t1(i int)".format(DDL_TEST_DB))
    # Verify the table directory exists
    assert self.filesystem_client.exists("test-warehouse/{0}.db/t1/".format(DDL_TEST_DB))

    # Dropping the table removes the table's directory and preserves the db's directory
    self.client.execute("drop table {0}.t1".format(DDL_TEST_DB))
    assert not self.filesystem_client.exists(
        "test-warehouse/{0}.db/t1/".format(DDL_TEST_DB))
    assert self.filesystem_client.exists("test-warehouse/{0}.db/".format(DDL_TEST_DB))

    # Dropping the db removes the db's directory
    self.client.execute("drop database {0}".format(DDL_TEST_DB))
    assert not self.filesystem_client.exists("test-warehouse/{0}.db/".format(DDL_TEST_DB))

    # Dropping the db using "cascade" removes all tables' and db's directories
    # but keeps the external tables' directory
    self._create_db(DDL_TEST_DB)
    self.client.execute("create table {0}.t1(i int)".format(DDL_TEST_DB))
    self.client.execute("create table {0}.t2(i int)".format(DDL_TEST_DB))
    result = self.client.execute("create external table {0}.t3(i int) "
        "location '{1}/{0}/t3/'".format(DDL_TEST_DB, WAREHOUSE))
    self.client.execute("drop database {0} cascade".format(DDL_TEST_DB))
    assert not self.filesystem_client.exists("test-warehouse/{0}.db/".format(DDL_TEST_DB))
    assert not self.filesystem_client.exists(
        "test-warehouse/{0}.db/t1/".format(DDL_TEST_DB))
    assert not self.filesystem_client.exists(
        "test-warehouse/{0}.db/t2/".format(DDL_TEST_DB))
    assert self.filesystem_client.exists("test-warehouse/{0}/t3/".format(DDL_TEST_DB))
    self.filesystem_client.delete_file_dir("test-warehouse/{0}/t3/".format(DDL_TEST_DB),
        recursive=True)
    assert not self.filesystem_client.exists("test-warehouse/{0}/t3/".format(DDL_TEST_DB))

  @SkipIfLocal.hdfs_client
  @pytest.mark.execute_serially
  def test_truncate_cleans_hdfs_files(self):
    TRUNCATE_DB = "truncate_table_test_db"
    self.filesystem_client.delete_file_dir("test-warehouse/%s.db/" % TRUNCATE_DB,
        recursive=True)
    assert not self.filesystem_client.exists("test-warehouse/%s.db/" % TRUNCATE_DB)

    self._create_db(TRUNCATE_DB, sync=True)
    # Verify the db directory exists
    assert self.filesystem_client.exists("test-warehouse/%s.db/" % TRUNCATE_DB)

    self.client.execute("create table %s.t1(i int)" % TRUNCATE_DB)
    # Verify the table directory exists
    assert self.filesystem_client.exists("test-warehouse/truncate_table_test_db.db/t1/")

    try:
      # If we're testing S3, we want the staging directory to be created.
      self.client.execute("set s3_skip_insert_staging=false")
      # Should have created one file in the table's dir
      self.client.execute("insert into %s.t1 values (1)" % TRUNCATE_DB)
      assert len(self.filesystem_client.ls("test-warehouse/%s.db/t1/" % TRUNCATE_DB)) == 2

      # Truncating the table removes the data files and preserves the table's directory
      self.client.execute("truncate table %s.t1" % TRUNCATE_DB)
      assert len(self.filesystem_client.ls("test-warehouse/%s.db/t1/" % TRUNCATE_DB)) == 1

      self.client.execute(
          "create table %s.t2(i int) partitioned by (p int)" % TRUNCATE_DB)
      # Verify the table directory exists
      assert self.filesystem_client.exists("test-warehouse/%s.db/t2/" % TRUNCATE_DB)

      # Should have created the partition dir, which should contain exactly one file
      self.client.execute(
          "insert into %s.t2 partition(p=1) values (1)" % TRUNCATE_DB)
      assert len(self.filesystem_client.ls(
          "test-warehouse/%s.db/t2/p=1" % TRUNCATE_DB)) == 1

      # Truncating the table removes the data files and preserves the partition's directory
      self.client.execute("truncate table %s.t2" % TRUNCATE_DB)
      assert self.filesystem_client.exists("test-warehouse/%s.db/t2/p=1" % TRUNCATE_DB)
      assert len(self.filesystem_client.ls(
          "test-warehouse/%s.db/t2/p=1" % TRUNCATE_DB)) == 0
    finally:
      # Reset to its default value.
      self.client.execute("set s3_skip_insert_staging=true")

  @pytest.mark.execute_serially
  def test_truncate_table(self, vector):
    vector.get_value('exec_option')['abort_on_error'] = False
    self._create_db('truncate_table_test_db', sync=True)
    self.run_test_case('QueryTest/truncate-table', vector,
        use_db='truncate_table_test_db',
        multiple_impalad=self._use_multiple_impalad(vector))

  @pytest.mark.execute_serially
  def test_create_database(self, vector):
    self.run_test_case('QueryTest/create-database', vector,
        multiple_impalad=self._use_multiple_impalad(vector))

  # There is a query in QueryTest/create-table that references nested types, which is not
  # supported if old joins and aggs are enabled. Since we do not get any meaningful
  # additional coverage by running a DDL test under the old aggs and joins, it can be
  # skipped.
  @SkipIfOldAggsJoins.nested_types
  @pytest.mark.execute_serially
  def test_create_table(self, vector):
    vector.get_value('exec_option')['abort_on_error'] = False
    test_db_name = 'ddl_test_db'
    self._create_db(test_db_name, sync=True)
    self.run_test_case('QueryTest/create-table', vector, use_db=test_db_name,
        multiple_impalad=self._use_multiple_impalad(vector))

  @pytest.mark.execute_serially
  def test_create_table_like_table(self, vector):
    vector.get_value('exec_option')['abort_on_error'] = False
    test_db_name = 'ddl_test_db'
    self._create_db(test_db_name, sync=True)
    self.run_test_case('QueryTest/create-table-like-table', vector, use_db=test_db_name,
        multiple_impalad=self._use_multiple_impalad(vector))

  @pytest.mark.execute_serially
  def test_create_table_like_file(self, vector):
    vector.get_value('exec_option')['abort_on_error'] = False
    test_db_name = 'ddl_test_db'
    self._create_db(test_db_name, sync=True)
    self.run_test_case('QueryTest/create-table-like-file', vector, use_db=test_db_name,
        multiple_impalad=self._use_multiple_impalad(vector))

  @pytest.mark.execute_serially
  def test_create_table_as_select(self, vector):
    vector.get_value('exec_option')['abort_on_error'] = False
    test_db_name = 'ddl_test_db'
    self._create_db(test_db_name, sync=True)
    self.run_test_case('QueryTest/create-table-as-select', vector, use_db=test_db_name,
        multiple_impalad=self._use_multiple_impalad(vector))

  @SkipIf.kudu_not_supported
  @pytest.mark.execute_serially
  # TODO: Move this and other Kudu-related DDL tests into a separate py test
  # under test_ddl_base.py.
  def test_create_kudu(self, vector):
    self.expected_exceptions = 2
    vector.get_value('exec_option')['abort_on_error'] = False
    self._create_db('ddl_test_db', sync=True)
    self.run_test_case('QueryTest/create_kudu', vector, use_db='ddl_test_db',
        multiple_impalad=self._use_multiple_impalad(vector))

  @pytest.mark.execute_serially
  def test_sync_ddl_drop(self, vector):
    """Verifies the catalog gets updated properly when dropping objects with sync_ddl
    enabled"""
    self._create_db('ddl_test_db')
    self.client.set_configuration({'sync_ddl': 1})
    # Drop the database immediately after creation (within a statestore heartbeat) and
    # verify the catalog gets updated properly.
    self.client.execute('drop database ddl_test_db')
    assert 'ddl_test_db' not in self.all_db_names()

  # TODO: don't use hdfs_client
  @SkipIfLocal.hdfs_client
  @pytest.mark.execute_serially
  def test_alter_table(self, vector):
    vector.get_value('exec_option')['abort_on_error'] = False
    self.__test_alter_table_cleanup()
    # Create directory for partition data that does not use the (key=value)
    # format.
    self.filesystem_client.make_dir("test-warehouse/part_data/", permission=777)
    self.filesystem_client.create_file(
        "test-warehouse/part_data/data.txt", file_data='1984')

    try:
      # Create test databases
      self._create_db('alter_table_test_db', sync=True)
      self._create_db('alter_table_test_db2', sync=True)
      self.run_test_case('QueryTest/alter-table', vector, use_db='alter_table_test_db',
          multiple_impalad=self._use_multiple_impalad(vector))
    finally:
      self.__test_alter_table_cleanup()

  def __test_alter_table_cleanup(self):
    if IS_LOCAL: return
    # Cleanup the test table HDFS dirs between test runs so there are no errors the next
    # time a table is created with the same location. This also helps remove any stale
    # data from the last test run.
    for dir_ in ['part_data', 't1_tmp1', 't_part_tmp']:
      self.filesystem_client.delete_file_dir('test-warehouse/%s' % dir_, recursive=True)

  @pytest.mark.execute_serially
  def test_alter_set_column_stats(self, vector):
    self._create_db('alter_table_test_db', sync=True)
    self.run_test_case('QueryTest/alter-table-set-column-stats',
        vector, use_db='alter_table_test_db',
        multiple_impalad=self._use_multiple_impalad(vector))

  @SkipIfLocal.hdfs_client
  @pytest.mark.execute_serially
  def test_drop_partition_with_purge(self, vector):
    """Verfies whether alter <tbl> drop partition purge actually skips trash"""
    ALTER_PURGE_DB = "alter_purge_db"
    # Create a sample database alter_purge_db and table t1 in it
    self._create_db(ALTER_PURGE_DB)
    self.client.execute("create table {0}.t1(i int) partitioned\
        by (j int)".format(ALTER_PURGE_DB))
    # Add two partitions (j=1) and (j=2) to table t1
    self.client.execute("alter table {0}.t1 add partition(j=1)".format(ALTER_PURGE_DB))
    self.client.execute("alter table {0}.t1 add partition(j=2)".format(ALTER_PURGE_DB))
    self.filesystem_client.create_file(\
        "test-warehouse/{0}.db/t1/j=1/j1.txt".format(ALTER_PURGE_DB), file_data='j1')
    self.filesystem_client.create_file(\
        "test-warehouse/{0}.db/t1/j=2/j2.txt".format(ALTER_PURGE_DB), file_data='j2')
    # Drop the partition (j=1) without purge and make sure it exists in trash
    self.client.execute("alter table {0}.t1 drop partition(j=1)".format(ALTER_PURGE_DB));
    assert not self.filesystem_client.exists("test-warehouse/{0}.db/t1/j=1/j1.txt".\
        format(ALTER_PURGE_DB))
    assert not self.filesystem_client.exists("test-warehouse/{0}.db/t1/j=1".\
        format(ALTER_PURGE_DB))
    assert self.filesystem_client.exists(\
        "user/{0}/.Trash/Current/test-warehouse/{1}.db/t1/j=1/j1.txt".\
        format(getpass.getuser(), ALTER_PURGE_DB))
    assert self.filesystem_client.exists(\
        "user/{0}/.Trash/Current/test-warehouse/{1}.db/t1/j=1".\
        format(getpass.getuser(), ALTER_PURGE_DB))
    # Drop the partition (with purge) and make sure it doesn't exist in trash
    self.client.execute("alter table {0}.t1 drop partition(j=2) purge".\
        format(ALTER_PURGE_DB));
    if not IS_S3:
      # In S3, deletes are eventual. So even though we dropped the partition, the files
      # belonging to this partition will still be visible for some unbounded time. This
      # happens only with PURGE. A regular DROP TABLE is just a copy of files which is
      # consistent.
      assert not self.filesystem_client.exists("test-warehouse/{0}.db/t1/j=2/j2.txt".\
          format(ALTER_PURGE_DB))
      assert not self.filesystem_client.exists("test-warehouse/{0}.db/t1/j=2".\
          format(ALTER_PURGE_DB))
    assert not self.filesystem_client.exists(\
        "user/{0}/.Trash/Current/test-warehouse/{1}.db/t1/j=2".\
        format(getpass.getuser(), ALTER_PURGE_DB))
    assert not self.filesystem_client.exists(
        "user/{0}/.Trash/Current/test-warehouse/{1}.db/t1/j=2/j2.txt".\
        format(getpass.getuser(), ALTER_PURGE_DB))

  @pytest.mark.execute_serially
  def test_views_ddl(self, vector):
    vector.get_value('exec_option')['abort_on_error'] = False
    self._create_db('ddl_test_db', sync=True)
    self.run_test_case('QueryTest/views-ddl', vector, use_db='ddl_test_db',
        multiple_impalad=self._use_multiple_impalad(vector))

  @pytest.mark.execute_serially
  def test_functions_ddl(self, vector):
    self._create_db('function_ddl_test', sync=True)
    self.run_test_case('QueryTest/functions-ddl', vector, use_db='function_ddl_test',
        multiple_impalad=self._use_multiple_impalad(vector))

  @pytest.mark.execute_serially
  def test_create_drop_function(self, vector):
    """This will create, run, and drop the same function repeatedly, exercising the
    lib cache mechanism.
    """
    create_fn_stmt = ("create function f() returns int "
                      "location '%s/libTestUdfs.so' symbol='NoArgs'" % WAREHOUSE)
    select_stmt = "select f() from functional.alltypes limit 10"
    drop_fn_stmt = "drop function %s f()"
    self.create_drop_ddl(vector, "udf_test", [create_fn_stmt], [drop_fn_stmt],
        select_stmt)

  @pytest.mark.execute_serially
  def test_create_drop_data_src(self, vector):
    """This will create, run, and drop the same data source repeatedly, exercising
    the lib cache mechanism.
    """
    create_ds_stmt = ("CREATE DATA SOURCE test_data_src "
        "LOCATION '%s/data-sources/test-data-source.jar' "
        "CLASS 'com.cloudera.impala.extdatasource.AllTypesDataSource' "
        "API_VERSION 'V1'" % WAREHOUSE)
    create_tbl_stmt = """CREATE TABLE data_src_tbl (x int)
        PRODUCED BY DATA SOURCE test_data_src('dummy_init_string')"""
    drop_ds_stmt = "drop data source %s test_data_src"
    drop_tbl_stmt = "drop table %s data_src_tbl"
    select_stmt = "select * from data_src_tbl limit 1"
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
    self.create_drop_ddl(vector, "data_src_test", create_stmts, drop_stmts,
        select_stmt, num_iterations)

    # Check class cache metrics. Shouldn't have any new cache hits, there should be
    # 2 cache misses for every iteration (jar is loaded by both the FE and BE).
    expected_cache_misses = class_cache_misses + (num_iterations * 2)
    service.wait_for_metric_value(class_cache_hits_metric, class_cache_hits)
    service.wait_for_metric_value(class_cache_misses_metric,
        expected_cache_misses)

    # Test with a table that caches the class
    create_tbl_stmt = """CREATE TABLE data_src_tbl (x int)
        PRODUCED BY DATA SOURCE test_data_src('CACHE_CLASS::dummy_init_string')"""
    create_stmts = [create_ds_stmt, create_tbl_stmt]
    # Run once before capturing metrics because the class already may be cached from
    # a previous test run.
    # TODO: Provide a way to clear the cache
    self.create_drop_ddl(vector, "data_src_test", create_stmts, drop_stmts,
        select_stmt, 1)

    # Capture metric values and run again, should hit the cache.
    class_cache_hits = service.get_metric_value(class_cache_hits_metric)
    class_cache_misses = service.get_metric_value(class_cache_misses_metric)
    self.create_drop_ddl(vector, "data_src_test", create_stmts, drop_stmts,
        select_stmt, 1)
    service.wait_for_metric_value(class_cache_hits_metric, class_cache_hits + 2)
    service.wait_for_metric_value(class_cache_misses_metric, class_cache_misses)

  @SkipIfLocal.hdfs_client
  @pytest.mark.execute_serially
  def test_create_alter_bulk_partition(self, vector):
    TBL_NAME = 'foo_part'
    # Change the scale depending on the exploration strategy, with 50 partitions this
    # takes a few minutes to run, with 10 partitions it takes ~50s for two configurations.
    num_parts = 50 if self.exploration_strategy() == 'exhaustive' else 10
    self.client.execute("use default")
    self.client.execute("drop table if exists {0}".format(TBL_NAME))
    self.client.execute("""create table {0}(i int) partitioned by(j int, s string)
         location '{1}/{0}'""".format(TBL_NAME, WAREHOUSE))

    # Add some partitions (first batch of two)
    for i in xrange(num_parts / 5):
      start = time.time()
      self.client.execute("alter table {0} add partition(j={1}, s='{1}')".format(TBL_NAME,
                                                                                 i))
      LOG.info('ADD PARTITION #%d exec time: %s' % (i, time.time() - start))

    # Modify one of the partitions
    self.client.execute("alter table %s partition(j=1, s='1')"
        " set fileformat parquetfile" % TBL_NAME)

    # Alter one partition to a non-existent location twice (IMPALA-741)
    self.filesystem_client.delete_file_dir("tmp/dont_exist1/", recursive=True)
    self.filesystem_client.delete_file_dir("tmp/dont_exist2/", recursive=True)

    self.execute_query_expect_success(self.client,
        "alter table {0} partition(j=1,s='1') set location '{1}/tmp/dont_exist1'"
        .format(TBL_NAME, WAREHOUSE))
    self.execute_query_expect_success(self.client,
        "alter table {0} partition(j=1,s='1') set location '{1}/tmp/dont_exist2'"
        .format(TBL_NAME, WAREHOUSE))

    # Add some more partitions
    for i in xrange(num_parts / 5, num_parts):
      start = time.time()
      self.client.execute("alter table {0} add partition(j={1},s='{1}')".format(TBL_NAME,
                                                                                i))
      LOG.info('ADD PARTITION #%d exec time: %s' % (i, time.time() - start))

    # Insert data and verify it shows up.
    self.client.execute("insert into table {0} partition(j=1, s='1') select 1"
      .format(TBL_NAME))
    assert '1' == self.execute_scalar("select count(*) from {0}".format(TBL_NAME))
    self.client.execute("drop table {0}".format(TBL_NAME))

  @pytest.mark.execute_serially
  def test_create_alter_tbl_properties(self, vector):
    self._create_db('alter_table_test_db', sync=True)
    self.client.execute("use alter_table_test_db")

    # Specify TBLPROPERTIES and SERDEPROPERTIES at CREATE time
    self.client.execute("""create table test_alter_tbl (i int)
    with serdeproperties ('s1'='s2', 's3'='s4')
    tblproperties ('p1'='v0', 'p1'='v1')""")
    properties = self._get_tbl_properties('test_alter_tbl')

    assert len(properties) == 2
    # The transient_lastDdlTime is variable, so don't verify the value.
    assert 'transient_lastDdlTime' in properties
    del properties['transient_lastDdlTime']
    assert {'p1': 'v1'} == properties

    properties = self._get_serde_properties('test_alter_tbl')
    assert {'s1': 's2', 's3': 's4'} == properties

    # Modify the SERDEPROPERTIES using ALTER TABLE SET.
    self.client.execute("alter table test_alter_tbl set serdeproperties "\
        "('s1'='new', 's5'='s6')")
    properties = self._get_serde_properties('test_alter_tbl')
    assert {'s1': 'new', 's3': 's4', 's5': 's6'} == properties

    # Modify the TBLPROPERTIES using ALTER TABLE SET.
    self.client.execute("alter table test_alter_tbl set tblproperties "\
        "('prop1'='val1', 'p2'='val2', 'p2'='val3', ''='')")
    properties = self._get_tbl_properties('test_alter_tbl')

    assert 'transient_lastDdlTime' in properties
    assert properties['p1'] == 'v1'
    assert properties['prop1'] == 'val1'
    assert properties['p2'] == 'val3'
    assert properties[''] == ''
