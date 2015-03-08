#!/usr/bin/env python
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
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
# Impala tests for DDL statements
import logging
import pytest
import shlex
import time
from tests.common.test_result_verifier import *
from subprocess import call
from tests.common.test_vector import *
from tests.common.test_dimensions import ALL_NODES_ONLY
from tests.common.impala_test_suite import *
from tests.common.skip import SkipIfS3
from tests.util.filesystem_utils import WAREHOUSE, IS_DEFAULT_FS

# Validates DDL statements (create, drop)
class TestDdlStatements(ImpalaTestSuite):
  TEST_DBS = ['ddl_test_db', 'alter_table_test_db', 'alter_table_test_db2',
              'function_ddl_test', 'udf_test', 'data_src_test', 'truncate_table_test_db']

  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestDdlStatements, cls).add_test_dimensions()
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

  def setup_method(self, method):
    self.cleanup()

    # Get the current number of queries that are in the 'EXCEPTION' state. Used for
    # verification after running each test case.
    self.start_exception_count = self.query_exception_count()
    self.cleanup_hdfs_dirs()

  def teardown_method(self, method):
    end_exception_count = self.query_exception_count()
    # The number of exceptions may be < than what was in setup if the queries in the
    # EXCEPTION state were bumped out of the FINISHED list. We should never see an
    # increase in the number of queries in the exception state.
    assert end_exception_count <= self.start_exception_count

  def query_exception_count(self):
    """Returns the number of occurrences of 'EXCEPTION' on the debug /queries page"""
    return len(re.findall('EXCEPTION',
        self.impalad_test_service.read_debug_webpage('queries')))

  def cleanup(self):
    map(self.cleanup_db, self.TEST_DBS)
    self.cleanup_hdfs_dirs()

  def cleanup_hdfs_dirs(self):
    # Cleanup the test table HDFS dirs between test runs so there are no errors the next
    # time a table is created with the same location. This also helps remove any stale
    # data from the last test run.
    self.hdfs_client.delete_file_dir("test-warehouse/part_data/", recursive=True)
    self.hdfs_client.delete_file_dir("test-warehouse/t1_tmp1/", recursive=True)
    self.hdfs_client.delete_file_dir("test-warehouse/t_part_tmp/", recursive=True)

  @SkipIfS3.hdfs_client # S3: missing coverage: drop table/database
  @pytest.mark.execute_serially
  def test_drop_cleans_hdfs_dirs(self):
    self.hdfs_client.delete_file_dir("test-warehouse/ddl_test_db.db/", recursive=True)
    assert not self.hdfs_client.exists("test-warehouse/ddl_test_db.db/")

    self.client.execute('use default')
    self.client.execute('create database ddl_test_db')
    # Verify the db directory exists
    assert self.hdfs_client.exists("test-warehouse/ddl_test_db.db/")

    self.client.execute("create table ddl_test_db.t1(i int)")
    # Verify the table directory exists
    assert self.hdfs_client.exists("test-warehouse/ddl_test_db.db/t1/")

    # Dropping the table removes the table's directory and preserves the db's directory
    self.client.execute("drop table ddl_test_db.t1")
    assert not self.hdfs_client.exists("test-warehouse/ddl_test_db.db/t1/")
    assert self.hdfs_client.exists("test-warehouse/ddl_test_db.db/")

    # Dropping the db removes the db's directory
    self.client.execute("drop database ddl_test_db")
    assert not self.hdfs_client.exists("test-warehouse/ddl_test_db.db/")

  @SkipIfS3.insert # S3: missing coverage: truncate table
  @pytest.mark.execute_serially
  def test_truncate_cleans_hdfs_files(self):
    self.hdfs_client.delete_file_dir("test-warehouse/truncate_table_test_db.db/",
        recursive=True)
    assert not self.hdfs_client.exists("test-warehouse/truncate_table_test_db.db/")

    self.client.execute('create database truncate_table_test_db')
    # Verify the db directory exists
    assert self.hdfs_client.exists("test-warehouse/truncate_table_test_db.db/")

    self.client.execute("create table truncate_table_test_db.t1(i int)")
    # Verify the table directory exists
    assert self.hdfs_client.exists("test-warehouse/truncate_table_test_db.db/t1/")

    # Should have created one file in the table's dir
    self.client.execute("insert into truncate_table_test_db.t1 values (1)")
    ls = self.hdfs_client.list_dir("test-warehouse/truncate_table_test_db.db/t1/")
    assert len(ls['FileStatuses']['FileStatus']) == 2

    # Truncating the table removes the data files and preserves the table's directory
    self.client.execute("truncate table truncate_table_test_db.t1")
    ls = self.hdfs_client.list_dir("test-warehouse/truncate_table_test_db.db/t1/")
    assert len(ls['FileStatuses']['FileStatus']) == 1

    self.client.execute(
        "create table truncate_table_test_db.t2(i int) partitioned by (p int)")
    # Verify the table directory exists
    assert self.hdfs_client.exists("test-warehouse/truncate_table_test_db.db/t2/")

    # Should have created the partition dir, which should contain exactly one file
    self.client.execute(
        "insert into truncate_table_test_db.t2 partition(p=1) values (1)")
    ls = self.hdfs_client.list_dir("test-warehouse/truncate_table_test_db.db/t2/p=1")
    assert len(ls['FileStatuses']['FileStatus']) == 1

    # Truncating the table removes the data files and preserves the partition's directory
    self.client.execute("truncate table truncate_table_test_db.t2")
    assert self.hdfs_client.exists("test-warehouse/truncate_table_test_db.db/t2/p=1")
    ls = self.hdfs_client.list_dir("test-warehouse/truncate_table_test_db.db/t2/p=1")
    assert len(ls['FileStatuses']['FileStatus']) == 0

  @SkipIfS3.insert # S3: missing coverage: truncate table
  @pytest.mark.execute_serially
  def test_truncate_table(self, vector):
    vector.get_value('exec_option')['abort_on_error'] = False
    self.__create_db_synced('truncate_table_test_db', vector)
    self.run_test_case('QueryTest/truncate-table', vector, use_db='truncate_table_test_db',
        multiple_impalad=self.__use_multiple_impalad(vector))

  @SkipIfS3.insert
  @pytest.mark.execute_serially
  def test_create(self, vector):
    vector.get_value('exec_option')['abort_on_error'] = False
    self.__create_db_synced('ddl_test_db', vector)
    self.run_test_case('QueryTest/create', vector, use_db='ddl_test_db',
        multiple_impalad=self.__use_multiple_impalad(vector))

  @pytest.mark.execute_serially
  def test_sync_ddl_drop(self, vector):
    """Verifies the catalog gets updated properly when dropping objects with sync_ddl
    enabled"""
    self.client.set_configuration({'sync_ddl': 0})
    if IS_DEFAULT_FS:
      self.client.execute('create database ddl_test_db')
    else:
      self.client.execute("create database ddl_test_db location "
                          "'%s/ddl_test_db.db'" % WAREHOUSE)

    self.client.set_configuration({'sync_ddl': 1})
    # Drop the database immediately after creation (within a statestore heartbeat) and
    # verify the catalog gets updated properly.
    self.client.execute('drop database ddl_test_db')
    assert 'ddl_test_db' not in self.client.execute("show databases").data

  # TODO: don't use hdfs_client
  @SkipIfS3.insert # S3: missing coverage: alter table
  @pytest.mark.execute_serially
  def test_alter_table(self, vector):
    vector.get_value('exec_option')['abort_on_error'] = False
    # Create directory for partition data that does not use the (key=value)
    # format.
    self.hdfs_client.make_dir("test-warehouse/part_data/", permission=777)
    self.hdfs_client.create_file("test-warehouse/part_data/data.txt", file_data='1984')

    # Create test databases
    self.__create_db_synced('alter_table_test_db', vector)
    self.__create_db_synced('alter_table_test_db2', vector)
    self.run_test_case('QueryTest/alter-table', vector, use_db='alter_table_test_db',
        multiple_impalad=self.__use_multiple_impalad(vector))

  @pytest.mark.execute_serially
  def test_views_ddl(self, vector):
    vector.get_value('exec_option')['abort_on_error'] = False
    self.__create_db_synced('ddl_test_db', vector)
    self.run_test_case('QueryTest/views-ddl', vector, use_db='ddl_test_db',
        multiple_impalad=self.__use_multiple_impalad(vector))

  @pytest.mark.execute_serially
  def test_functions_ddl(self, vector):
    self.__create_db_synced('function_ddl_test', vector)
    self.run_test_case('QueryTest/functions-ddl', vector, use_db='function_ddl_test',
        multiple_impalad=self.__use_multiple_impalad(vector))

  @pytest.mark.execute_serially
  def test_create_drop_function(self, vector):
    # This will create, run, and drop the same function repeatedly, exercising the
    # lib cache mechanism.
    create_fn_stmt = ("create function f() returns int "
                      "location '%s/libTestUdfs.so' symbol='NoArgs'" % WAREHOUSE)
    select_stmt = "select f() from functional.alltypes limit 10"
    drop_fn_stmt = "drop function %s f()"
    self.create_drop_ddl(vector, "udf_test", [create_fn_stmt], [drop_fn_stmt],
        select_stmt)

  @pytest.mark.execute_serially
  def test_create_drop_data_src(self, vector):
    # This will create, run, and drop the same data source repeatedly, exercising
    # the lib cache mechanism.
    create_ds_stmt = ("CREATE DATA SOURCE test_data_src "
        "LOCATION '%s/data-sources/test-data-source.jar' "
        "CLASS 'com.cloudera.impala.extdatasource.AllTypesDataSource' "
        "API_VERSION 'V1'" % WAREHOUSE)
    create_tbl_stmt = """CREATE TABLE data_src_tbl (x int)
        PRODUCED BY DATA SOURCE test_data_src"""
    drop_ds_stmt = "drop data source %s test_data_src"
    drop_tbl_stmt = "drop table %s data_src_tbl"
    select_stmt = "select * from data_src_tbl limit 1"

    create_stmts = [create_ds_stmt, create_tbl_stmt]
    drop_stmts = [drop_tbl_stmt, drop_ds_stmt]
    self.create_drop_ddl(vector, "data_src_test", create_stmts, drop_stmts,
        select_stmt)

  def create_drop_ddl(self, vector, db_name, create_stmts, drop_stmts, select_stmt):
    # Helper method to run CREATE/DROP DDL commands repeatedly and exercise the lib cache
    # create_stmts is the list of CREATE statements to be executed in order drop_stmts is
    # the list of DROP statements to be executed in order. Each statement should have a
    # '%s' placeholder to insert "IF EXISTS" or "". The select_stmt is just a single
    # statement to test after executing the CREATE statements.
    # TODO: it's hard to tell that the cache is working (i.e. if it did nothing to drop
    # the cache, these tests would still pass). Testing that is a bit harder and requires
    # us to update the udf binary in the middle.
    self.__create_db_synced(db_name, vector)
    self.client.set_configuration(vector.get_value('exec_option'))

    self.client.execute("use %s" % (db_name,))
    for drop_stmt in drop_stmts: self.client.execute(drop_stmt % ("if exists"))
    for i in xrange(1, 10):
      for create_stmt in create_stmts: self.client.execute(create_stmt)
      self.client.execute(select_stmt)
      for drop_stmt in drop_stmts: self.client.execute(drop_stmt % (""))

  @pytest.mark.execute_serially
  def test_create_alter_bulk_partition(self, vector):
    # Change the scale depending on the exploration strategy, with 50 partitions this
    # takes a few minutes to run, with 10 partitions it takes ~50s for two configurations.
    num_parts = 50
    if self.exploration_strategy() != 'exhaustive': num_parts = 10

    self.client.execute("use default")
    self.client.execute("drop table if exists foo_part")
    self.client.execute("create table foo_part(i int) partitioned by(j int, s string)")

    # Add some partitions (first batch of two)
    for i in xrange(num_parts / 5):
      start = time.time()
      self.client.execute("alter table foo_part add partition(j=%d, s='%s')" % (i, i))
      print 'ADD PARTITION #%d exec time: %s' % (i, time.time() - start)

    # Modify one of the partitions
    self.client.execute("""alter table foo_part partition(j=1, s='1')
        set fileformat parquetfile""")

    # Alter one partition to a non-existent location twice (IMPALA-741)
    self.hdfs_client.delete_file_dir("tmp/dont_exist1/", recursive=True)
    self.hdfs_client.delete_file_dir("tmp/dont_exist2/", recursive=True)

    self.execute_query_expect_success(self.client,
        "alter table foo_part partition(j=1,s='1') set location '/tmp/dont_exist1'")
    self.execute_query_expect_success(self.client,
        "alter table foo_part partition(j=1,s='1') set location '/tmp/dont_exist2'")

    # Add some more partitions
    for i in xrange(num_parts / 5, num_parts):
      start = time.time()
      self.client.execute("alter table foo_part add partition(j=%d,s='%s')" % (i,i))
      print 'ADD PARTITION #%d exec time: %s' % (i, time.time() - start)

    # Insert data and verify it shows up.
    self.client.execute("insert into table foo_part partition(j=1, s='1') select 1")
    assert '1' == self.execute_scalar("select count(*) from foo_part")
    self.client.execute("drop table foo_part")

  @pytest.mark.execute_serially
  def test_create_alter_tbl_properties(self, vector):
    self.__create_db_synced('alter_table_test_db', vector)
    self.client.execute("use alter_table_test_db")

    # Specify TBLPROPERTIES and SERDEPROPERTIES at CREATE time
    self.client.execute("""create table test_alter_tbl (i int)
    with serdeproperties ('s1'='s2', 's3'='s4')
    tblproperties ('p1'='v0', 'p1'='v1')""")
    properties = self.__get_tbl_properties('test_alter_tbl')

    assert len(properties) == 2
    # The transient_lastDdlTime is variable, so don't verify the value.
    assert 'transient_lastDdlTime' in properties
    del properties['transient_lastDdlTime']
    assert {'p1': 'v1'} == properties

    properties = self.__get_serde_properties('test_alter_tbl')
    assert {'s1': 's2', 's3': 's4'} == properties

    # Modify the SERDEPROPERTIES using ALTER TABLE SET.
    self.client.execute("alter table test_alter_tbl set serdeproperties "\
        "('s1'='new', 's5'='s6')")
    properties = self.__get_serde_properties('test_alter_tbl')
    assert {'s1': 'new', 's3': 's4', 's5': 's6'} == properties

    # Modify the TBLPROPERTIES using ALTER TABLE SET.
    self.client.execute("alter table test_alter_tbl set tblproperties "\
        "('prop1'='val1', 'p2'='val2', 'p2'='val3', ''='')")
    properties = self.__get_tbl_properties('test_alter_tbl')

    assert 'transient_lastDdlTime' in properties
    assert properties['p1'] == 'v1'
    assert properties['prop1'] == 'val1'
    assert properties['p2'] == 'val3'
    assert properties[''] == ''

  @classmethod
  def __use_multiple_impalad(cls, vector):
    return vector.get_value('exec_option')['sync_ddl'] == 1

  @classmethod
  def __create_db_synced(cls, db_name, vector):
    """Creates a database using synchronized DDL to ensure all nodes have the test
    database available for use before executing the .test file(s).
    """
    cls.client.execute('use default')
    cls.client.set_configuration({'sync_ddl': 1})
    if IS_DEFAULT_FS:
      cls.client.execute("create database %s" % db_name)
    else:
      cls.client.execute("create database %s location "
                         "'%s/%s.db'" % (db_name, WAREHOUSE, db_name))
    cls.client.set_configuration(vector.get_value('exec_option'))

  def __get_tbl_properties(self, table_name):
    """Extracts the table properties mapping from the output of DESCRIBE FORMATTED"""
    return self.__get_properties('Table Parameters:', table_name)

  def __get_serde_properties(self, table_name):
    """Extracts the serde properties mapping from the output of DESCRIBE FORMATTED"""
    return self.__get_properties('Storage Desc Params:', table_name)

  def __get_properties(self, section_name, table_name):
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
