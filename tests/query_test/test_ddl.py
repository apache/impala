#!/usr/bin/env python
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
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

# Validates DDL statements (create, drop)
class TestDdlStatements(ImpalaTestSuite):
  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestDdlStatements, cls).add_test_dimensions()
    cls.TestMatrix.add_dimension(create_single_exec_option_dimension())

    # There is no reason to run these tests using all dimensions.
    cls.TestMatrix.add_constraint(lambda v:\
        v.get_value('table_format').file_format == 'text' and\
        v.get_value('table_format').compression_codec == 'none')

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
    map(self.cleanup_db, ['ddl_test_db', 'alter_table_test_db', 'alter_table_test_db2'])
    self.cleanup_hdfs_dirs()
    self.client.refresh()

  def cleanup_hdfs_dirs(self):
    # Cleanup the test table HDFS dirs between test runs so there are no errors the next
    # time a table is created with the same location. This also helps remove any stale
    # data from the last test run.
    self.hdfs_client.delete_file_dir("test-warehouse/part_data/", recursive=True)
    self.hdfs_client.delete_file_dir("test-warehouse/t1_tmp1/", recursive=True)
    self.hdfs_client.delete_file_dir("test-warehouse/t_part_tmp/", recursive=True)

  @pytest.mark.execute_serially
  def test_create(self, vector):
    vector.get_value('exec_option')['abort_on_error'] = False
    self.run_test_case('QueryTest/create', vector)

  @pytest.mark.execute_serially
  def test_alter_table(self, vector):
    vector.get_value('exec_option')['abort_on_error'] = False
    # Create directory for partition data that does not use the (key=value)
    # format.
    self.hdfs_client.make_dir("test-warehouse/part_data/", permission=777)
    self.hdfs_client.create_file("test-warehouse/part_data/data.txt", file_data='1984')
    self.run_test_case('QueryTest/alter-table', vector)

  @pytest.mark.execute_serially
  def test_views_ddl(self, vector):
    vector.get_value('exec_option')['abort_on_error'] = False
    self.run_test_case('QueryTest/views-ddl', vector)

  @pytest.mark.execute_serially
  def test_functions_ddl(self, vector):
    self.run_test_case('QueryTest/functions-ddl', vector)

  @pytest.mark.execute_serially
  def test_create_drop_function(self, vector):
    # This will create and drop the same function repeatedly, exercising the
    # lib cache mechanism.
    # TODO: it's hard to tell that the cache is working (i.e. if it did
    # nothing to drop the cache, these tests would still pass). Testing
    # that is a bit harder and requires us to update the udf binary in
    # the middle.
    create_fn_stmt = """create function f() returns int
        location '/test-warehouse/libTestUdfs.so' symbol='NoArgs'""";
    drop_fn_stmt = "drop function f()"

    self.client.execute("create database if not exists udf_test")
    self.client.execute("use udf_test")
    self.client.execute("drop function if exists f()")

    for i in xrange(1, 5):
      self.client.execute(create_fn_stmt)
      self.client.execute(drop_fn_stmt)

  @pytest.mark.execute_serially
  def test_create_alter_bulk_partition(self, vector):
    # Only run during exhaustive exploration strategy, this doesn't add a lot of extra
    # coverage to the existing test cases and takes a couple minutes to execute.
    if self.exploration_strategy() != 'exhaustive': pytest.skip()

    self.client.execute("use default")
    self.client.execute("drop table if exists foo_part")
    self.client.execute("create table foo_part(i int) partitioned by(j int, s string)")

    # Add some partitions
    for i in xrange(10):
      start = time.time()
      self.client.execute("alter table foo_part add partition(j=%d, s='%s')" % (i, i))
      print 'ADD PARTITION #%d exec time: %s' % (i, time.time() - start)

    # Modify one of the partitions
    self.client.execute("""alter table foo_part partition(j=5, s='5')
        set fileformat parquetfile""")

    # Add some more partitions
    for i in xrange(10, 50):
      start = time.time()
      self.client.execute("alter table foo_part add partition(j=%d,s='%s')" % (i,i))
      print 'ADD PARTITION #%d exec time: %s' % (i, time.time() - start)

    # Insert data and verify it shows up.
    self.client.execute("insert into table foo_part partition(j=5, s='5') select 1")
    assert '1' == self.execute_scalar("select count(*) from foo_part")
    self.client.execute("drop table foo_part")

  @pytest.mark.execute_serially
  def test_create_alter_tbl_properties(self, vector):
    self.client.execute("use default")
    self.client.execute("drop table if exists test_alter_tbl")

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

    assert len(properties) == 5
    assert 'transient_lastDdlTime' in properties
    del properties['transient_lastDdlTime']
    assert {'p1': 'v1', 'prop1': 'val1', 'p2': 'val3', '': ''} == properties

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
