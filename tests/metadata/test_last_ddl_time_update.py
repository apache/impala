# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
# Impala tests for DDL statements
import logging
import pytest
import sys
import time
from subprocess import call
from tests.common.skip import SkipIfS3
from tests.common.test_vector import *
from tests.common.impala_test_suite import *
from tests.util.filesystem_utils import WAREHOUSE, IS_S3

DB_NAME = "metastore_update"
TABLE_NAME = "ddltime_test"
FULL_NAME = "%s.%s" % (DB_NAME, TABLE_NAME)
HIVE_LAST_DDL_TIME_PARAM_KEY = "transient_lastDdlTime"
TBL_LOC = "%s/t_part_tmp" % WAREHOUSE
DB_LOC = "%s/%s" % (WAREHOUSE, DB_NAME)

# Checks that ALTER and INSERT statements update the last DDL time of the modified table.
class TestLastDdlTimeUpdate(ImpalaTestSuite):

  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestLastDdlTimeUpdate, cls).add_test_dimensions()
    # There is no reason to run these tests using all dimensions.
    cls.TestMatrix.add_constraint(lambda v:\
        v.get_value('table_format').file_format == 'text' and\
        v.get_value('table_format').compression_codec == 'none')

    if cls.exploration_strategy() == 'core' and not IS_S3:
      # Don't run on core.  This test is very slow and we are unlikely
      # to regress here.
      cls.TestMatrix.add_constraint(lambda v: False)

  def __cleanup(self):
    self.execute_query("drop table if exists %s" % FULL_NAME)
    self.execute_query("drop database if exists %s" % DB_NAME)
    call(["hadoop", "fs", "-rm", "-r", "-f", TBL_LOC], shell=False)

  def setup_method(self, method):
    self.__cleanup()
    self.execute_query("create database if not exists %s LOCATION '%s'" % (DB_NAME,
      DB_LOC))
    self.execute_query("create external table if not exists %s "
                       "(i int) partitioned by (j int, s string)"
                       % FULL_NAME)

  def teardown_method(self, method):
    self.__cleanup()

  @pytest.mark.execute_serially
  def test_alter(self, vector):
    # add/drop partitions
    self.run_test("alter table %s add partition (j=1, s='2012')" % FULL_NAME, True)
    self.run_test("alter table %s add if not exists "
                  "partition (j=1, s='2012')" % FULL_NAME, False)
    self.run_test("alter table %s drop partition (j=1, s='2012')" % FULL_NAME, True)
    self.run_test("alter table %s drop if exists "
                  "partition (j=2, s='2012')" % FULL_NAME, False)
    # rename columns
    self.run_test("alter table %s change column i k int" % FULL_NAME, True)
    self.run_test("alter table %s change column k i int" % FULL_NAME, True)
    # change location of table
    self.run_test("alter table %s set location "
                  "'%s'" % (FULL_NAME, TBL_LOC), True)
    # change format of table
    self.run_test("alter table %s set fileformat textfile" % FULL_NAME, True)

  @pytest.mark.execute_serially
  @SkipIfS3.insert
  def test_insert(self, vector):
    # static partition insert
    self.run_test("insert into %s partition(j=1, s='2012') "
                  "select 10" % FULL_NAME, True)
    # dynamic partition insert
    self.run_test("insert into %s partition(j, s) "
                  "select 10, 2, '2013'" % FULL_NAME, True)
    # dynamic partition insert changing no partitions (empty input)
    self.run_test("insert into %s partition(j, s) "
                  "select * from (select 10 as i, 2 as j, '2013' as s) as t "
                  "where t.i < 10" % FULL_NAME, False)
    # dynamic partition insert modifying an existing partition
    self.run_test("insert into %s partition(j, s) "
                  "select 20, 1, '2012'" % FULL_NAME, False)

  def run_test(self, query, expect_changed):
    """
    Runs the given query (expected to be an ALTER or INSERT statement)
    and compares the last ddl time before and after executing the query.
    If expect_change is true then we expect the last ddl time to increase,
    otherwise we expect no change.
    """

    # Get last DDL time before executing query.
    table = self.hive_client.get_table(DB_NAME, TABLE_NAME)
    assert table is not None
    beforeDdlTime = table.parameters[HIVE_LAST_DDL_TIME_PARAM_KEY]

    # Sleep for 2s to make sure the new ddlTime is strictly greater than the old one.
    # Hive uses a seconds granularity on the last ddl time.
    time.sleep (2)

    result = self.execute_query(query)

    # Get last ddl time after executing query .
    table = self.hive_client.get_table(DB_NAME, TABLE_NAME)
    afterDdlTime = table.parameters[HIVE_LAST_DDL_TIME_PARAM_KEY]

    if expect_changed:
      # check that the time difference is within 20s
      assert long(afterDdlTime) - long(beforeDdlTime) <= 20
    else:
      assert long(afterDdlTime) == long(beforeDdlTime)

