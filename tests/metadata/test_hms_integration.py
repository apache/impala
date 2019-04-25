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
#
# Impala tests for Hive Metastore, covering the expected propagation
# of metadata from Hive to Impala or Impala to Hive. Each test
# modifies the metadata via Hive and checks that the modification
# succeeded by querying Impala, or vice versa.
#
# TODO: For each test, verify all the metadata available via Hive and
# Impala, in all the possible ways of validating that metadata.


import pytest
import random
import string
from subprocess import call

from tests.common.environ import IMPALA_TEST_CLUSTER_PROPERTIES
from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.skip import SkipIfS3, SkipIfABFS, SkipIfADLS, SkipIfIsilon, SkipIfLocal
from tests.common.test_dimensions import (
    create_single_exec_option_dimension,
    create_uncompressed_text_dimension)
from tests.util.hive_utils import HiveDbWrapper, HiveTableWrapper

@SkipIfS3.hive
@SkipIfABFS.hive
@SkipIfADLS.hive
@SkipIfIsilon.hive
@SkipIfLocal.hive
class TestHmsIntegrationSanity(ImpalaTestSuite):
  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestHmsIntegrationSanity, cls).add_test_dimensions()
    # There is no reason to run these tests using all dimensions.
    cls.ImpalaTestMatrix.add_dimension(create_single_exec_option_dimension())
    cls.ImpalaTestMatrix.add_dimension(
        create_uncompressed_text_dimension(cls.get_workload()))

  @pytest.mark.execute_serially
  def test_sanity(self, vector):
    """Verifies that creating a catalog entity (database, table) in Impala using
    'IF NOT EXISTS' while the entity exists in HMS, does not throw an error."""
    # Create a database in Hive
    self.run_stmt_in_hive("drop database if exists hms_sanity_db cascade")
    self.run_stmt_in_hive("create database hms_sanity_db")
    # Make sure Impala's metadata is in sync.
    if IMPALA_TEST_CLUSTER_PROPERTIES.is_catalog_v2_cluster():
      # Using local catalog + HMS event processor - wait until the database shows up.
      self.wait_for_db_to_appear("hms_sanity_db", timeout_s=30)
    else:
      # Using traditional catalog - need to invalidate to pick up hive-created db.
      self.client.execute("invalidate metadata")
    # Creating a database with the same name using 'IF NOT EXISTS' in Impala should
    # not fail
    self.client.execute("create database if not exists hms_sanity_db")
    # The database should appear in the catalog (IMPALA-2441)
    assert 'hms_sanity_db' in self.all_db_names()
    # Ensure a table can be created in this database from Impala and that it is
    # accessable in both Impala and Hive
    self.client.execute("create table hms_sanity_db.test_tbl_in_impala(a int)")
    self.run_stmt_in_hive("select * from hms_sanity_db.test_tbl_in_impala")
    self.client.execute("select * from hms_sanity_db.test_tbl_in_impala")
    # Create a table in Hive
    self.run_stmt_in_hive("create table hms_sanity_db.test_tbl (a int)")
    # Creating a table with the same name using 'IF NOT EXISTS' in Impala should
    # not fail
    self.client.execute("create table if not exists hms_sanity_db.test_tbl (a int)")
    # The table should not appear in the catalog for catalog_v1 unless invalidate
    # metadata is executed.
    if IMPALA_TEST_CLUSTER_PROPERTIES.is_catalog_v2_cluster():
      self.wait_for_table_to_appear("hms_sanity_db", "test_tbl", 10)
      assert 'test_tbl' in self.client.execute("show tables in hms_sanity_db").data
    else:
      assert 'test_tbl' not in self.client.execute("show tables in hms_sanity_db").data
    self.client.execute("invalidate metadata hms_sanity_db.test_tbl")
    assert 'test_tbl' in self.client.execute("show tables in hms_sanity_db").data

@SkipIfS3.hive
@SkipIfABFS.hive
@SkipIfADLS.hive
@SkipIfIsilon.hive
@SkipIfLocal.hive
class TestHmsIntegration(ImpalaTestSuite):

  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestHmsIntegration, cls).add_test_dimensions()

    if cls.exploration_strategy() != 'exhaustive':
      pytest.skip("Should only run in exhaustive due to long execution time.")

    # There is no reason to run these tests using all dimensions.
    cls.ImpalaTestMatrix.add_dimension(create_single_exec_option_dimension())
    cls.ImpalaTestMatrix.add_dimension(
        create_uncompressed_text_dimension(cls.get_workload()))

  class ImpalaDbWrapper(object):
    """
    A wrapper class for using `with` guards with databases created through
    Impala ensuring deletion even if an exception occurs.
    """

    def __init__(self, impala, db_name):
      self.impala = impala
      self.db_name = db_name

    def __enter__(self):
      self.impala.client.execute(
          'create database if not exists ' + self.db_name)
      return self.db_name

    def __exit__(self, typ, value, traceback):
      self.impala.cleanup_db(self.db_name)

  class ImpalaTableWrapper(object):
    """
    A wrapper class for using `with` guards with tables created through Impala
    ensuring deletion even if an exception occurs.
    """

    def __init__(self, impala, table_name, table_spec):
      self.impala = impala
      self.table_name = table_name
      self.table_spec = table_spec

    def __enter__(self):
      self.impala.client.execute(
          'create table if not exists %s %s' %
          (self.table_name, self.table_spec))
      return self.table_name

    def __exit__(self, typ, value, traceback):
      self.impala.client.execute('drop table if exists %s' % self.table_name)

  def impala_table_stats(self, table):
    """Returns a dictionary of stats for a table according to Impala."""
    output = self.client.execute('show table stats %s' % table).get_data()
    output_lines = output.split('\n')
    result = {}
    for line in output_lines:
      parts = line.split('\t')
      stats = {}
      stats['location'] = parts[-1]
      stats['incremental stats'] = parts[-2]
      stats['format'] = parts[-3]
      stats['cache replication'] = parts[-4]
      stats['bytes cached'] = parts[-5]
      stats['size'] = parts[-6]
      stats['#files'] = parts[-7]
      stats['#rows'] = parts[-8]
      result[tuple(parts[:-8])] = stats
    return result

  def impala_all_column_stats(self, table):
    """Returns a dictionary of stats for columns according to Impala."""
    output = self.client.execute('show column stats %s' % table).get_data()
    output_lines = output.split('\n')
    result = {}
    for line in output_lines:
      stats = line.split('\t')
      attributes = {}
      attributes['type'] = stats[1]
      attributes['ndv'] = stats[2]
      attributes['#nulls'] = stats[3]
      attributes['max size'] = stats[4]
      attributes['avg size'] = stats[5]
      result[stats[0]] = attributes
    return result

  def hive_column_stats(self, table, column):
    """Returns a dictionary of stats for a column according to Hive."""
    output = self.run_stmt_in_hive(
        'describe formatted %s %s' %
        (table, column))
    result = {}
    output_lines = output.split('\n')
    stat_names = map(string.strip, output_lines[0].split(','))
    stat_values = output_lines[3].split(',')
    assert len(stat_names) == len(stat_values)
    for i in range(0, len(stat_names)):
      result[stat_names[i]] = stat_values[i]
    return result

  def impala_columns(self, table_name):
    """
    Returns a dict with column names as the keys and dicts of type and comments
    as the values.
    """
    columns = self.client.execute('describe %s' %
                                  table_name).get_data().split('\n')
    result = {}
    for column in columns:
      attributes = column.split('\t')
      result[attributes[0]] = {'type': attributes[1], 'comment': attributes[2]}
    return result

  def hive_columns(self, table_name):
    """
    Returns a dict with column names as the keys and dicts of types and
    comments as the values.
    """

    columns = self.run_stmt_in_hive(
        'describe %s' % table_name).split('\n')[1:-1]
    result = {}
    for column in columns:
      attributes = column.split(',')
      result[attributes[0]] = {'type': attributes[1], 'comment': attributes[2]}
    return result

  def unique_string(self):
    return ''.join([random.choice(string.ascii_lowercase)
                    for i in range(0, 16)])

  def assert_sql_error(self, engine, command, *strs_in_error):
    """
    Passes 'command' to 'engine' callable (e.g. execute method of a BeeswaxConnection
    object) and makes sure that it raises an exception.
    It also verifies that the string representation of the exception contains all the
    strings listed in 'strs_in_error'.

    If the call doesn't raise an exception or the exception doesn't contain one of the
    strings in 'strs_in_error', it throws AssertError exception.
    """

    try:
      engine(command)
    except Exception as e:
      for str_in_error in strs_in_error:
        assert str_in_error in str(e)
    else:
      assert False, '%s should have triggered an error containing %s' % (
          command, strs_in_error)

  @pytest.mark.execute_serially
  def test_hive_db_hive_table_add_partition(self, vector):
    self.add_hive_partition_helper(vector, HiveDbWrapper, HiveTableWrapper)

  @pytest.mark.execute_serially
  def test_hive_db_impala_table_add_partition(self, vector):
    self.add_hive_partition_helper(vector, HiveDbWrapper, self.ImpalaTableWrapper)

  @pytest.mark.execute_serially
  def test_impala_db_impala_table_add_partition(self, vector):
    self.add_hive_partition_helper(vector, self.ImpalaDbWrapper,
                                   self.ImpalaTableWrapper)

  @pytest.mark.execute_serially
  def test_impala_db_hive_table_add_partition(self, vector):
    self.add_hive_partition_helper(vector, self.ImpalaDbWrapper,
                                   HiveTableWrapper)

  @pytest.mark.xfail(run=False, reason="This is a bug: IMPALA-2426")
  @pytest.mark.execute_serially
  def test_incremental_stats_new_partition(self, vector):
    with self.ImpalaDbWrapper(self, self.unique_string()) as db_name:
      with self.ImpalaTableWrapper(self, db_name + '.' + self.unique_string(),
                                   '(x int) partitioned by (y int)') as table_name:
        self.client.execute('insert into table %s partition (y=42) values (2)'
                            % table_name)
        self.run_stmt_in_hive('alter table %s add partition (y = 333)'
                              % table_name)
        self.client.execute('compute incremental stats %s' % table_name)
        table_stats = self.impala_table_stats(table_name)
        assert 'true' == table_stats[('333',)]['incremental stats']
        assert '0' == table_stats[('333',)]['#rows']
        assert '0' == table_stats[('333',)]['#files']

  def add_hive_partition_helper(self, vector, DbWrapper, TableWrapper):
    """
    Partitions added in Hive can be viewed in Impala after computing stats.
    """
    with DbWrapper(self, self.unique_string()) as db_name:
      self.client.execute('invalidate metadata')
      with TableWrapper(self, db_name + '.' + self.unique_string(),
                        '(x int) partitioned by (y int, z int)') as table_name:
        # Invalidate metadata so Impala can see the table
        self.client.execute('invalidate metadata')
        self.run_stmt_in_hive(
            'alter table %s add partition (y = 333, z = 5309)' %
            table_name)
        self.client.execute('compute incremental stats %s' % table_name)
        # Impala can see the partition's name
        assert [('333', '5309')] == self.get_impala_partition_info(table_name, 'y', 'z')
        # Impala's compute stats didn't alter Hive's knowledge of the partition
        assert ['y=333/z=5309'] == self.hive_partition_names(table_name)
    self.add_hive_partition_table_stats_helper(vector, DbWrapper, TableWrapper)

  def add_hive_partition_table_stats_helper(
          self, vector, DbWrapper, TableWrapper):
    """
    Partitions added in Hive don't make Impala's table stats incorrect.
    """
    # TODO: check the same thing with column stats
    with DbWrapper(self, self.unique_string()) as db_name:
      self.client.execute('invalidate metadata')
      with TableWrapper(self, db_name + '.' + self.unique_string(),
                        '(x int) partitioned by (y int, z int)') as table_name:
        # Invalidate metadata so Impala can see the table
        self.client.execute('invalidate metadata')
        self.client.execute(
            'insert into table %s partition (y=42, z=867) values (2)'
            % table_name)
        self.client.execute('compute incremental stats %s' % table_name)
        impala_table_stats = self.impala_table_stats(table_name)
        self.run_stmt_in_hive(
            'alter table %s add partition (y = 333, z = 5309)' %
            table_name)
        self.client.execute('compute incremental stats %s' % table_name)
        assert impala_table_stats[
            ('42', '867')] == self.impala_table_stats(table_name)[
            ('42', '867')]

  @pytest.mark.execute_serially
  def test_add_impala_partition(self, vector):
    """
    Partitions added in Impala can be viewed in Hive immediately
    """
    with self.ImpalaDbWrapper(self, self.unique_string()) as db_name:
      with self.ImpalaTableWrapper(self, db_name + '.' + self.unique_string(),
                                   '(x int) partitioned by (y int, z int)') as table_name:
        self.client.execute(
            'insert into table %s partition (y=42, z=867) values (2)'
            % table_name)
        assert [('42', '867')] == self.get_impala_partition_info(table_name, 'y', 'z')
        assert ['y=42/z=867'] == self.hive_partition_names(table_name)

  @pytest.mark.execute_serially
  def test_drop_column_maintains_stats(self, vector):
    """
    Dropping a column in Impala doesn't alter the stats of other columns in Hive
    or Impala.
    """
    with self.ImpalaDbWrapper(self, self.unique_string()) as db_name:
      with self.ImpalaTableWrapper(self, db_name + '.' + self.unique_string(),
                                   '(x int, y int, z int)') as table_name:
        self.run_stmt_in_hive('select * from %s' % table_name)
        self.run_stmt_in_hive(
            'use %s; analyze table %s compute statistics for columns' %
            (db_name, table_name.split('.')[1]))
        self.client.execute('compute stats %s' % table_name)
        hive_x_stats = self.hive_column_stats(table_name, 'x')
        hive_y_stats = self.hive_column_stats(table_name, 'y')
        impala_stats = self.impala_all_column_stats(table_name)
        self.client.execute('alter table %s drop column z' % table_name)
        assert hive_x_stats == self.hive_column_stats(table_name, 'x')
        assert hive_y_stats == self.hive_column_stats(table_name, 'y')
        assert impala_stats['x'] == self.impala_all_column_stats(table_name)[
            'x']
        assert impala_stats['y'] == self.impala_all_column_stats(table_name)[
            'y']
        self.run_stmt_in_hive(
            'alter table %s replace columns (x int)' %
            table_name)
        assert hive_x_stats == self.hive_column_stats(table_name, 'x')
        assert impala_stats['x'] == self.impala_all_column_stats(table_name)[
            'x']

  @pytest.mark.execute_serially
  def test_select_without_compute_stats(self, vector):
    """
    Data added in Hive shows up in Impala 'select *', and if the table is not
    partitioned, 'compute incremental stats' is not required.
    """

    with self.ImpalaDbWrapper(self, self.unique_string()) as db_name:
      with self.ImpalaTableWrapper(self, db_name + '.' + self.unique_string(),
                                   '(x int)') as table_name:
        # In the unpartitioned case, 'compute incremental stats' is not
        # required.
        self.run_stmt_in_hive(
            'insert into table %s values (66)'
            % table_name)
        assert '66' == self.client.execute(
            'select * from %s' % table_name).get_data()

      with self.ImpalaTableWrapper(self, db_name + '.' + self.unique_string(),
                                   '(x int) partitioned by (y int)') as table_name:
        assert [] == self.get_impala_partition_info(table_name, 'y')
        self.run_stmt_in_hive(
            'insert into table %s partition (y=33) values (44)'
            % table_name)
        self.client.execute('compute incremental stats %s' % table_name)
        assert '44\t33' == self.client.execute(
            'select * from %s' % table_name).get_data()

  @pytest.mark.xfail(run=False, reason="This is a bug: IMPALA-2458")
  @pytest.mark.execute_serially
  def test_overwrite_added_column(self, vector):
    """
    Impala can't overwrite Hive's column types, and vice versa.
    """
    with self.ImpalaDbWrapper(self, self.unique_string()) as db_name:
      with self.ImpalaTableWrapper(self, db_name + '.' + self.unique_string(),
                                   '(x int, y int)') as table_name:
        inttype = {'comment': '', 'type': 'int'}
        hive_expected = {'x': inttype, 'y': inttype}
        impala_expected = {'x': inttype, 'y': inttype}
        # Hive and Impala both know all columns:
        assert hive_expected == self.hive_columns(table_name)
        assert impala_expected == self.impala_columns(table_name)
        # Add column in Hive but don't tell Impala
        self.run_stmt_in_hive(
            'alter table %s add columns (z int)' % table_name)
        hive_expected['z'] = inttype
        assert hive_expected == self.hive_columns(table_name)
        # Overwriting an Hive-created column in Impala does not work
        self.assert_sql_error(
            self.client.execute,
            'alter table %s add columns (z string)' %
            table_name,
            'Column already exists: z')
        # Overwriting an Impala-created column in Hive does not work
        self.client.execute(
            'alter table %s add columns (v string)' % table_name)
        self.assert_sql_error(
            self.run_stmt_in_hive,
            'alter table %s add columns (v string)' %
            table_name,
            'Duplicate column name: v')

  @pytest.mark.execute_serially
  def test_compute_stats_get_to_hive(self, vector):
    """Stats computed in Impala are also visible in Hive."""
    with self.ImpalaDbWrapper(self, self.unique_string()) as db_name:
      with self.ImpalaTableWrapper(self, db_name + '.' + self.unique_string(),
                                   '(x int)') as table_name:
        self.run_stmt_in_hive(
            'insert into table %s values (33)' % table_name)
        hive_stats = self.hive_column_stats(table_name, 'x')
        impala_stats = self.client.execute('show column stats %s' % table_name)
        self.client.execute('compute stats %s' % table_name)
        assert impala_stats != self.client.execute(
            'show column stats %s' % table_name)
        assert hive_stats != self.hive_column_stats(table_name, 'x')

  @pytest.mark.execute_serially
  def test_compute_stats_get_to_impala(self, vector):
    """Column stats computed in Hive are also visible in Impala."""
    with HiveDbWrapper(self, self.unique_string()) as db_name:
      with HiveTableWrapper(self, db_name + '.' + self.unique_string(),
                            '(x int)') as table_name:
        hive_stats = self.hive_column_stats(table_name, 'x')
        self.client.execute('invalidate metadata')
        self.client.execute('refresh %s' % table_name)
        impala_stats = self.impala_all_column_stats(table_name)
        self.run_stmt_in_hive(
            'insert into table %s values (33)' % table_name)
        self.run_stmt_in_hive(
            'use %s; analyze table %s compute statistics for columns' %
            (db_name, table_name.split('.')[1]))
        new_hive_stats = self.hive_column_stats(table_name, 'x')
        assert hive_stats != new_hive_stats
        assert '33' == new_hive_stats['min']
        assert '33' == new_hive_stats['max']
        assert '0' == new_hive_stats['num_nulls']
        self.client.execute('refresh %s' % table_name)
        new_impala_stats = self.impala_all_column_stats(table_name)
        assert impala_stats != new_impala_stats
        assert '0' == new_impala_stats['x']['#nulls']

  @pytest.mark.execute_serially
  def test_drop_partition(self, vector):
    """
    Impala can see that a partitions was dropped by Hive by invalidating
    metadata.
    """

    with self.ImpalaDbWrapper(self, self.unique_string()) as db_name:
      with self.ImpalaTableWrapper(self, db_name + '.' + self.unique_string(),
                                   '(x int) partitioned by (y int)') as table_name:
        self.run_stmt_in_hive(
            'insert into table %s partition(y=33) values (44)' % table_name)
        self.client.execute('compute stats %s' % table_name)
        self.run_stmt_in_hive(
            'alter table %s drop partition (y=33)' % table_name)
        self.client.execute('invalidate metadata %s' % table_name)
        assert '' == self.client.execute(
            'select * from %s' % table_name).get_data()

  @pytest.mark.execute_serially
  def test_drop_column_with_data(self, vector):
    """Columns dropped by Hive are ignored in Impala 'select *'."""
    with self.ImpalaDbWrapper(self, self.unique_string()) as db_name:
      with self.ImpalaTableWrapper(self, db_name + '.' + self.unique_string(),
                                   '(x int, y int)') as table_name:
        self.run_stmt_in_hive(
            'insert into table %s values (33,44)' % table_name)
        self.run_stmt_in_hive(
            'alter table %s replace columns (x int)' % table_name)
        assert '33' == self.client.execute(
            'select * from %s' % table_name).get_data()

  @pytest.mark.execute_serially
  def test_add_column(self, vector):
    """Columns added in one engine are visible in the other via DESCRIBE."""
    with self.ImpalaDbWrapper(self, self.unique_string()) as db_name:
      with self.ImpalaTableWrapper(self, db_name + '.' + self.unique_string(),
                                   '(x int)') as table_name:
        int_column = {'type': 'int', 'comment': ''}
        expected = {'x': int_column}
        assert expected == self.hive_columns(table_name)
        assert expected == self.impala_columns(table_name)
        self.client.execute('alter table %s add columns (y int)' % table_name)
        expected['y'] = int_column
        assert expected == self.hive_columns(table_name)
        assert expected == self.impala_columns(table_name)
        self.run_stmt_in_hive(
            'alter table %s add columns (z int)' %
            table_name)
        self.client.execute('invalidate metadata %s' % table_name)
        expected['z'] = int_column
        assert expected == self.hive_columns(table_name)
        assert expected == self.impala_columns(table_name)

  @pytest.mark.execute_serially
  def test_drop_database(self, vector):
    """
    If a DB is created, then dropped, in Hive, Impala can create one with the
    same name without invalidating metadata.
    """

    test_db = self.unique_string()
    with HiveDbWrapper(self, test_db) as db_name:
      pass
    self.assert_sql_error(
        self.client.execute,
        'create table %s.%s (x int)' %
        (test_db,
         self.unique_string()),
        'Database does not exist: %s' %
        test_db)
    with self.ImpalaDbWrapper(self, test_db) as db_name:
      pass

  @pytest.mark.execute_serially
  def test_table_format_change(self, vector):
    """
    Hive storage format changes propagate to Impala.
    """
    # TODO: check results of insert, then select * before and after
    # storage format change.
    with HiveDbWrapper(self, self.unique_string()) as db_name:
      with HiveTableWrapper(self, db_name + '.' + self.unique_string(),
                            '(x int, y int) stored as parquet') as table_name:
        self.client.execute('invalidate metadata')
        self.client.execute('invalidate metadata %s' % table_name)
        print self.impala_table_stats(table_name)
        assert 'PARQUET' == self.impala_table_stats(table_name)[()]['format']
        self.run_stmt_in_hive(
            'alter table %s set fileformat avro' % table_name)
        self.client.execute('invalidate metadata %s' % table_name)
        assert 'AVRO' == self.impala_table_stats(table_name)[()]['format']

  @pytest.mark.execute_serially
  def test_change_column_type(self, vector):
    """Hive column type changes propagate to Impala."""

    with HiveDbWrapper(self, self.unique_string()) as db_name:
      with HiveTableWrapper(self, db_name + '.' + self.unique_string(),
                            '(x int, y int)') as table_name:
        self.run_stmt_in_hive(
            'insert into table %s values (33,44)' % table_name)
        self.run_stmt_in_hive('alter table %s change y y string' % table_name)
        assert '33,44' == self.run_stmt_in_hive(
            'select * from %s' % table_name).split('\n')[1]
        self.client.execute('invalidate metadata %s' % table_name)
        assert '33\t44' == self.client.execute(
            'select * from %s' % table_name).get_data()
        assert 'string' == self.impala_columns(table_name)['y']['type']

  @pytest.mark.execute_serially
  def test_change_parquet_column_type(self, vector):
    """
    Changing column types in Parquet doesn't work in Hive and it causes
    'select *' to fail in Impala as well, after invalidating metadata. This is a
    known issue with changing column types in Hive/parquet.
    """

    with HiveDbWrapper(self, self.unique_string()) as db_name:
      with HiveTableWrapper(self, db_name + '.' + self.unique_string(),
                            '(x int, y int) stored as parquet') as table_name:
        self.run_stmt_in_hive(
            'insert into table %s values (33,44)' % table_name)
        assert '33,44' == self.run_stmt_in_hive(
            'select * from %s' % table_name).split('\n')[1]
        self.client.execute('invalidate metadata')
        assert '33\t44' == self.client.execute(
            'select * from %s' % table_name).get_data()
        self.run_stmt_in_hive('alter table %s change y y string' % table_name)
        self.assert_sql_error(
            self.run_stmt_in_hive, 'select * from %s' %
            table_name, 'Cannot inspect org.apache.hadoop.io.IntWritable')
        self.client.execute('invalidate metadata %s' % table_name)
        self.assert_sql_error(
            self.client.execute,
            'select * from %s' %
            table_name,
            "Column type: STRING, Parquet schema:")

  @pytest.mark.execute_serially
  def test_change_table_name(self, vector):
    """
    Changing the table name in Hive propagates to Impala after 'invalidate
    metadata'.
    """

    with HiveDbWrapper(self, self.unique_string()) as db_name:
      with HiveTableWrapper(self, db_name + '.' + self.unique_string(),
                            '(x int, y int)') as table_name:
        self.client.execute('invalidate metadata')
        int_column = {'type': 'int', 'comment': ''}
        expected_columns = {'x': int_column, 'y': int_column}
        assert expected_columns == self.impala_columns(table_name)
        new_name = table_name + '2'
        self.run_stmt_in_hive('alter table %s rename to %s' %
                              (table_name, new_name))
        self.client.execute('invalidate metadata')
        assert expected_columns == self.impala_columns(new_name)
        self.assert_sql_error(self.client.execute,
                              'describe %s' % table_name,
                              'Could not resolve path')

  @pytest.mark.execute_serially
  def test_add_overlapping_partitions(self, vector):
    """
    IMPALA-1670, IMPALA-4141: Test interoperability with Hive when adding overlapping
    partitions to a table
    """
    with self.ImpalaDbWrapper(self, self.unique_string()) as db_name:
      # Create a table in Impala.
      with self.ImpalaTableWrapper(self, db_name + '.' + self.unique_string(),
                                   '(a int) partitioned by (x int)') as table_name:
        # Trigger metadata load. No partitions exist yet in Impala.
        assert [] == self.get_impala_partition_info(table_name, 'x')

        # Add partition in Hive.
        self.run_stmt_in_hive("alter table %s add partition (x=2)" % table_name)
        # Impala is not aware of the new partition.
        assert [] == self.get_impala_partition_info(table_name, 'x')

        # Try to add partitions with caching in Impala, one of them (x=2) exists in HMS.
        self.assert_sql_error(self.client.execute,
            "alter table %s add partition (x=1) uncached "
            "partition (x=2) cached in 'testPool' with replication=2 "
            "partition (x=3) cached in 'testPool' with replication=3" % table_name,
            "Partition already exists")
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
        self.client.execute("alter table %s add if not exists "\
            "partition (x=1) location '/_X_1'" % table_name)
        # (x=1) partition's location hasn't changed
        (x1_value, x1_location) = self.get_impala_partition_info(table_name, 'x',
            'Location')[0]
        assert '1' == x1_value
        assert x1_location.endswith("/x=1");

  @pytest.mark.execute_serially
  def test_add_preexisting_partitions_with_data(self, vector):
    """
    IMPALA-1670, IMPALA-4141: After addding partitions that already exist in HMS, Impala
    can access the partition data.
    """
    with self.ImpalaDbWrapper(self, self.unique_string()) as db_name:
      # Create a table in Impala.
      with self.ImpalaTableWrapper(self, db_name + '.' + self.unique_string(),
                                   '(a int) partitioned by (x int)') as table_name:
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
        self.client.execute("alter table %s add if not exists partition (x=1) "\
            "partition (x=2) "
            "partition (x=3)" % table_name)
        # Impala sees the partitions
        assert [('1',), ('2',), ('3',)] == self.get_impala_partition_info(table_name, 'x')
        # Data exists in Impala
        assert ['1\t1', '1\t2', '1\t3',
            '2\t1', '2\t2', '2\t3', '2\t4',
            '3\t1'] ==\
            self.client.execute('select x, a from %s order by x, a' %
            table_name).get_data().split('\n')

  @pytest.mark.execute_serially
  def test_impala_partitions_accessible_in_hive(self, vector):
    """
    IMPALA-1670, IMPALA-4141: Partitions added in Impala are accessible through Hive
    """
    with self.ImpalaDbWrapper(self, self.unique_string()) as db_name:
      # Create a table in Impala.
      with self.ImpalaTableWrapper(self, db_name + '.' + self.unique_string(),
                                   '(a int) partitioned by (x int)') as table_name:
        # Add partitions in Impala.
        self.client.execute("alter table %s add partition (x=1) "
            "partition (x=2) "
            "partition (x=3)" % table_name)
        # Insert rows in Impala
        self.client.execute("insert into %s partition(x=1) values (1), (2), (3)"
            % table_name)
        self.client.execute("insert into %s partition(x=2) values (1), (2), (3), (4)"
            % table_name)
        self.client.execute("insert into %s partition(x=3) values (1)"
            % table_name)

        # Hive sees the partitions
        assert ['x=1', 'x=2', 'x=3'] == self.hive_partition_names(table_name)
        # Data exists in Hive
        data = self.run_stmt_in_hive('select x, a from %s order by x, a' % table_name)
        assert ['x,a',
            '1,1', '1,2', '1,3',
            '2,1', '2,2', '2,3', '2,4',
            '3,1'] == data.strip().split('\n')
