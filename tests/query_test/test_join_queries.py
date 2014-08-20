#!/usr/bin/env python
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
# Targeted tests for Impala joins
#
import logging
import pytest
from copy import copy
from tests.common.test_vector import *
from tests.common.impala_test_suite import *

class TestJoinQueries(ImpalaTestSuite):
  BATCH_SIZES = [0, 1]

  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestJoinQueries, cls).add_test_dimensions()
    cls.TestMatrix.add_dimension(
        TestDimension('batch_size', *TestJoinQueries.BATCH_SIZES))
    # TODO: Look into splitting up join tests to accomodate hbase.
    # Joins with hbase tables produce drastically different results.
    cls.TestMatrix.add_constraint(lambda v:\
        v.get_value('table_format').file_format in ['parquet'])

    if cls.exploration_strategy() != 'exhaustive':
      # Cut down on execution time when not running in exhaustive mode.
      cls.TestMatrix.add_constraint(lambda v: v.get_value('batch_size') != 1)

  def test_joins(self, vector):
    new_vector = copy(vector)
    new_vector.get_value('exec_option')['batch_size'] = vector.get_value('batch_size')
    self.run_test_case('QueryTest/joins', new_vector)

  def test_outer_joins(self, vector):
    new_vector = copy(vector)
    new_vector.get_value('exec_option')['batch_size'] = vector.get_value('batch_size')
    self.run_test_case('QueryTest/outer-joins', new_vector)

class TestAntiJoinQueries(ImpalaTestSuite):
  IMP1160_TABLES = ['functional.imp1160a', 'functional.imp1160b']

  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestAntiJoinQueries, cls).add_test_dimensions()
    cls.TestMatrix.add_dimension(
        TestDimension('batch_size', *TestJoinQueries.BATCH_SIZES))
    # TODO: Look into splitting up join tests to accomodate hbase.
    # Joins with hbase tables produce drastically different results.
    cls.TestMatrix.add_constraint(lambda v:\
        v.get_value('table_format').file_format in ['parquet'])

    if cls.exploration_strategy() != 'exhaustive':
      # Cut down on execution time when not running in exhaustive mode.
      cls.TestMatrix.add_constraint(lambda v: v.get_value('batch_size') != 1)

  def setup_method(self, method):
    self.__cleanup_anti_join_tables()
    self.__load_anti_join_tables()

  def teardown_method(self, method):
    self.__cleanup_anti_join_tables()

  def __load_anti_join_tables(self):
    # Cleanup, create and load fresh test tables for anti-join test
    for t in self.IMP1160_TABLES:
      self.client.execute('create table if not exists %s(a int, b int, c int)' % t)
    # loads some values with NULLs in the first table
    self.client.execute('insert into %s values(1,1,1)' % self.IMP1160_TABLES[0]);
    self.client.execute('insert into %s values(1,1,10)' % self.IMP1160_TABLES[0]);
    self.client.execute('insert into %s values(1,2,10)' % self.IMP1160_TABLES[0]);
    self.client.execute('insert into %s values(1,3,10)' % self.IMP1160_TABLES[0]);
    self.client.execute('insert into %s values(NULL,NULL,30)'  % self.IMP1160_TABLES[0]);
    self.client.execute('insert into %s values(2,4,30)' % self.IMP1160_TABLES[0]);
    self.client.execute('insert into %s values(2,NULL,20)' % self.IMP1160_TABLES[0]);
    # loads some values with NULLs in the second table
    self.client.execute('insert into %s values(1,1,1)' % self.IMP1160_TABLES[1]);
    self.client.execute('insert into %s values(1,1,10)' % self.IMP1160_TABLES[1]);
    self.client.execute('insert into %s values(1,2,5)' % self.IMP1160_TABLES[1]);
    self.client.execute('insert into %s values(1,NULL,10)' % self.IMP1160_TABLES[1]);
    self.client.execute('insert into %s values(2,10,NULL)' % self.IMP1160_TABLES[1]);
    self.client.execute('insert into %s values(3,NULL,NULL)' % self.IMP1160_TABLES[1]);
    self.client.execute('insert into %s values(3,NULL,50)' % self.IMP1160_TABLES[1]);

  def __cleanup_anti_join_tables(self):
    for t in self.IMP1160_TABLES:
      self.client.execute('drop table if exists %s' % t)

  def test_anti_joins(self, vector):
    new_vector = copy(vector)
    new_vector.get_value('exec_option')['batch_size'] = vector.get_value('batch_size')
    self.run_test_case('QueryTest/anti-joins', new_vector)
