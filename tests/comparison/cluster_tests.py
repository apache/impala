# Copyright (c) 2015 Cloudera, Inc. All rights reserved.

# These are unit tests for cluster.py.

from time import time

from tests.common.errors import Timeout

from common import Column, Table
from db_types import BigInt, String

class TestCluster(object):

  def test_cmd(self, cluster):
    host = cluster.impala.impalads[0].host_name
    assert cluster.shell("echo -n HI", host) == "HI"

    try:
      cluster.shell("bad", host)
      assert False
    except Exception as e:
      assert "command not found" in str(e)

    start = time()
    try:
      cluster.shell("echo HI; sleep 60", host, timeout_secs=3)
      assert False
    except Timeout as e:
      assert "HI" in str(e)
      assert 3 <= time() - start <= 6


class TestHdfs(object):

  def test_ls(self, cluster):
    ls = cluster.hdfs.create_client().list("/")
    assert "tmp" in ls
    assert "etc" not in ls


class TestHive(object):

  def test_list_databases(self, hive_cursor):
    assert "default" in hive_cursor.list_db_names()

  def test_non_mr_exec(self, hive_cursor):
    hive_cursor.execute("SELECT 1")
    rows = hive_cursor.fetchall()
    assert rows
    assert rows[0][0] == 1


class TestImpala(object):

  def test_list_databases(self, cursor):
    assert "default" in cursor.list_db_names()

  def test_exec(self, cursor):
    cursor.execute("SELECT 1")
    rows = cursor.fetchall()
    assert rows
    assert rows[0][0] == 1


class TestModel(object):

  def test_table_model(self, cursor, hive_cursor):
    table = Table("some_test_table")
    cursor.drop_table(table.name, if_exists=True)
    table.storage_format = 'textfile'
    table.cols.append(Column(table, "bigint_col", BigInt))
    table.cols.append(Column(table, "string_col", String))
    cursor.create_table(table)
    try:
      other = hive_cursor.describe_table(table.name)
      assert other.name == table.name
      assert other.cols == table.cols
    finally:
      cursor.drop_table(table.name)
