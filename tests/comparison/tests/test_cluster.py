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

# These are unit tests for cluster.py.

from __future__ import absolute_import, division, print_function
from time import time

from tests.common.errors import Timeout

from tests.comparison.common import Column, Table
from tests.comparison.db_types import BigInt, String

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
    table.add_col(Column(table, "bigint_col", BigInt))
    table.add_col(Column(table, "string_col", String))
    cursor.create_table(table)
    try:
      other = hive_cursor.describe_table(table.name)
      assert other.name == table.name
      assert other.cols == table.cols
    finally:
      cursor.drop_table(table.name)
