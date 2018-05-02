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
# Utilities for interacting with Hive.


class HiveDbWrapper(object):
  """
  A wrapper class for using `with` guards with databases created through Hive
  ensuring deletion even if an exception occurs.
  """

  def __init__(self, hive, db_name):
    self.hive = hive
    self.db_name = db_name

  def __enter__(self):
    self.hive.run_stmt_in_hive(
        'create database if not exists ' + self.db_name)
    return self.db_name

  def __exit__(self, typ, value, traceback):
    self.hive.run_stmt_in_hive(
        'drop database if exists %s cascade' % self.db_name)


class HiveTableWrapper(object):
  """
  A wrapper class for using `with` guards with tables created through Hive
  ensuring deletion even if an exception occurs.
  """

  def __init__(self, hive, table_name, table_spec):
    self.hive = hive
    self.table_name = table_name
    self.table_spec = table_spec

  def __enter__(self):
    self.hive.run_stmt_in_hive(
        'create table if not exists %s %s' %
        (self.table_name, self.table_spec))
    return self.table_name

  def __exit__(self, typ, value, traceback):
    self.hive.run_stmt_in_hive('drop table if exists %s' % self.table_name)
