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

from __future__ import absolute_import, division, print_function
from builtins import range
import os
import pytest
import requests
import string
from contextlib import contextmanager
from kudu.schema import (
    BOOL,
    DOUBLE,
    FLOAT,
    INT16,
    INT32,
    INT64,
    INT8,
    SchemaBuilder,
    STRING,
    BINARY,
    UNIXTIME_MICROS,
    DATE)
from kudu.client import Partitioning
from random import choice, sample
from string import ascii_lowercase, digits

from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.test_dimensions import create_kudu_dimension

DEFAULT_KUDU_MASTER_WEBUI_PORT = os.getenv('KUDU_MASTER_WEBUI_PORT', '8051')


def get_kudu_master_webpage(page_name):
  kudu_master = pytest.config.option.kudu_master_hosts

  if "," in kudu_master:
    raise NotImplementedError("Multi-master not supported yet")
  if ":" in kudu_master:
    kudu_master_host = kudu_master.split(":")[0]
  else:
    kudu_master_host = kudu_master
  url = "http://%s:%s/%s" % (kudu_master_host, DEFAULT_KUDU_MASTER_WEBUI_PORT, page_name)
  return requests.get(url).text


def get_kudu_master_flag(flag):
  varz = get_kudu_master_webpage("varz")
  for line in varz.split("\n"):
    split = line.split("=")
    if len(split) == 2 and split[0] == flag:
      return split[1]
  assert False, "Failed to find Kudu master flag: %s" % flag

class KuduTestSuite(ImpalaTestSuite):

  # Lazily set.
  __DB_NAME = None

  @classmethod
  def get_conn_timeout(cls):
    # For IMPALA-5079,IMPALA-4454
    return 60 * 5 # 5 minutes

  @classmethod
  def setup_class(cls):
    super(KuduTestSuite, cls).setup_class()

  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(KuduTestSuite, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(
        create_kudu_dimension(cls.get_workload()))

  @classmethod
  def auto_create_db(cls):
    return True

  @classmethod
  def get_db_name(cls):
    # When py.test runs with the xdist plugin, several processes are started and each
    # process runs some partition of the tests. It's possible that multiple processes
    # will call this method. To avoid multiple processes using the same database at the
    # same time, the database name is formed by concatenating the test class name, the pid
    # and a random value. The class name distinguishes classes and the pid distinguishes
    # the same class run in different processes. The value is cached so within a single
    # process the same database name is always used for the class. This doesn't need to
    # be thread-safe since multi-threading is never used.
    if not cls.__DB_NAME:
      salt = choice(ascii_lowercase) + "".join(sample(ascii_lowercase + digits, 5))
      cls.__DB_NAME = cls.__name__.lower() + "_" + str(os.getpid()) + "_" + salt

    return cls.__DB_NAME

  @classmethod
  def random_table_name(cls):
    return "".join(choice(string.ascii_lowercase) for _ in range(10))

  @classmethod
  def to_kudu_table_name(cls, db_name, tbl_name):
    """Return the name of the underlying Kudu table, from the Impala database and table
    name. This must be kept in sync with KuduUtil.getDefaultKuduTableName() in the
    FE."""
    if get_kudu_master_flag("--hive_metastore_uris") != "":
      return "%s.%s" % (db_name, tbl_name)
    else:
      return "impala::%s.%s" % (db_name, tbl_name)

  @classmethod
  def get_kudu_table_base_name(cls, name):
    return name.split(".")[-1]

  @contextmanager
  def temp_kudu_table(self, kudu, col_types, name=None, num_key_cols=1, col_names=None,
      prepend_db_name=True, db_name=None, num_partitions=2):
    """Create and return a table. This function should be used in a "with" context.
       'kudu' must be a kudu.client.Client. If a table name is not provided, a random
       name will be used. If 'prepend_db_name' is True, the table name will be prepended
       with (get_db_name() + "."). If column names are not provided, the letters
       "a", "b", "c", ... will be used. The number of partitions can be set using
       'num_partitions'.

       Example:
         with self.temp_kudu_table(kudu, [INT32]) as kudu_table:
            assert kudu.table_exists(kudu_table.name)
         assert not kudu.table_exists(kudu_table.name)
    """
    if not col_names:
      if len(col_types) > 26:
        raise Exception("Too many columns for default naming")
      col_names = [chr(97 + i) for i in range(len(col_types))]
    schema_builder = SchemaBuilder()
    for i, t in enumerate(col_types):
      column_spec = schema_builder.add_column(col_names[i], type_=t)
      if i < num_key_cols:
        column_spec.nullable(False)
    schema_builder.set_primary_keys(col_names[:num_key_cols])
    schema = schema_builder.build()
    name = name or self.random_table_name()
    if prepend_db_name:
      name = (db_name or self.get_db_name().lower()) + "." + name
    kudu.create_table(name, schema,
        partitioning=Partitioning().add_hash_partitions(col_names[:num_key_cols],
            num_partitions))
    try:
      yield kudu.table(name)
    finally:
      if kudu.table_exists(name):
        kudu.delete_table(name)

  @contextmanager
  def drop_impala_table_after_context(self, cursor, table_name):
    """For use in a "with" block: The named table will be dropped using the provided
       cursor when the block exits.

       cursor.execute("CREATE TABLE foo ...")
       with drop_impala_table_after_context(cursor, "foo"):
         ...
       # Now table foo no longer exists.
    """
    try:
      yield
    finally:
      cursor.execute("DROP TABLE %s" % table_name)

  def kudu_col_type_to_impala_col_type(self, col_type):
    mapping = {BOOL: "BOOLEAN",
        DOUBLE: "DOUBLE",
        FLOAT: "FLOAT",
        INT16: "SMALLINT",
        INT32: "INT",
        INT64: "BIGINT",
        INT8: "TINYINT",
        STRING: "STRING",
        BINARY: "BINARY",
        UNIXTIME_MICROS: "TIMESTAMP",
        DATE: "DATE"}
    if col_type not in mapping:
      raise Exception("Unexpected column type: %s" % col_type)
    return mapping[col_type]
