# Copyright 2012 Cloudera Inc.
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

import pytest
from copy import copy
from tests.beeswax.impala_beeswax import ImpalaBeeswaxException
from tests.common.skip import SkipIf
from tests.common.test_vector import *
from tests.common.impala_test_suite import *

@SkipIf.kudu_not_supported
class TestKuduOperations(ImpalaTestSuite):
  """
  This suite tests the different modification operations when using a kudu table.
  """

  @classmethod
  def file_format_constraint(cls, v):
    return v.get_value('table_format').file_format in ["parquet"]

  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestKuduOperations, cls).add_test_dimensions()
    cls.TestMatrix.add_constraint(cls.file_format_constraint)

  # TODO(kudu-merge) IMPALA-3178 DROP DATABASE ... CASCADE is broken in Kudu so we need
  # to clean up table-by-table. Once solved, delete this and rely on the overriden method.
  def cleanup_db(self, db_name):
    self.client.execute("use default")
    self.client.set_configuration({'sync_ddl': True})
    if db_name + "\t" in self.client.execute("show databases", ).data:
      # We use quoted identifiers to avoid name clashes with keywords
      for tbl_name in self.client.execute("show tables in `" + db_name + "`").data:
        full_tbl_name = '`%s`.`%s`' % (db_name, tbl_name)
        result = self.client.execute("describe formatted " + full_tbl_name)
        if 'VIRTUAL_VIEW' in '\n'.join(result.data):
          self.client.execute("drop view " + full_tbl_name)
        else:
          self.client.execute("drop table " + full_tbl_name)
      for fn_result in self.client.execute("show functions in `" + db_name + "`").data:
        # First column is the return type, second is the function signature
        fn_name = fn_result.split('\t')[1]
        self.client.execute("drop function `%s`.%s" % (db_name, fn_name))
      for fn_result in self.client.execute(\
        "show aggregate functions in `" + db_name + "`").data:
        fn_name = fn_result.split('\t')[1]
        self.client.execute("drop function `%s`.%s" % (db_name, fn_name))
      self.client.execute("drop database `" + db_name + "`")

  def setup_method(self, method):
    self.cleanup_db("kududb_test")
    self.client.execute("create database kududb_test")

  def teardown_method(self, method):
    self.cleanup_db("kududb_test")

  def test_kudu_scan_node(self, vector):
    self.run_test_case('QueryTest/kudu-scan-node', vector, use_db="functional_kudu")

  @pytest.mark.execute_serially
  def test_insert_update_delete(self, vector):
    self.run_test_case('QueryTest/kudu_crud', vector, use_db="kududb_test")

  @pytest.mark.execute_serially
  def test_kudu_partition_ddl(self, vector):
    self.run_test_case('QueryTest/kudu_partition_ddl', vector, use_db="kududb_test")

  # TODO(kudu-merge) IMPALA-3179 - Altering table properties is broken. When that is
  # solved uncomment this.
  #def test_kudu_alter_table(self, vector):
  #  self.run_test_case('QueryTest/kudu_alter', vector, use_db="kududb_test")

  def test_kudu_stats(self, vector):
    self.run_test_case('QueryTest/kudu_stats', vector, use_db="kududb_test")


@SkipIf.kudu_not_supported
class TestKuduMemLimits(ImpalaTestSuite):
  QUERIES = ["select * from kudu_mem_limit.lineitem where l_orderkey = -1",
             "select * from kudu_mem_limit.lineitem where l_commitdate like '%cheese'",
             "select * from kudu_mem_limit.lineitem limit 90"]

  # The value indicates the minimum memory requirements for the queries above, the first
  # memory limit corresponds to the first query
  QUERY_MEM_LIMITS = [1, 1, 10]

  # The values from this array are used as a mem_limit test dimension
  TEST_LIMITS = [1, 10, 0]

  CREATE = """
    CREATE TABLE kudu_mem_limit.lineitem (
    l_orderkey BIGINT,
    l_linenumber INT,
    l_partkey BIGINT,
    l_suppkey BIGINT,
    l_quantity double,
    l_extendedprice double,
    l_discount double,
    l_tax double,
    l_returnflag STRING,
    l_linestatus STRING,
    l_shipdate STRING,
    l_commitdate STRING,
    l_receiptdate STRING,
    l_shipinstruct STRING,
    l_shipmode STRING,
    l_comment STRING
  )
  TBLPROPERTIES(
    'storage_handler' = 'com.cloudera.kudu.hive.KuduStorageHandler',
    'kudu.table_name' = 'tpch_lineitem',
    'kudu.master_addresses' = '127.0.0.1',
    'kudu.key_columns' = 'l_orderkey,l_linenumber'
  )
  """

  LOAD = """
  insert into kudu_mem_limit.lineitem
  select l_orderkey, l_linenumber, l_partkey, l_suppkey, cast(l_quantity as double),
  cast(l_extendedprice as double), cast(l_discount as double), cast(l_tax as double),
  l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct,
  l_shipmode, l_comment from tpch_parquet.lineitem;
  """

  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestKuduMemLimits, cls).add_test_dimensions()

    # add mem_limit as a test dimension.
    new_dimension = TestDimension('mem_limit', *TestKuduMemLimits.TEST_LIMITS)
    cls.TestMatrix.add_dimension(new_dimension)
    if cls.exploration_strategy() != 'exhaustive':
      cls.TestMatrix.add_constraint(lambda v:\
          v.get_value('table_format').file_format in ['parquet'])

  def setup_method(self, method):
    self.cleanup_db("kudu_mem_limit")
    self.client.execute("create database kudu_mem_limit")
    self.client.execute(self.CREATE)
    self.client.execute(self.LOAD)

  def teardown_method(self, methd):
    self.cleanup_db("kudu_mem_limit")

  # TODO(kudu-merge) IMPALA-3178 DROP DATABASE ... CASCADE is broken in Kudu so we need
  # to clean up table-by-table. Once solved, delete this and rely on the overriden method.
  def cleanup_db(self, db_name):
    self.client.execute("use default")
    self.client.set_configuration({'sync_ddl': True})
    if db_name + "\t" in self.client.execute("show databases", ).data:
      # We use quoted identifiers to avoid name clashes with keywords
      for tbl_name in self.client.execute("show tables in `" + db_name + "`").data:
        full_tbl_name = '`%s`.`%s`' % (db_name, tbl_name)
        result = self.client.execute("describe formatted " + full_tbl_name)
        if 'VIRTUAL_VIEW' in '\n'.join(result.data):
          self.client.execute("drop view " + full_tbl_name)
        else:
          self.client.execute("drop table " + full_tbl_name)
      for fn_result in self.client.execute("show functions in `" + db_name + "`").data:
        # First column is the return type, second is the function signature
        fn_name = fn_result.split('\t')[1]
        self.client.execute("drop function `%s`.%s" % (db_name, fn_name))
      for fn_result in self.client.execute(\
        "show aggregate functions in `" + db_name + "`").data:
        fn_name = fn_result.split('\t')[1]
        self.client.execute("drop function `%s`.%s" % (db_name, fn_name))
      self.client.execute("drop database `" + db_name + "`")

  @pytest.mark.execute_serially
  def test_low_mem_limit_low_selectivity_scan(self, vector):
    """Tests that the queries specified in this test suite run under the given
    memory limits."""
    mem_limit = copy(vector.get_value('mem_limit'))
    exec_options = copy(vector.get_value('exec_option'))
    exec_options['mem_limit'] = "{0}m".format(mem_limit)
    for i, q in enumerate(self.QUERIES):
      try:
        self.execute_query(q, exec_options)
        pass
      except ImpalaBeeswaxException as e:
        if (mem_limit > self.QUERY_MEM_LIMITS[i]):
          raise
        assert "Memory limit exceeded" in str(e)
