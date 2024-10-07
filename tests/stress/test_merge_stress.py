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
from builtins import map, range
import pytest
import random
import time

from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.parametrize import UniqueDatabase
from tests.stress.stress_util import run_tasks, Task


# Stress test for concurrent MERGE operations against Iceberg tables.
class TestIcebergConcurrentMergeStress(ImpalaTestSuite):
  @classmethod
  def get_workload(self):
    return 'targeted-stress'

  @classmethod
  def add_test_dimensions(cls):
    super(TestIcebergConcurrentMergeStress, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_constraint(
        lambda v: (v.get_value('table_format').file_format == 'parquet'
            and v.get_value('table_format').compression_codec == 'snappy'))

  def _impala_role_concurrent_updater(self, tbl_name, col, num_writes):
    """Increments values in column 'total' and in the column which is passed in 'col'."""
    target_impalad = random.randint(0, ImpalaTestSuite.get_impalad_cluster_size() - 1)
    impalad_client = ImpalaTestSuite.create_client_for_nth_impalad(target_impalad)
    merge_stmt = """merge into {0} target using
        {0} source on source.total = target.total
        when matched then update set
        total = source.total + 1, {1} = source.{1} + 1""".format(tbl_name, col)
    update_count = 0
    while update_count < num_writes:
      try:
        impalad_client.execute(merge_stmt)
        update_count += 1
        # Sleep after a succesful operation.
        time.sleep(random.random())
      except Exception:
        # Exceptions are expected due to concurrent operations.
        pass
    impalad_client.close()

  def _impala_role_concurrent_writer(self, tbl_name, num_inserts):
    """Adds a new row based on the maximum 'total' value."""
    target_impalad = random.randint(0, ImpalaTestSuite.get_impalad_cluster_size() - 1)
    impalad_client = ImpalaTestSuite.create_client_for_nth_impalad(target_impalad)
    merge_stmt = """merge into {0} target using
        (select total, a, b, c from {0} order by total desc limit 1) source
        on source.total +1 = target.total
        when not matched then insert values
        (source.total + 1, source.a + 1, source.b, source.c)""".format(tbl_name)
    insert_count = 0
    while insert_count < num_inserts:
      try:
        impalad_client.execute(merge_stmt)
        insert_count += 1
        # Sleep after a succesful operation.
        time.sleep(random.random())
      except Exception:
        # Exceptions are expected due to concurrent operations.
        pass
    impalad_client.close()

  def _impala_role_concurrent_checker(self, tbl_name, target_total):
    """Checks if the table's invariant is true. The invariant is that the equation
    'total == a + b + c' is true. Returns 'total'."""
    def verify_result_set(result):
      max_total = 0
      for line in result.data:
        [total, a, b, c] = list(map(int, (line.split('\t'))))
        if max_total <= total:
          max_total = total
        assert total == a + b + c
      return max_total

    target_impalad = random.randint(0, ImpalaTestSuite.get_impalad_cluster_size() - 1)
    impalad_client = ImpalaTestSuite.create_client_for_nth_impalad(target_impalad)
    total = 0
    while total < target_total:
      result = impalad_client.execute("select * from %s" % tbl_name)
      total = verify_result_set(result)
      time.sleep(random.random())
    impalad_client.close()

  @pytest.mark.execute_serially
  @UniqueDatabase.parametrize(sync_ddl=True)
  def test_iceberg_merge_updates(self, unique_database):
    """Issues MERGE statements with UPDATE and INSERT clause against multiple
    impalads in a way that some invariants must be true when a spectator
    process inspects the table. E.g. the value of a column should be equal
    to the sum of other columns."""
    tbl_name = "%s.test_concurrent_merges" % unique_database
    self.client.set_configuration_option("SYNC_DDL", "true")
    self.client.execute("""create table {0}
        (total bigint, a bigint, b bigint, c bigint)
        stored as iceberg
        tblproperties('format-version'='2')""".format(tbl_name,))
    self.client.execute(
        "insert into {} values (0, 0, 0, 0)".format(tbl_name))

    num_checkers = 2
    cols = 3
    writes_per_col = 30
    target_total = writes_per_col * cols

    writer_a = Task(self._impala_role_concurrent_writer, tbl_name, writes_per_col)
    updater_b = Task(self._impala_role_concurrent_updater, tbl_name, "b", writes_per_col)
    updater_c = Task(self._impala_role_concurrent_updater, tbl_name, "c", writes_per_col)
    checkers = [Task(self._impala_role_concurrent_checker, tbl_name, target_total)
                for i in range(0, num_checkers)]
    run_tasks([writer_a, updater_b, updater_c] + checkers)
