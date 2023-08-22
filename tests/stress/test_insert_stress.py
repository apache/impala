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

from multiprocessing import Value

from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.parametrize import UniqueDatabase
from tests.common.skip import SkipIf
from tests.stress.stress_util import run_tasks, Task
from tests.util.filesystem_utils import WAREHOUSE


# Stress test for concurrent INSERT operations.
class TestInsertStress(ImpalaTestSuite):
  @classmethod
  def get_workload(self):
    return 'targeted-stress'

  @classmethod
  def add_test_dimensions(cls):
    super(TestInsertStress, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_constraint(
        lambda v: (v.get_value('table_format').file_format == 'parquet' and
                   v.get_value('table_format').compression_codec == 'snappy'))

  def _impala_role_concurrent_writer(self, tbl_name, wid, num_inserts, counter):
    """Writes ascending numbers up to 'num_inserts' into column 'i'. To column 'wid' it
    writes its identifier passed in parameter 'wid'."""
    target_impalad = wid % ImpalaTestSuite.get_impalad_cluster_size()
    impalad_client = ImpalaTestSuite.create_client_for_nth_impalad(target_impalad)
    try:
      insert_cnt = 0
      while insert_cnt < num_inserts:
        impalad_client.execute("insert into table %s values (%i, %i)" % (
            tbl_name, wid, insert_cnt))
        insert_cnt += 1
    finally:
      with counter.get_lock():
        counter.value += 1
      impalad_client.close()

  def _impala_role_concurrent_checker(self, tbl_name, cid, counter, writers):
    """Checks if the table's invariant is true. The invariant is that for each
    'wid' column 'i' should store a continuous integer range."""
    def verify_result_set(result):
      wid_to_run = dict()
      for line in result.data:
        [wid, i] = list(map(int, (line.split('\t'))))
        wid_to_run.setdefault(wid, []).append(i)
      for wid, run in wid_to_run.items():
        sorted_run = sorted(run)
        assert sorted_run == list(range(sorted_run[0], sorted_run[-1] + 1)), \
          "wid: %d" % wid

    target_impalad = cid % ImpalaTestSuite.get_impalad_cluster_size()
    impalad_client = ImpalaTestSuite.create_client_for_nth_impalad(target_impalad)
    try:
      while counter.value != writers:
        result = impalad_client.execute("select * from %s" % tbl_name)
        verify_result_set(result)
        time.sleep(random.random())
    finally:
      impalad_client.close()

  @pytest.mark.stress
  @UniqueDatabase.parametrize(sync_ddl=True)
  def test_inserts(self, unique_database):
    """Issues INSERT statements against multiple impalads in a way that some
    invariants must be true when a spectator process inspects the table. E.g.
    if the table contains continuous ranges of integers."""
    tbl_name = "%s.test_concurrent_inserts" % unique_database
    self.client.set_configuration_option("SYNC_DDL", "true")
    self.client.execute("""create table {0} (wid int, i int)""".format(tbl_name))

    counter = Value('i', 0)
    num_writers = 16
    num_checkers = 4
    inserts = 50

    writers = [Task(self._impala_role_concurrent_writer, tbl_name, i, inserts, counter)
               for i in range(0, num_writers)]
    checkers = [Task(self._impala_role_concurrent_checker, tbl_name, i, counter,
                     num_writers)
                for i in range(0, num_checkers)]
    run_tasks(writers + checkers)

  @pytest.mark.stress
  @SkipIf.not_dfs
  @UniqueDatabase.parametrize(sync_ddl=True)
  def test_iceberg_inserts(self, unique_database):
    """Issues INSERT statements against multiple impalads in a way that some
    invariants must be true when a spectator process inspects the table. E.g.
    if the table contains continuous ranges of integers."""
    tbl_name = "%s.test_concurrent_inserts" % unique_database
    self.client.set_configuration_option("SYNC_DDL", "true")
    self.client.execute("""create table {0} (wid int, i int) stored as iceberg
        tblproperties('iceberg.catalog'='hadoop.catalog',
                      'iceberg.catalog_location'='{1}')""".format(
        tbl_name, "{0}/{1}".format(WAREHOUSE, unique_database)))

    counter = Value('i', 0)
    num_writers = 4
    num_checkers = 2
    inserts = 30

    writers = [Task(self._impala_role_concurrent_writer, tbl_name, i, inserts, counter)
               for i in range(0, num_writers)]
    checkers = [Task(self._impala_role_concurrent_checker, tbl_name, i, counter,
                     num_writers)
                for i in range(0, num_checkers)]
    run_tasks(writers + checkers)
