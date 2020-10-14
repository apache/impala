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

import pytest
import random
import time

from multiprocessing import Value

from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.parametrize import UniqueDatabase
from tests.stress.stress_util import run_tasks, Task


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

  def _impala_role_concurrent_writer(self, tbl_name, wid, counter):
    """Writes ascending numbers into column 'i'. To column 'wid' it writes its identifier
    passed in parameter 'wid'."""
    target_impalad = wid % ImpalaTestSuite.get_impalad_cluster_size()
    impalad_client = ImpalaTestSuite.create_client_for_nth_impalad(target_impalad)
    try:
      num_inserts = 0
      while num_inserts < 50:
        impalad_client.execute("insert into table %s values (%i, %i)" % (
            tbl_name, wid, num_inserts))
        num_inserts += 1
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
        [wid, i] = map(int, (line.split('\t')))
        wid_to_run.setdefault(wid, []).append(i)
      for wid, run in wid_to_run.items():
        sorted_run = sorted(run)
        assert sorted_run == range(sorted_run[0], sorted_run[-1] + 1), "wid: %d" % wid

    target_impalad = cid % ImpalaTestSuite.get_impalad_cluster_size()
    impalad_client = ImpalaTestSuite.create_client_for_nth_impalad(target_impalad)
    try:
      while counter.value != writers:
        result = impalad_client.execute("select * from %s" % tbl_name)
        verify_result_set(result)
        time.sleep(random.random())
    finally:
      impalad_client.close()

  @pytest.mark.execute_serially
  @pytest.mark.stress
  @UniqueDatabase.parametrize(sync_ddl=True)
  def test_inserts(self, unique_database):
    """Issues INSERT statements against multiple impalads in a way that some
    invariants must be true when a spectator process inspects the table. E.g.
    if the table contains continuous ranges of integers."""
    tbl_name = "%s.test_concurrent_inserts" % unique_database
    self.client.set_configuration_option("SYNC_DDL", "true")
    self.client.execute("drop table if exists %s" % tbl_name)
    self.client.execute("""create table {0} (wid int, i int)""".format(tbl_name))

    counter = Value('i', 0)
    num_writers = 16
    num_checkers = 4

    writers = [Task(self._impala_role_concurrent_writer, tbl_name, i, counter)
               for i in xrange(0, num_writers)]
    checkers = [Task(self._impala_role_concurrent_checker, tbl_name, i, counter,
                     num_writers)
                for i in xrange(0, num_checkers)]
    run_tasks(writers + checkers)
