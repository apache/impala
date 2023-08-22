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
from tests.common.skip import SkipIf, SkipIfHive2, SkipIfFS, SkipIfDockerizedCluster
from tests.stress.stress_util import Task, run_tasks
from tests.util.filesystem_utils import IS_OZONE

NUM_OVERWRITES = 2
NUM_INSERTS_PER_OVERWRITE = 4


class TestAcidStress(ImpalaTestSuite):
  @classmethod
  def get_workload(self):
    return 'targeted-stress'

  @classmethod
  def add_test_dimensions(cls):
    super(TestAcidStress, cls).add_test_dimensions()
    # Could be moved to exhaustive tests due to the long execution time, but this only
    # runs with Hive3, where the main goal currently is to make ACID work, so it is better
    # to run this frequently.
    cls.ImpalaTestMatrix.add_constraint(
        lambda v: (v.get_value('table_format').file_format == 'text' and
                   v.get_value('table_format').compression_codec == 'none'))


class TestAcidInsertsBasic(TestAcidStress):
  @classmethod
  def get_workload(self):
    return super(TestAcidInsertsBasic, self).get_workload()

  @classmethod
  def add_test_dimensions(cls):
    super(TestAcidInsertsBasic, cls).add_test_dimensions()

  def _verify_result(self, result, expected_result):
    """Verify invariants for 'run' and 'i'."""
    assert len(result.data) > 0
    run_max = -1
    i_list = []
    for line in result.data:
      [run, i] = list(map(int, (line.split('\t'))))
      run_max = max(run_max, run)
      i_list.append(i)
    assert expected_result["run"] <= run_max  # shouldn't see data overwritten in the past
    i_list.sort()
    if expected_result["run"] < run_max:
      expected_result["run"] = run_max
      expected_result["i"] = 0
      return
    assert i_list[-1] >= expected_result["i"]
    # 'i' should have all values from 0 to max_i
    assert i_list == list(range(i_list[-1] + 1))
    expected_result["i"] = i_list[-1]

  def _hive_role_write_inserts(self, tbl_name, partitioned):
    """INSERT INTO/OVERWRITE a table several times from Hive."""
    part_expr = "partition (p=1)" if partitioned else ""
    for run in range(0, NUM_OVERWRITES):
      OVERWRITE_SQL = """insert overwrite table %s %s values (%i, %i)
          """ % (tbl_name, part_expr, run, 0)
      self.run_stmt_in_hive(OVERWRITE_SQL)
      for i in range(1, NUM_INSERTS_PER_OVERWRITE + 1):
        INSERT_SQL = """insert into table %s %s values (%i, %i)
            """ % (tbl_name, part_expr, run, i)
        self.run_stmt_in_hive(INSERT_SQL)

  def _impala_role_write_inserts(self, tbl_name, partitioned):
    """INSERT INTO/OVERWRITE a table several times from Impala."""
    try:
      impalad_client = ImpalaTestSuite.create_impala_client()
      part_expr = "partition (p=1)" if partitioned else ""
      for run in range(0, NUM_OVERWRITES + 1):
        OVERWRITE_SQL = """insert overwrite table %s %s values (%i, %i)
            """ % (tbl_name, part_expr, run, 0)
        impalad_client.execute(OVERWRITE_SQL)
        for i in range(1, NUM_INSERTS_PER_OVERWRITE + 1):
          INSERT_SQL = """insert into table %s %s values (%i, %i)
              """ % (tbl_name, part_expr, run, i)
          impalad_client.execute(INSERT_SQL)
    finally:
      impalad_client.close()

  def _impala_role_read_inserts(self, tbl_name, needs_refresh, sleep_seconds):
    """SELECT from a table many times until the expected final values are found."""
    try:
      impalad_client = ImpalaTestSuite.create_impala_client()
      expected_result = {"run": -1, "i": 0}
      accept_empty_table = True
      while expected_result["run"] != NUM_OVERWRITES and \
          expected_result["i"] != NUM_INSERTS_PER_OVERWRITE:
        time.sleep(sleep_seconds)
        if needs_refresh: impalad_client.execute("refresh %s" % tbl_name)
        result = impalad_client.execute("select run, i from %s" % tbl_name)
        if len(result.data) == 0:
          assert accept_empty_table
          continue
        accept_empty_table = False
        self._verify_result(result, expected_result)
    finally:
      impalad_client.close()

  def _create_table(self, full_tbl_name, partitioned):
    """Creates test table with name 'full_tbl_name'. Table is partitioned if
    'partitioned' is set to True."""
    part_expr = "partitioned by (p int)" if partitioned else ""

    CREATE_SQL = """create table %s (run int, i int) %s TBLPROPERTIES (
         'transactional_properties' = 'insert_only', 'transactional' = 'true')
         """ % (full_tbl_name, part_expr)
    self.client.execute("drop table if exists %s" % full_tbl_name)
    self.client.execute(CREATE_SQL)

  def _run_test_read_hive_inserts(self, unique_database, partitioned):
    """Check that Impala can read a single insert only ACID table (over)written by Hive
    several times. Consistency can be checked by using incremental values for
    overwrites ('run') and inserts ('i').
    """
    tbl_name = "%s.test_read_hive_inserts" % unique_database
    self._create_table(tbl_name, partitioned)

    run_tasks([
        Task(self._hive_role_write_inserts, tbl_name, partitioned),
        Task(self._impala_role_read_inserts, tbl_name, needs_refresh=True,
           sleep_seconds=3)])

  def _run_test_read_impala_inserts(self, unique_database, partitioned):
    """Check that Impala can read a single insert only ACID table (over)written by Hive
    several times. Consistency can be checked by using incremental values for
    overwrites ('run') and inserts ('i').
    """
    tbl_name = "%s.test_read_impala_inserts" % unique_database
    self._create_table(tbl_name, partitioned)

    run_tasks([
        Task(self._impala_role_write_inserts, tbl_name, partitioned),
        Task(self._impala_role_read_inserts, tbl_name, needs_refresh=False,
           sleep_seconds=0.1)])

  @SkipIfHive2.acid
  @SkipIfFS.hive
  @pytest.mark.stress
  def test_read_hive_inserts(self, unique_database):
    """Check that Impala can read partitioned and non-partitioned ACID tables
    written by Hive."""
    for is_partitioned in [False, True]:
      self._run_test_read_hive_inserts(unique_database, is_partitioned)

  @SkipIfHive2.acid
  @pytest.mark.stress
  def test_read_impala_inserts(self, unique_database):
    """Check that Impala can read partitioned and non-partitioned ACID tables
    written by Hive."""
    for is_partitioned in [False, True]:
      self._run_test_read_impala_inserts(unique_database, is_partitioned)

  def _impala_role_partition_writer(self, tbl_name, partition, is_overwrite, sleep_sec):
    insert_op = "OVERWRITE" if is_overwrite else "INTO"
    try:
      impalad_client = ImpalaTestSuite.create_impala_client()
      impalad_client.execute(
          """insert {op} table {tbl_name} partition({partition})
             select sleep({sleep_ms})""".format(op=insert_op, tbl_name=tbl_name,
          partition=partition, sleep_ms=sleep_sec * 1000))
    finally:
      impalad_client.close()

  @pytest.mark.stress
  @SkipIf.not_dfs
  @UniqueDatabase.parametrize(sync_ddl=True)
  def test_partitioned_inserts(self, unique_database):
    """Check that the different ACID write operations take appropriate locks.
       INSERT INTO: should take a shared lock
       INSERT OVERWRITE: should take an exclusive lock
       Both should take PARTITION-level lock in case of static partition insert."""
    tbl_name = "%s.test_concurrent_partitioned_inserts" % unique_database
    self.client.set_configuration_option("SYNC_DDL", "true")
    self.client.execute("""
        CREATE TABLE {0} (i int) PARTITIONED BY (p INT, q INT)
        TBLPROPERTIES(
        'transactional_properties'='insert_only','transactional'='true')""".format(
        tbl_name))
    # Warmup INSERT
    self.execute_query("alter table {0} add partition(p=0,q=0)".format(tbl_name))
    sleep_sec = 5
    task_insert_into = Task(self._impala_role_partition_writer, tbl_name,
        "p=1,q=1", False, sleep_sec)
    # INSERT INTO the same partition can run in parallel.
    duration = run_tasks([task_insert_into, task_insert_into])
    assert duration < 3 * sleep_sec
    task_insert_overwrite = Task(self._impala_role_partition_writer, tbl_name,
      "p=1,q=1", True, sleep_sec)
    # INSERT INTO + INSERT OVERWRITE should have mutual exclusion.
    duration = run_tasks([task_insert_into, task_insert_overwrite])
    assert duration > 4 * sleep_sec
    # INSERT OVERWRITEs to the same partition should have mutual exclusion.
    duration = run_tasks([task_insert_overwrite, task_insert_overwrite])
    assert duration > 4 * sleep_sec
    task_insert_overwrite_2 = Task(self._impala_role_partition_writer, tbl_name,
      "p=1,q=2", True, sleep_sec)
    # INSERT OVERWRITEs to different partitions can run in parallel.
    duration = run_tasks([task_insert_overwrite, task_insert_overwrite_2])
    assert duration < 3 * sleep_sec


class TestConcurrentAcidInserts(TestAcidStress):
  @classmethod
  def get_workload(self):
    return super(TestConcurrentAcidInserts, self).get_workload()

  @classmethod
  def add_test_dimensions(cls):
    super(TestConcurrentAcidInserts, cls).add_test_dimensions()

  def _impala_role_concurrent_writer(self, tbl_name, wid, counter):
    """Writes ascending numbers into column 'i'. To column 'wid' it writes its identifier
    passed in parameter 'wid'. Occasionally it truncates the table."""
    target_impalad = wid % ImpalaTestSuite.get_impalad_cluster_size()
    impalad_client = ImpalaTestSuite.create_client_for_nth_impalad(target_impalad)
    try:
      num_inserts = 0
      while num_inserts < 50:
        impalad_client.execute("insert into table %s values (%i, %i)" % (
            tbl_name, wid, num_inserts))
        num_inserts += 1
        if random.randint(0, 100) < 5:
          impalad_client.execute("truncate table %s" % tbl_name)
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
        time.sleep(0.5)
    finally:
      impalad_client.close()

  @SkipIfFS.stress_insert_timeouts
  @SkipIfHive2.acid
  @SkipIfDockerizedCluster.jira(reason="IMPALA-11189")
  @pytest.mark.stress
  @UniqueDatabase.parametrize(sync_ddl=True)
  def test_concurrent_inserts(self, unique_database):
    """Issues INSERT statements against multiple impalads in a way that some
    invariants must be true when a spectator process inspects the table. E.g.
    if the table contains continuous ranges of integers."""
    tbl_name = "%s.test_concurrent_inserts" % unique_database
    self.client.set_configuration_option("SYNC_DDL", "true")
    self.client.execute("drop table if exists %s" % tbl_name)
    self.client.execute("""create table %s (wid int, i int) TBLPROPERTIES (
        'transactional_properties' = 'insert_only', 'transactional' = 'true')
        """ % tbl_name)

    counter = Value('i', 0)
    num_writers = 6
    num_checkers = 3

    writers = [Task(self._impala_role_concurrent_writer, tbl_name, i, counter)
               for i in range(0, num_writers)]
    checkers = [Task(self._impala_role_concurrent_checker, tbl_name, i, counter,
                     num_writers)
                for i in range(0, num_checkers)]
    # HDDS-8289: Ozone listStatus is slow with lots of files
    timeout = 900 if IS_OZONE else 600
    run_tasks(writers + checkers, timeout_seconds=timeout)


class TestFailingAcidInserts(TestAcidStress):
  @classmethod
  def get_workload(self):
    return super(TestFailingAcidInserts, self).get_workload()

  @classmethod
  def add_test_dimensions(cls):
    super(TestFailingAcidInserts, cls).add_test_dimensions()

  def _impala_role_insert(self, tbl_name, partitioned, target_impalad, counter):
    """Inserts data to table 'tbl_name'. INSERTs with the value 1 should succeed, while
    INSERTs with the value -1 must fail with a debug action.
    Occasionally it truncates the table."""
    FAIL_ACTION = "CLIENT_REQUEST_UPDATE_CATALOG:FAIL@1.0"
    impalad_client = ImpalaTestSuite.create_client_for_nth_impalad(target_impalad)
    try:
      num_inserts = 0
      while num_inserts < 50:
        try:
          should_succeed = random.randint(0, 99) < 50
          val = 1 if should_succeed else -1
          impalad_client.set_configuration_option(
              "DEBUG_ACTION", "" if should_succeed else FAIL_ACTION)
          part_expr = "partition (p=%d)" % random.randint(0, 3) if partitioned else ""
          impalad_client.execute(
              "insert into table %s %s values (%i)" % (tbl_name, part_expr, val))
          num_inserts += 1
          if random.randint(0, 100) < 5:
            impalad_client.execute("truncate table %s" % tbl_name)
        except Exception as e:
          if should_succeed or "CLIENT_REQUEST_UPDATE_CATALOG" not in str(e):
            raise e
    finally:
      with counter.get_lock():
        counter.value += 1
      impalad_client.close()

  def _impala_role_checker(self, tbl_name, target_impalad, counter, writers):
    """Checks that the table doesn't contain other values than 1."""
    impalad_client = ImpalaTestSuite.create_client_for_nth_impalad(target_impalad)
    try:
      while counter.value != writers:
        result = impalad_client.execute("select * from %s where i != 1" % tbl_name)
        assert len(result.data) == 0
        time.sleep(0.5)
    finally:
      impalad_client.close()

  def _run_test_failing_inserts(self, unique_database, partitioned):
    """Tests that failing INSERTs cannot be observed."""
    tbl_name = "%s.test_inserts_fail" % unique_database
    self.client.set_configuration_option("SYNC_DDL", "true")
    self.client.execute("drop table if exists %s" % tbl_name)
    part_expr = "partitioned by (p int)" if partitioned else ""
    self.client.execute("""create table %s (i int) %s TBLPROPERTIES (
        'transactional_properties' = 'insert_only', 'transactional' = 'true')
        """ % (tbl_name, part_expr))

    counter = Value('i', 0)
    num_writers = 3
    num_checkers = 3

    writers = [Task(self._impala_role_insert, tbl_name, partitioned, i, counter)
               for i in range(0, num_writers)]
    checkers = [Task(self._impala_role_checker, tbl_name, i, counter, num_writers)
                for i in range(0, num_checkers)]
    run_tasks(writers + checkers)

  @SkipIfFS.stress_insert_timeouts
  @SkipIfDockerizedCluster.jira(reason="IMPALA-11191")
  @SkipIfHive2.acid
  @pytest.mark.stress
  @UniqueDatabase.parametrize(sync_ddl=True)
  def test_failing_inserts(self, unique_database):
    """Tests that failing INSERTs cannot be observed."""
    for is_partitioned in [False, True]:
      self._run_test_failing_inserts(unique_database, is_partitioned)
