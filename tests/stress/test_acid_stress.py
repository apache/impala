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
import time
import traceback

from multiprocessing.pool import ThreadPool

from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.skip import SkipIfHive2

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

  def __verify_result(self, result, expected_result):
    """Verify invariants for 'run' and 'i'."""
    assert len(result.data) > 0
    run_max = -1
    i_list = []
    for line in result.data:
      [run, i] = map(int, (line.split('\t')))
      run_max = max(run_max, run)
      i_list.append(i)
    assert expected_result["run"] <= run_max  # shouldn't see data overwritten in the past
    i_list.sort()
    if expected_result["run"] < run_max:
      expected_result["run"] = run_max
      expected_result["i"] = 0
      return
    assert i_list[-1] >= expected_result["i"]
    assert i_list == range(i_list[-1] + 1)  # 'i' should have all values from 0 to max_i
    expected_result["i"] = i_list[-1]

  def __hive_role_write_hive_inserts(self, tbl_name, partitioned):
    """INSERT INTO/OVERWRITE a table several times from Hive."""
    part_expr = "partition (p=1)" if partitioned else ""
    for run in xrange(0, NUM_OVERWRITES):
      OVERWRITE_SQL = """insert overwrite table %s %s values (%i, %i)
          """ % (tbl_name, part_expr, run, 0)
      self.run_stmt_in_hive(OVERWRITE_SQL)
      for i in xrange(1, NUM_INSERTS_PER_OVERWRITE + 1):
        INSERT_SQL = """insert into table %s %s values (%i, %i)
            """ % (tbl_name, part_expr, run, i)
        self.run_stmt_in_hive(INSERT_SQL)

  def __impala_role_read_hive_inserts(self, tbl_name):
    """SELECT from a table many times until the expected final values are found."""
    expected_result = {"run": -1, "i": 0}
    accept_empty_table = True
    while expected_result["run"] != NUM_OVERWRITES and \
        expected_result["i"] != NUM_INSERTS_PER_OVERWRITE - 1:
      time.sleep(3)  # Hive inserts usually take a few seconds
      self.client.execute("refresh %s" % tbl_name)
      result = self.client.execute("select run, i from %s" % tbl_name)
      if len(result.data) == 0:
        assert accept_empty_table
        continue
      accept_empty_table = False
      self.__verify_result(result, expected_result)

  def __run_test_read_hive_inserts(self, unique_database, partitioned):
    """Check that Impala can read a single insert only ACID table (over)written by Hive
    several times. Consistency can be checked by using incremental values for
    overwrites ('run') and inserts ('i').
    """
    tbl_name = "%s.test_read_hive_inserts" % unique_database
    part_expr = "partitioned by (p int)" if partitioned else ""

    CREATE_SQL = """create table %s (run int, i int) %s TBLPROPERTIES (
         'transactional_properties' = 'insert_only', 'transactional' = 'true')
         """ % (tbl_name, part_expr)
    self.client.execute(CREATE_SQL)

    def do_role(role):
      try:
        if role == "hive":
          self.__hive_role_write_hive_inserts(tbl_name, partitioned)
        else:
          self.__impala_role_read_hive_inserts(tbl_name)
      except Exception:
        traceback.print_exc()
        raise

    # TODO: CTRL+C can't interrupt the test
    pool = ThreadPool(processes=2)
    pool.map_async(do_role, ["impala", "hive"]).get(600)

  @SkipIfHive2.acid
  @pytest.mark.stress
  def test_read_hive_inserts_non_partitioned(self, unique_database):
    """Check that Impala can read a non-partitioned ACID table written by Hive."""
    self.__run_test_read_hive_inserts(unique_database, False)

  @SkipIfHive2.acid
  @pytest.mark.stress
  def test_read_hive_inserts_partitioned(self, unique_database):
    """Check that Impala can read a partitioned ACID table written by Hive.
    Currently only writes a single partition.
    """
    self.__run_test_read_hive_inserts(unique_database, True)
