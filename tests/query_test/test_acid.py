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

# Functional tests for ACID integration with Hive.

import pytest
import time

from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.skip import (SkipIfHive2, SkipIfCatalogV2, SkipIfS3, SkipIfABFS,
                               SkipIfADLS, SkipIfIsilon, SkipIfLocal)
from tests.common.test_dimensions import create_single_exec_option_dimension
class TestAcid(ImpalaTestSuite):
  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestAcid, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(create_single_exec_option_dimension())

    # TODO(todd) consider running on other formats
    cls.ImpalaTestMatrix.add_constraint(lambda v:
        v.get_value('table_format').file_format in ['text'])

  @SkipIfHive2.acid
  @SkipIfS3.hive
  @SkipIfABFS.hive
  @SkipIfADLS.hive
  @SkipIfIsilon.hive
  @SkipIfLocal.hive
  def test_acid_basic(self, vector, unique_database):
    self.run_test_case('QueryTest/acid', vector, use_db=unique_database)

  @SkipIfHive2.acid
  @SkipIfS3.hive
  @SkipIfABFS.hive
  @SkipIfADLS.hive
  @SkipIfIsilon.hive
  @SkipIfLocal.hive
  def test_acid_compaction(self, vector, unique_database):
    self.run_test_case('QueryTest/acid-compaction', vector, use_db=unique_database)

  @SkipIfHive2.acid
  def test_acid_negative(self, vector, unique_database):
    self.run_test_case('QueryTest/acid-negative', vector, use_db=unique_database)

  @SkipIfHive2.acid
  @SkipIfS3.hive
  @SkipIfABFS.hive
  @SkipIfADLS.hive
  @SkipIfIsilon.hive
  @SkipIfLocal.hive
  def test_acid_partitioned(self, vector, unique_database):
    self.run_test_case('QueryTest/acid-partitioned', vector, use_db=unique_database)

  # When local CatalogV2 combines with hms_enent_polling enabled, it seems
  # that Catalog loads tables by itself, the query statement cannot trigger
  # loading tables. As the ValidWriteIdlists is part of table loading profile,
  # it can not be shown in the query profile.  Skip CatalogV2 to avoid flaky tests.
  @SkipIfHive2.acid
  @SkipIfCatalogV2.hms_event_polling_enabled()
  @SkipIfS3.hive
  @SkipIfABFS.hive
  @SkipIfADLS.hive
  @SkipIfIsilon.hive
  @SkipIfLocal.hive
  def test_acid_profile(self, vector, unique_database):
    self.run_test_case('QueryTest/acid-profile', vector, use_db=unique_database)

  @SkipIfHive2.acid
  @SkipIfS3.hive
  @SkipIfABFS.hive
  @SkipIfADLS.hive
  @SkipIfIsilon.hive
  @SkipIfLocal.hive
  def test_acid_insert_statschg(self, vector, unique_database):
    self.run_test_case('QueryTest/acid-clear-statsaccurate',
        vector, use_db=unique_database)
    result = self.run_stmt_in_hive("select count(*) from {0}.{1}".format(unique_database,
        "insertonly_nopart_colstatschg"))
    # The return from hive should look like '_c0\n2\n'
    assert "2" in result
    result = self.run_stmt_in_hive("select count(*) from {0}.{1} where ds='2010-01-01'"
        .format(unique_database, "insertonly_part_colstats"))
    assert "2" in result

  @SkipIfS3.hive
  @SkipIfABFS.hive
  @SkipIfADLS.hive
  @SkipIfIsilon.hive
  @SkipIfLocal.hive
  def test_ext_statschg(self, vector, unique_database):
    self.run_test_case('QueryTest/clear-statsaccurate',
        vector, use_db=unique_database)
    result = self.run_stmt_in_hive("select count(*) from {0}.{1}".format(unique_database,
        "ext_nopart_colstatschg"))
    # Hive should return correct row count after Impala insert.
    # The return from hive should look like '_c0\n2\n'
    assert "2" in result
    result = self.run_stmt_in_hive("select count(*) from {0}.{1} where ds='2010-01-01'"
        .format(unique_database, "ext_part_colstats"))
    assert "2" in result

#  TODO(todd): further tests to write:
#  TRUNCATE, once HIVE-20137 is implemented.
#  INSERT OVERWRITE with empty result set, once HIVE-21750 is fixed.
#  Negative test for LOAD DATA INPATH and all other SQL that we don't support.

  @SkipIfHive2.acid
  @SkipIfS3.hive
  @SkipIfABFS.hive
  @SkipIfADLS.hive
  @SkipIfIsilon.hive
  @SkipIfLocal.hive
  @pytest.mark.execute_serially
  def test_acid_heartbeats(self, vector, unique_database):
    """Tests heartbeating of transactions. Creates a long-running query via
    some jitting and in the meanwhile it periodically checks whether there is
    a transaction that has sent a heartbeat since its start.
    """
    if self.exploration_strategy() != 'exhaustive': pytest.skip()
    last_open_txn_start_time = self._latest_open_transaction()
    dummy_tbl = "{}.{}".format(unique_database, "dummy")
    self.execute_query("create table {} (i int) tblproperties"
                       "('transactional'='true',"
                       "'transactional_properties'='insert_only')".format(dummy_tbl))
    try:
      handle = self.execute_query_async(
          "insert into {} values (sleep(200000))".format(dummy_tbl))
      MAX_ATTEMPTS = 10
      attempt = 0
      success = False
      while attempt < MAX_ATTEMPTS:
        if self._any_open_heartbeated_transaction_since(last_open_txn_start_time):
          success = True
          break
        attempt += 1
        time.sleep(20)
      assert success
    finally:
      self.client.cancel(handle)

  def _latest_open_transaction(self):
    max_start = 0
    for txn in self._get_impala_transactions():
      start = txn['start_time']
      if start > max_start:
        max_start = start
    return max_start

  def _any_open_heartbeated_transaction_since(self, since_start_time):
    for txn in self._get_impala_transactions():
      if txn['state'] == 'OPEN':
        start = txn['start_time']
        if start > since_start_time and start != txn['last_heartbeat']:
          return True
    return False

  def _get_impala_transactions(self):
    transactions = self.run_stmt_in_hive("SHOW TRANSACTIONS")
    for transaction_line in transactions.split('\n')[2:-1]:
      transaction_columns = transaction_line.split(',')
      txn_dict = dict()
      txn_dict['state'] = transaction_columns[1]
      txn_dict['start_time'] = int(transaction_columns[2])
      txn_dict['last_heartbeat'] = int(transaction_columns[3])
      txn_dict['user'] = transaction_columns[4]
      if txn_dict['user'] != 'Impala':
        continue
      yield txn_dict
