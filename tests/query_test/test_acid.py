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

from __future__ import absolute_import, division, print_function
import os
import pytest
import time

from hive_metastore.ttypes import CommitTxnRequest, LockType, OpenTxnRequest
from subprocess import check_call
from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.skip import SkipIf, SkipIfHive2, SkipIfCatalogV2, SkipIfFS
from tests.common.test_dimensions import create_single_exec_option_dimension
from tests.util.acid_txn import AcidTxn


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
  @SkipIfFS.hive
  def test_acid_basic(self, vector, unique_database):
    self.run_test_case('QueryTest/acid', vector, use_db=unique_database)

  @SkipIfHive2.acid
  def test_acid_no_hive(self, vector, unique_database):
    """ Run tests that do not need a running Hive server. This means that (unlike other
    tests) these can be run in enviroments without Hive, e.g. S3.
    TODO: find a long term solution to run much more ACID tests in S3
    """
    self.run_test_case('QueryTest/acid-no-hive', vector, use_db=unique_database)

  @SkipIfHive2.acid
  @SkipIfFS.hive
  def test_acid_compaction(self, vector, unique_database):
    self.run_test_case('QueryTest/acid-compaction', vector, use_db=unique_database)

  @SkipIfHive2.acid
  @SkipIfFS.hive
  def test_acid_negative(self, vector, unique_database):
    self.run_test_case('QueryTest/acid-negative', vector, use_db=unique_database)

  @SkipIfHive2.acid
  @SkipIfFS.hive
  def test_acid_truncate(self, vector, unique_database):
    self.run_test_case('QueryTest/acid-truncate', vector, use_db=unique_database)
    assert "0" == self.run_stmt_in_hive("select count(*) from {0}.{1}".format(
        unique_database, "pt")).split("\n")[1]

  @SkipIfHive2.acid
  @SkipIfFS.hive
  def test_acid_partitioned(self, vector, unique_database):
    self.run_test_case('QueryTest/acid-partitioned', vector, use_db=unique_database)

  @SkipIfHive2.acid
  @SkipIfFS.hive
  def test_full_acid_scans(self, vector, unique_database):
    self.run_test_case('QueryTest/full-acid-scans', vector, use_db=unique_database)

  @SkipIfHive2.acid
  def test_full_acid_complex_type_scans(self, vector, unique_database):
    self.run_test_case('QueryTest/full-acid-complex-type-scans', vector,
        use_db='functional_orc_def')

  # When local CatalogV2 combines with hms_enent_polling enabled, it seems
  # that Catalog loads tables by itself, the query statement cannot trigger
  # loading tables. As the ValidWriteIdlists is part of table loading profile,
  # it can not be shown in the query profile.  Skip CatalogV2 to avoid flaky tests.
  @SkipIfHive2.acid
  @SkipIfCatalogV2.hms_event_polling_enabled()
  @SkipIfFS.hive
  def test_acid_profile(self, vector, unique_database):
    self.run_test_case('QueryTest/acid-profile', vector, use_db=unique_database)

  @SkipIfHive2.acid
  def test_full_acid_rowid(self, vector, unique_database):
    self.run_test_case('QueryTest/full-acid-rowid', vector, use_db=unique_database)

  @SkipIfHive2.acid
  @SkipIfFS.hive
  def test_full_acid_original_files(self, vector, unique_database):
    table_name = "alltypes_promoted_nopart"
    fq_table_name = "{0}.{1}".format(unique_database, table_name)
    self.client.execute("""CREATE TABLE {0} (
          id INT, bool_col BOOLEAN, tinyint_col TINYINT, smallint_col SMALLINT,
          int_col INT, bigint_col BIGINT, float_col FLOAT, double_col DOUBLE,
          date_string_col STRING, string_col STRING, timestamp_col TIMESTAMP,
          year INT, month INT) STORED AS ORC""".format(fq_table_name))
    table_uri = self._get_table_location(fq_table_name, vector)
    original_file = os.environ['IMPALA_HOME'] + "/testdata/data/alltypes_non_acid.orc"
    self.hdfs_client.copy_from_local(original_file, table_uri + "/000000_0")
    self.run_stmt_in_hive("""alter table {0}.{1}
        set tblproperties('EXTERNAL'='FALSE','transactional'='true')""".format(
        unique_database, table_name))
    self.run_test_case('QueryTest/full-acid-original-file', vector, unique_database)

  @SkipIfHive2.acid
  @SkipIfFS.hive
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

  @SkipIfFS.hive
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

  @SkipIfHive2.acid
  @SkipIfFS.hive
  def test_acid_compute_stats(self, vector, unique_database):
    self.run_test_case('QueryTest/acid-compute-stats', vector, use_db=unique_database)

#  TODO(todd): further tests to write:
#  TRUNCATE, once HIVE-20137 is implemented.
#  INSERT OVERWRITE with empty result set, once HIVE-21750 is fixed.
#  Negative test for LOAD DATA INPATH and all other SQL that we don't support.

  @SkipIfHive2.acid
  @SkipIfFS.hive
  def test_acid_heartbeats(self, vector, unique_database):
    """Tests heartbeating of transactions. Creates a long-running query via
    some jitting and in the meanwhile it periodically checks whether there is
    a transaction that has sent a heartbeat since its start.
    """
    if self.exploration_strategy() != 'exhaustive': pytest.skip()
    table_format = vector.get_value('table_format')
    if table_format.compression_codec != 'none': pytest.skip()

    last_open_txn_start_time = self._latest_open_transaction()
    dummy_tbl = "{}.{}".format(unique_database, "dummy")
    self.execute_query("create table {} (i int) tblproperties"
                       "('transactional'='true',"
                       "'transactional_properties'='insert_only')".format(dummy_tbl))
    try:
      handle = self.execute_query_async(
          "insert into {} values (sleep(320000))".format(dummy_tbl))
      MAX_ATTEMPTS = 16
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

  def _open_txn(self):
     open_txn_req = OpenTxnRequest()
     open_txn_req.num_txns = 1
     open_txn_req.user = "AcidTest"
     open_txn_req.hostname = "localhost"
     open_txn_resp = self.hive_client.open_txns(open_txn_req)
     return open_txn_resp.txn_ids[0]

  def _commit_txn(self, txn_id):
    commit_req = CommitTxnRequest()
    commit_req.txnid = txn_id
    return self.hive_client.commit_txn(commit_req)

  @SkipIfFS.hive
  def test_lock_timings(self, vector, unique_database):
    def elapsed_time_for_query(query):
      t_start = time.time()
      self.execute_query_expect_failure(self.client, query)
      return time.time() - t_start

    tbl_name = "test_lock"
    tbl = "{0}.{1}".format(unique_database, tbl_name)
    self.execute_query("create table {} (i int) tblproperties"
        "('transactional'='true',"
        "'transactional_properties'='insert_only')".format(tbl))
    acid_util = AcidTxn(self.hive_client)
    lock_resp = acid_util.lock(0, unique_database, tbl_name, LockType.EXCLUSIVE)
    try:
      if self.exploration_strategy() == 'exhaustive':
        elapsed = elapsed_time_for_query("insert into {} values (1)".format(tbl))
        assert elapsed > 300 and elapsed < 310
      self.execute_query("set lock_max_wait_time_s=20")
      elapsed = elapsed_time_for_query("insert into {} values (1)".format(tbl))
      assert elapsed > 20 and elapsed < 28

      self.execute_query("set lock_max_wait_time_s=0")
      elapsed = elapsed_time_for_query("insert into {} values (1)".format(tbl))
      assert elapsed < 8

      self.execute_query("set lock_max_wait_time_s=10")
      elapsed = elapsed_time_for_query("insert into {} values (1)".format(tbl))
      assert elapsed > 10 and elapsed < 18

      self.execute_query("set lock_max_wait_time_s=2")
      elapsed = elapsed_time_for_query("truncate table {}".format(tbl))
      assert elapsed > 2 and elapsed < 10

      self.execute_query("set lock_max_wait_time_s=5")
      elapsed = elapsed_time_for_query("drop table {}".format(tbl))
      assert elapsed > 5 and elapsed < 13
    finally:
      acid_util.unlock(lock_resp.lockid)

  @SkipIfHive2.acid
  @SkipIfFS.hive
  def test_in_progress_compactions(self, vector, unique_database):
    """Checks that in-progress compactions are not visible. The test mimics
    in-progress compactions by opening a transaction and creating a new base
    directory. The new base directory is empty and must not have an effect
    on query results until the transaction is committed."""
    tbl_name = "{}.{}".format(unique_database, "test_compaction")
    self.execute_query("create table {} (i int) tblproperties"
        "('transactional'='true',"
        "'transactional_properties'='insert_only')".format(tbl_name))
    self.execute_query("insert into {} values (1)".format(tbl_name))

    # Create new base directory with a valid write id.
    txn_id = self._open_txn()
    tbl_file = self.execute_query(
        "show files in {}".format(tbl_name)).data[0].split("\t")[0]
    tbl_dir = tbl_file[tbl_file.find("/test-warehouse"):tbl_file.rfind("delta_")]
    new_base_dir_with_old_write_id = tbl_dir + "base_1_v" + str(txn_id)
    check_call(['hdfs', 'dfs', '-mkdir', '-p', new_base_dir_with_old_write_id])

    # Transaction is not committed so the new empty base directory must not have
    # any effect on query results.
    self.execute_query("refresh {}".format(tbl_name))
    assert len(self.execute_query("select * from {}".format(tbl_name)).data) != 0

    # Transaction is committed, now the query should see the table as empty. Of course,
    # real compactions don't remove data, but that verifies that the query reads the
    # new directory.
    self._commit_txn(txn_id)
    self.execute_query("refresh {}".format(tbl_name))
    assert len(self.execute_query("select * from {}".format(tbl_name)).data) == 0

  @SkipIfHive2.acid
  @SkipIf.not_dfs
  def test_full_acid_schema_without_file_metadata_tag(self, vector, unique_database):
    """IMPALA-10115: Some files have full ACID schema without having
    'hive.acid.version' set. We still need to identify such files as full ACID"""
    table_name = "full_acid_schema_no_metadata"
    fq_table_name = "{0}.{1}".format(unique_database, table_name)
    self.client.execute("""CREATE TABLE {0} (i int) STORED AS ORC
        TBLPROPERTIES('transactional'='true')""".format(fq_table_name))
    table_uri = self._get_table_location(fq_table_name, vector)
    acid_file = (os.environ['IMPALA_HOME'] +
        "/testdata/data/full_acid_schema_but_no_acid_version.orc")
    self.filesystem_client.copy_from_local(acid_file, table_uri + "/bucket_00000")
    self.execute_query("refresh {}".format(fq_table_name))
    result = self.execute_query("select count(*) from {0}".format(fq_table_name))
    assert "3" in result.data

  def test_add_partition_write_id(self, vector, unique_database):
    """Test that ALTER TABLE ADD PARTITION increases the write id of the table."""
    # Test INSERT-only table
    io_tbl_name = "insert_only_table"
    self.client.execute("""CREATE TABLE {0}.{1} (i int) PARTITIONED BY (p int)
        TBLPROPERTIES('transactional'='true', 'transactional_properties'='insert_only')
        """.format(unique_database, io_tbl_name))
    self._check_add_partition_write_id_change(unique_database, io_tbl_name)

    # Test Full ACID table
    full_acid_name = "full_acid_table"
    self.client.execute("""CREATE TABLE {0}.{1} (i int) PARTITIONED BY (p int)
        STORED AS ORC TBLPROPERTIES('transactional'='true')
        """.format(unique_database, full_acid_name))
    self._check_add_partition_write_id_change(unique_database, full_acid_name)

  def _check_add_partition_write_id_change(self, db_name, tbl_name):
    acid_util = AcidTxn(self.hive_client)
    valid_write_ids = acid_util.get_valid_write_ids(db_name, tbl_name)
    orig_write_id = valid_write_ids.tblValidWriteIds[0].writeIdHighWaterMark
    self.client.execute("""alter table {0}.{1} add partition (p=1)
        """.format(db_name, tbl_name))
    valid_write_ids = acid_util.get_valid_write_ids(db_name, tbl_name)
    new_write_id = valid_write_ids.tblValidWriteIds[0].writeIdHighWaterMark
    assert new_write_id > orig_write_id

  @SkipIfHive2.acid
  def test_alloc_write_id_error_handing(self, unique_database):
    tbl_name = "insert_only_table"
    self.client.execute("""CREATE TABLE {0}.{1} (i int)
        TBLPROPERTIES('transactional'='true', 'transactional_properties'='insert_only')
        """.format(unique_database, tbl_name))
    self.execute_query_expect_failure(
        self.client,
        "INSERT INTO {0}.{1} VALUES (0), (1), (2)".format(unique_database, tbl_name),
        {"debug_action": "catalogd_update_catalog_abort_txn"})
    # Create a new table and load it in catalogd. Catalogd should not hang.
    self.client.execute("CREATE TABLE {0}.tbl (i int)".format(unique_database))
    self.client.execute("DESCRIBE {0}.tbl".format(unique_database))
