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
import json
import time
import requests

from tests.common.environ import build_flavor_timeout
from tests.common.skip import SkipIfS3, SkipIfABFS, SkipIfADLS, SkipIfIsilon, \
    SkipIfLocal, SkipIfHive2
from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.skip import SkipIfS3, SkipIfABFS, SkipIfADLS, SkipIfIsilon, SkipIfLocal
from tests.util.hive_utils import HiveDbWrapper
from tests.util.event_processor_utils import EventProcessorUtils


@SkipIfS3.hive
@SkipIfABFS.hive
@SkipIfADLS.hive
@SkipIfIsilon.hive
@SkipIfLocal.hive
class TestEventProcessing(CustomClusterTestSuite):
  """This class contains tests that exercise the event processing mechanism in the
  catalog."""
  CATALOG_URL = "http://localhost:25020"
  PROCESSING_TIMEOUT_S = 10

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args="--blacklisted_dbs=testBlackListedDb "
                 "--blacklisted_tables=functional_parquet.testBlackListedTbl",
    catalogd_args="--blacklisted_dbs=testBlackListedDb "
                  "--blacklisted_tables=functional_parquet.testBlackListedTbl "
                  "--hms_event_polling_interval_s=1")
  def test_events_on_blacklisted_objects(self):
    """Executes hive queries on blacklisted database and tables and makes sure that
    event processor does not error out
    """
    try:
      self.run_stmt_in_hive("create database testBlackListedDb")
      self.run_stmt_in_hive("create table testBlackListedDb.testtbl (id int)")
      self.run_stmt_in_hive(
        "create table functional_parquet.testBlackListedTbl (id int, val string)"
        " partitioned by (part int) stored as parquet")
      self.run_stmt_in_hive(
        "alter table functional_parquet.testBlackListedTbl add partition (part=1)")
      # wait until all the events generated above are processed
      EventProcessorUtils.wait_for_event_processing(self.hive_client)
      assert EventProcessorUtils.get_event_processor_status() == "ACTIVE"
    finally:
      self.run_stmt_in_hive("drop database testBlackListedDb cascade")
      self.run_stmt_in_hive("drop table functional_parquet.testBlackListedTbl")

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(catalogd_args="--hms_event_polling_interval_s=2")
  @SkipIfHive2.acid
  def test_insert_events_transactional(self):
    """Executes 'run_test_insert_events' for transactional tables.
    """
    self.run_test_insert_events(is_transactional=True)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(catalogd_args="--hms_event_polling_interval_s=2")
  def test_insert_events(self):
    """Executes 'run_test_insert_events' for non-transactional tables.
    """
    self.run_test_insert_events()

  def run_test_insert_events(self, is_transactional=False):
    """Test for insert event processing. Events are created in Hive and processed in
    Impala. The following cases are tested :
    Insert into table --> for partitioned and non-partitioned table
    Insert overwrite table --> for partitioned and non-partitioned table
    Insert into partition --> for partitioned table
    """
    db_name = 'test_db'
    with HiveDbWrapper(self, db_name):
     # Test table with no partitions.
     TBL_INSERT_NOPART = 'tbl_insert_nopart'
     self.run_stmt_in_hive("drop table if exists %s.%s" % (db_name, TBL_INSERT_NOPART))
     last_synced_event_id = self.get_last_synced_event_id()
     TBLPROPERTIES = ""
     if is_transactional:
       TBLPROPERTIES = "TBLPROPERTIES ('transactional'='true'," \
           "'transactional_properties'='insert_only')"
     self.run_stmt_in_hive("create table %s.%s (id int, val int) %s"
         % (db_name, TBL_INSERT_NOPART, TBLPROPERTIES))
     # Test insert into table, this will fire an insert event.
     self.run_stmt_in_hive("insert into %s.%s values(101, 200)"
         % (db_name, TBL_INSERT_NOPART))
     # With MetastoreEventProcessor running, the insert event will be processed. Query the
     # table from Impala.
     assert self.wait_for_insert_event_processing(last_synced_event_id) is True
     # Verify that the data is present in Impala.
     data = self.execute_scalar("select * from %s.%s" % (db_name, TBL_INSERT_NOPART))
     assert data.split('\t') == ['101', '200']

     # Test insert overwrite. Overwrite the existing value.
     last_synced_event_id = self.get_last_synced_event_id()
     self.run_stmt_in_hive("insert overwrite table %s.%s values(101, 201)"
         % (db_name, TBL_INSERT_NOPART))
     # Make sure the event has been processed.
     assert self.wait_for_insert_event_processing(last_synced_event_id) is True
     # Verify that the data is present in Impala.
     data = self.execute_scalar("select * from %s.%s" % (db_name, TBL_INSERT_NOPART))
     assert data.split('\t') == ['101', '201']

     # Test partitioned table.
     last_synced_event_id = self.get_last_synced_event_id()
     TBL_INSERT_PART = 'tbl_insert_part'
     self.run_stmt_in_hive("drop table if exists %s.%s" % (db_name, TBL_INSERT_PART))
     self.run_stmt_in_hive("create table %s.%s (id int, name string) "
         "partitioned by(day int, month int, year int) %s"
         % (db_name, TBL_INSERT_PART, TBLPROPERTIES))
     # Insert data into partitions.
     self.run_stmt_in_hive("insert into %s.%s partition(day=28, month=03, year=2019)"
         "values(101, 'x')" % (db_name, TBL_INSERT_PART))
     # Make sure the event has been processed.
     assert self.wait_for_insert_event_processing(last_synced_event_id) is True
     # Verify that the data is present in Impala.
     data = self.execute_scalar("select * from %s.%s" % (db_name, TBL_INSERT_PART))
     assert data.split('\t') == ['101', 'x', '28', '3', '2019']

     # Test inserting into existing partitions.
     last_synced_event_id = self.get_last_synced_event_id()
     self.run_stmt_in_hive("insert into %s.%s partition(day=28, month=03, year=2019)"
         "values(102, 'y')" % (db_name, TBL_INSERT_PART))
     assert self.wait_for_insert_event_processing(last_synced_event_id) is True
     # Verify that the data is present in Impala.
     data = self.execute_scalar("select count(*) from %s.%s where day=28 and month=3 "
         "and year=2019" % (db_name, TBL_INSERT_PART))
     assert data.split('\t') == ['2']

     # Test insert overwrite into existing partitions
     last_synced_event_id = self.get_last_synced_event_id()
     self.run_stmt_in_hive("insert overwrite table %s.%s partition(day=28, month=03, "
         "year=2019)" "values(101, 'z')" % (db_name, TBL_INSERT_PART))
     assert self.wait_for_insert_event_processing(last_synced_event_id) is True
     # Verify that the data is present in Impala.
     data = self.execute_scalar("select * from %s.%s where day=28 and month=3 and"
         " year=2019 and id=101" % (db_name, TBL_INSERT_PART))
     assert data.split('\t') == ['101', 'z', '28', '3', '2019']

  @CustomClusterTestSuite.with_args(
    catalogd_args="--hms_event_polling_interval_s=1"
  )
  @SkipIfHive2.acid
  def test_empty_partition_events_transactional(self, unique_database):
    self._run_test_empty_partition_events(unique_database, True)

  @CustomClusterTestSuite.with_args(
    catalogd_args="--hms_event_polling_interval_s=1"
  )
  def test_empty_partition_events(self, unique_database):
    self._run_test_empty_partition_events(unique_database, False)

  def _run_test_empty_partition_events(self, unique_database, is_transactional):
    TBLPROPERTIES = ""
    if is_transactional:
       TBLPROPERTIES = "TBLPROPERTIES ('transactional'='true'," \
           "'transactional_properties'='insert_only')"
    test_tbl = unique_database + ".test_events"
    self.run_stmt_in_hive("create table {0} (key string, value string) \
      partitioned by (year int) stored as parquet {1}".format(test_tbl, TBLPROPERTIES))
    EventProcessorUtils.wait_for_event_processing(self.hive_client)
    self.client.execute("describe {0}".format(test_tbl))

    self.run_stmt_in_hive(
      "alter table {0} add partition (year=2019)".format(test_tbl))
    EventProcessorUtils.wait_for_event_processing(self.hive_client)
    assert [('2019',)] == self.get_impala_partition_info(test_tbl, 'year')

    self.run_stmt_in_hive(
      "alter table {0} add if not exists partition (year=2019)".format(test_tbl))
    EventProcessorUtils.wait_for_event_processing(self.hive_client)
    assert [('2019',)] == self.get_impala_partition_info(test_tbl, 'year')
    assert EventProcessorUtils.get_event_processor_status() == "ACTIVE"

    self.run_stmt_in_hive(
      "alter table {0} drop partition (year=2019)".format(test_tbl))
    EventProcessorUtils.wait_for_event_processing(self.hive_client)
    assert ('2019') not in self.get_impala_partition_info(test_tbl, 'year')
    assert EventProcessorUtils.get_event_processor_status() == "ACTIVE"

    self.run_stmt_in_hive(
      "alter table {0} drop if exists partition (year=2019)".format(test_tbl))
    EventProcessorUtils.wait_for_event_processing(self.hive_client)
    assert ('2019') not in self.get_impala_partition_info(test_tbl, 'year')
    assert EventProcessorUtils.get_event_processor_status() == "ACTIVE"


  def wait_for_insert_event_processing(self, previous_event_id):
    """Waits until the event processor has finished processing insert events. Since two
    events are created for every insert done through hive, we wait until the event id is
    incremented by at least two. Returns true if at least two events were processed within
    self.PROCESSING_TIMEOUT_S, False otherwise.
    """
    new_event_id = self.get_last_synced_event_id()
    success = False
    end_time = time.time() + self.PROCESSING_TIMEOUT_S
    while time.time() < end_time:
      new_event_id = self.get_last_synced_event_id()
      if new_event_id - previous_event_id >= 2:
        success = True
        break
      time.sleep(0.1)
    # Wait for catalog update to be propagated.
    time.sleep(build_flavor_timeout(2, slow_build_timeout=4))
    return success

  def get_event_processor_metrics(self):
    """Scrapes the catalog's /events webpage and return a dictionary with the event
    processor metrics."""
    response = requests.get("%s/events?json" % self.CATALOG_URL)
    assert response.status_code == requests.codes.ok
    varz_json = json.loads(response.text)
    metrics = varz_json["event_processor_metrics"].strip().splitlines()

    # Helper to strip a pair of elements
    def strip_pair(p):
      return (p[0].strip(), p[1].strip())

    pairs = [strip_pair(kv.split(':')) for kv in metrics if kv]
    return dict(pairs)

  def get_last_synced_event_id(self):
    """Returns the last_synced_event_id."""
    metrics = self.get_event_processor_metrics()
    assert 'last-synced-event-id' in metrics.keys()
    return int(metrics['last-synced-event-id'])
