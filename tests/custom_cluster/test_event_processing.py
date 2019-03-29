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
from datetime import datetime
from tests.common.environ import build_flavor_timeout
import requests
from tests.common.skip import SkipIfS3, SkipIfABFS, SkipIfADLS, SkipIfIsilon, SkipIfLocal
from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.util.hive_utils import HiveDbWrapper


@SkipIfS3.hive
@SkipIfABFS.hive
@SkipIfADLS.hive
@SkipIfIsilon.hive
@SkipIfLocal.hive
class TestEventProcessing(CustomClusterTestSuite):
  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    catalogd_args="--hms_event_polling_interval_s=2"
  )
  def test_insert_events(self):
    """
    Test for insert event processing. Events are created in Hive and processed in Impala.
    The following cases are tested :
    Insert into table --> for partitioned and non-partitioned table
    Insert overwrite table --> for partitioned and non-partitioned table
    Insert into partition --> for partitioned table
    """
    db_name = 'test_db'
    with HiveDbWrapper(self, db_name):
     # Test table with no partitions.
     TBL_INSERT_NOPART = 'tbl_insert_nopart'
     last_synced_event_id = self.get_last_synced_event_id()
     self.run_stmt_in_hive("create table %s.%s (id int, val int)"
         % (db_name, TBL_INSERT_NOPART))
     # Test insert into table, this will fire an insert event.
     self.run_stmt_in_hive("insert into %s.%s values(101, 200)"
         % (db_name, TBL_INSERT_NOPART))
     # With MetastoreEventProcessor running, the insert event will be processsed. Query
     # the table from Impala
     assert self.wait_for_insert_event_processing(last_synced_event_id) is True
     data = self.execute_scalar("select * from %s.%s" % (db_name, TBL_INSERT_NOPART))
     assert data.split('\t') == ['101', '200']
     # Test insert overwrite. Overwite the existing value.
     last_synced_event_id = self.get_last_synced_event_id()
     self.run_stmt_in_hive("insert overwrite table %s.%s values(101, 201)"
         % (db_name, TBL_INSERT_NOPART))
     # Make sure the event has been processed.
     assert self.wait_for_insert_event_processing(last_synced_event_id) is True
     # Query table from Impala
     data = self.execute_scalar("select * from %s.%s" % (db_name, TBL_INSERT_NOPART))
     assert data.split('\t') == ['101', '201']

     # Test partitioned table.
     last_synced_event_id = self.get_last_synced_event_id()
     TBL_INSERT_PART = 'tbl_insert_part'
     self.run_stmt_in_hive("create table %s.%s (id int, name string) "
         "partitioned by(day int, month int, year int)" % (db_name, TBL_INSERT_PART))
     # Insert data into partitions
     self.run_stmt_in_hive("insert into %s.%s partition(day=28, month=03, year=2019)"
         "values(101, 'x')" % (db_name, TBL_INSERT_PART))
     #  Make sure the event is  processed.
     assert self.wait_for_insert_event_processing(last_synced_event_id) is True
     # Test if the data is present in Impala
     data = self.execute_scalar("select * from %s.%s" % (db_name, TBL_INSERT_PART))
     assert data.split('\t') == ['101', 'x', '28', '3', '2019']

     # Test inserting into existing partitions.
     last_synced_event_id = self.get_last_synced_event_id()
     self.run_stmt_in_hive("insert into %s.%s partition(day=28, month=03, year=2019)"
         "values(102, 'y')" % (db_name, TBL_INSERT_PART))
     assert self.wait_for_insert_event_processing(last_synced_event_id) is True
     data = self.execute_scalar("select count(*) from %s.%s where day=28 and month=3 "
         "and year=2019" % (db_name, TBL_INSERT_PART))
     assert data.split('\t') == ['2']

     # Test insert overwrite into existing partitions
     last_synced_event_id = self.get_last_synced_event_id()
     self.run_stmt_in_hive("insert overwrite table %s.%s partition(day=28, month=03, "
         "year=2019)" "values(101, 'z')" % (db_name, TBL_INSERT_PART))
     assert self.wait_for_insert_event_processing(last_synced_event_id) is True
     data = self.execute_scalar("select * from %s.%s where day=28 and month=3 and"
         " year=2019 and id=101" % (db_name, TBL_INSERT_PART))
     assert data.split('\t') == ['101', 'z', '28', '3', '2019']

  def wait_for_insert_event_processing(self, previous_event_id):
    """ Wait till the event processor has finished processing insert events. This is
    detected by scrapping the /events webpage for changes in last_synced_event_id.
    Since two events are created for every insert done through hive, we wait till the
    event id is incremented by at least two. Returns true if at least two events were
    processed within 10 sec. False otherwise.
    """
    new_event_id = self.get_last_synced_event_id()
    success = True
    start_time = datetime.now()
    while new_event_id - previous_event_id < 2:
      new_event_id = self.get_last_synced_event_id()
      # Prevent infinite loop
      time_delta = (datetime.now() - start_time).total_seconds()
      if time_delta > 10:
        success = False
        break
    # Wait for catalog update to be propagated.
    time.sleep(build_flavor_timeout(2, slow_build_timeout=4))
    return success

  def get_last_synced_event_id(self):
    """
    Scrape the /events webpage and return the last_synced_event_id.
    """
    response = requests.get("http://localhost:25020/events?json")
    assert response.status_code == requests.codes.ok
    varz_json = json.loads(response.text)
    metrics = varz_json["event_processor_metrics"].strip().split('\n')
    kv_map = {}
    for kv in metrics:
      if len(kv) > 0:
        pair = kv.split(':')
        kv_map[pair[0].strip()] = pair[1].strip()

    last_synced_event_id = int(kv_map['last-synced-event-id'])
    return last_synced_event_id
