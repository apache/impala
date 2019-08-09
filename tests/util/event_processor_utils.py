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
#
# Impala tests for Hive Metastore, covering the expected propagation
# of metadata from Hive to Impala or Impala to Hive. Each test
# modifies the metadata via Hive and checks that the modification
# succeeded by querying Impala, or vice versa.

import requests
import time
import json
from tests.common.environ import build_flavor_timeout


class EventProcessorUtils(object):

  DEFAULT_CATALOG_URL = "http://localhost:25020"

  @staticmethod
  def wait_for_event_processing(hive_client, timeout=10):
      """Waits till the event processor has synced to the latest event id from metastore
         or the timeout value in seconds whichever is earlier"""
      success = False
      assert timeout > 0
      assert hive_client is not None
      current_event_id = EventProcessorUtils.get_current_notification_id(hive_client)
      end_time = time.time() + timeout
      while time.time() < end_time:
        new_event_id = EventProcessorUtils.get_last_synced_event_id()
        if new_event_id >= current_event_id:
          success = True
          break
        time.sleep(0.1)
      if not success:
        raise Exception(
          "Event processor did not sync till last known event id{0} \
          within {1} seconds".format(current_event_id, timeout))
      # Wait for catalog update to be propagated.
      time.sleep(build_flavor_timeout(6, slow_build_timeout=10))
      return success

  @staticmethod
  def get_event_processor_metrics():
     """Scrapes the catalog's /events webpage and return a dictionary with the event
     processor metrics."""
     response = requests.get("%s/events?json" % EventProcessorUtils.DEFAULT_CATALOG_URL)
     assert response.status_code == requests.codes.ok
     varz_json = json.loads(response.text)
     metrics = varz_json["event_processor_metrics"].strip().splitlines()

     # Helper to strip a pair of elements
     def strip_pair(p):
       return (p[0].strip(), p[1].strip())

     pairs = [strip_pair(kv.split(':')) for kv in metrics if kv]
     return dict(pairs)

  @staticmethod
  def get_last_synced_event_id():
    """Returns the last_synced_event_id."""
    metrics = EventProcessorUtils.get_event_processor_metrics()
    assert 'last-synced-event-id' in metrics.keys()
    return int(metrics['last-synced-event-id'])

  @staticmethod
  def get_event_processor_status():
    """
    Returns the current status of the EventsProcessor
    """
    metrics = EventProcessorUtils.get_event_processor_metrics()
    assert 'status' in metrics.keys()
    return metrics['status']

  @staticmethod
  def get_current_notification_id(hive_client):
    """Returns the current notification from metastore"""
    assert hive_client is not None
    return hive_client.get_current_notificationEventId()
