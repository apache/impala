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

from __future__ import absolute_import, division, print_function
import logging
import requests
import time
import json
from hive_metastore.ttypes import NotificationEventRequest

from tests.common.impala_cluster import ImpalaCluster
from tests.common.custom_cluster_test_suite import CustomClusterTestSuite

LOG = logging.getLogger('event_processor_utils')
LOG.setLevel(level=logging.DEBUG)


class EventProcessorUtils(object):

  DEFAULT_CATALOG_URL = "http://localhost:25020"

  @staticmethod
  def wait_for_synced_event_id(timeout, error_status_possible, target_event_id):
    LOG.info("Waiting until events processor syncs to event id:" + str(
        target_event_id))
    # Wait more than timeout in case there is some progress in synced events.
    # The goal is to decrease the effect of parallel DML/DDL tests that can
    # also fire events that have to be processed and delay syncing up to the
    # last event.
    TIMEOUT_MULTIPLIER_IF_THERE_IS_PROGRESS = 10
    total_timeot = timeout * TIMEOUT_MULTIPLIER_IF_THERE_IS_PROGRESS
    status_list = ["ACTIVE", "PAUSED"]
    if error_status_possible:
      status_list.append("ERROR")
    end_time = time.time() + total_timeot
    last_synced_id = EventProcessorUtils.get_last_synced_event_id()
    last_synced_time = time.time()
    while True:
      t = time.time()
      current_synced_id = EventProcessorUtils.get_last_synced_event_id()
      if current_synced_id >= target_event_id:
        LOG.debug(
            "Metric last-synced-event-id has reached the desired value: %d",
            target_event_id)
        break
      status = EventProcessorUtils.get_event_processor_status()
      if status not in status_list:
        error_msg = EventProcessorUtils.get_event_processor_error_msg()
        raise Exception("Event processor is not working. Status: {0}. Error msg: {1}"
                        .format(status, error_msg))
      made_progress = current_synced_id > last_synced_id
      if t >= end_time:
        raise Exception(
            "Event processor did not sync till last known event id {0} \
            within {1} seconds".format(target_event_id, total_timeot))
      elif not made_progress and t >= last_synced_time + timeout:
        raise Exception(
            "Event processor did not make progress since event id {0} \
            within {1} seconds".format(last_synced_id, timeout))
      if made_progress:
        LOG.debug(
            "Metric last-synced-event-id has been increased to %d but has not yet \
            reached the desired value: %d", current_synced_id, target_event_id)
        last_synced_id = current_synced_id
        last_synced_time = t
      time.sleep(0.1)

  @staticmethod
  def wait_for_event_processing(test_suite, timeout=10, error_status_possible=False):
    if isinstance(test_suite, CustomClusterTestSuite):
      impala_cluster = test_suite.cluster
    else:
      impala_cluster = ImpalaCluster.get_e2e_test_cluster()
    EventProcessorUtils.wait_for_event_processing_impl(test_suite.hive_client,
      impala_cluster, timeout, error_status_possible)

  @staticmethod
  def wait_for_event_processing_impl(hive_client, impala_cluster, timeout=10,
                                     error_status_possible=False):
    """Waits till the event processor has synced to the latest event id from metastore
    or the timeout value in seconds whichever is earlier"""
    if EventProcessorUtils.get_event_processor_status() == "DISABLED":
      return
    assert timeout > 0
    assert hive_client is not None
    current_event_id = EventProcessorUtils.get_current_notification_id(
      hive_client)
    EventProcessorUtils.wait_for_synced_event_id(timeout, error_status_possible,
      current_event_id)
    # Wait until the impalad catalog versions agree with the catalogd's version.
    catalogd_version = impala_cluster.catalogd.service.get_catalog_version()
    for impalad in impala_cluster.impalads:
      impalad.service.wait_for_metric_value("catalog.curr-version", catalogd_version,
        allow_greater=True)

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
  def get_event_processor_error_msg():
    """Scrapes the catalog's /events webpage and return the error message (if exists) of
    the event processor"""
    response = requests.get("%s/events?json" % EventProcessorUtils.DEFAULT_CATALOG_URL)
    assert response.status_code == requests.codes.ok
    res_json = json.loads(response.text)
    if "event_processor_error_msg" in res_json:
      return res_json["event_processor_error_msg"].strip()
    return None

  @staticmethod
  def get_int_metric(metric_key, default_val=None):
    """Returns the int value of event processor metric from the /events catalogd debug
     page"""
    metrics = EventProcessorUtils.get_event_processor_metrics()
    if metric_key not in metrics:
      return int(default_val)
    return int(metrics[metric_key])

  @staticmethod
  def get_last_synced_event_id():
    """Returns the last_synced_event_id."""
    metrics = EventProcessorUtils.get_event_processor_metrics()
    assert 'last-synced-event-id' in metrics.keys()
    return int(metrics['last-synced-event-id'])

  @staticmethod
  def get_num_skipped_events():
    """Returns number of skipped events from metrics"""
    metrics = EventProcessorUtils.get_event_processor_metrics()
    assert "events-skipped" in metrics.keys()
    return int(metrics['events-skipped'])

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
    return int(hive_client.get_current_notificationEventId().eventId)

  @staticmethod
  def get_next_notification(hive_client, last_event_id):
    """Returns notification events from metastore"""
    assert hive_client is not None
    assert last_event_id > 0
    notification_event_request = NotificationEventRequest(lastEvent=last_event_id)
    return hive_client.get_next_notification(notification_event_request).events
