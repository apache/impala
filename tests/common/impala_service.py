# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Basic object model of a Impala Services (impalad + statestored). Provides a way to
# programatically interact with the services and perform operations such as querying
# the debug webpage, getting metric values, or creating client connections.
import json
import logging
import os
import re
import sys
import urllib

from collections import defaultdict
from HTMLParser import HTMLParser
from tests.common.impala_connection import ImpalaConnection, create_connection
from tests.common.impala_connection import create_ldap_connection
from time import sleep, time

logging.basicConfig(level=logging.ERROR, format='%(threadName)s: %(message)s')
LOG = logging.getLogger('impala_service')
LOG.setLevel(level=logging.DEBUG)

# Base class for all Impala services
# TODO: Refactor the retry/timeout logic into a common place.
class BaseImpalaService(object):
  def __init__(self, hostname, webserver_port):
    self.hostname = hostname
    self.webserver_port = webserver_port

  def open_debug_webpage(self, page_name, timeout=10, interval=1):
    start_time = time()

    while (time() - start_time < timeout):
      try:
        return urllib.urlopen("http://%s:%d/%s" %
            (self.hostname, int(self.webserver_port), page_name))
      except Exception:
        LOG.info("Debug webpage not yet available.")
      sleep(interval)
    assert 0, 'Debug webpage did not become available in expected time.'

  def read_debug_webpage(self, page_name, timeout=10, interval=1):
    return self.open_debug_webpage(page_name, timeout=timeout, interval=interval).read()

  def get_metric_value(self, metric_name, default_value=None):
    """Returns the value of the the given metric name from the Impala debug webpage"""
    metrics = json.loads(self.read_debug_webpage('jsonmetrics?json'))
    return metrics.get(metric_name, default_value)

  def wait_for_metric_value(self, metric_name, expected_value, timeout=10, interval=1):
    start_time = time()
    while (time() - start_time < timeout):
      LOG.info("Getting metric: %s from %s:%s" %
          (metric_name, self.hostname, self.webserver_port))
      value = None
      try:
        value = self.get_metric_value(metric_name)
      except Exception, e:
        LOG.error(e)

      if value == expected_value:
        LOG.info("Metric '%s' has reach desired value: %s" % (metric_name, value))
        return value
      else:
        LOG.info("Waiting for metric value '%s'=%s. Current value: %s" %
            (metric_name, expected_value, value))
      LOG.info("Sleeping %ds before next retry." % interval)
      sleep(interval)
    assert 0, 'Metric value %s did not reach value %s in %ss' %\
        (metric_name, expected_value, timeout)

# Allows for interacting with an Impalad instance to perform operations such as creating
# new connections or accessing the debug webpage.
class ImpaladService(BaseImpalaService):
  def __init__(self, hostname, webserver_port=25000, beeswax_port=21000, be_port=22000):
    super(ImpaladService, self).__init__(hostname, webserver_port)
    self.beeswax_port = beeswax_port
    self.be_port = be_port

  def get_num_known_live_backends(self, timeout=30, interval=1):
    LOG.info("Getting num_known_live_backends from %s:%s" %
        (self.hostname, self.webserver_port))
    result = json.loads(self.read_debug_webpage('backends?json', timeout, interval))
    num = len(result['backends'])
    return None if num is None else int(num)

  def get_num_in_flight_queries(self, timeout=30, interval=1):
    LOG.info("Getting num_in_flight_queries from %s:%s" %
        (self.hostname, self.webserver_port))
    result = self.read_debug_webpage('inflight_query_ids?raw', timeout, interval)
    return None if result is None else len([l for l in result.split('\n') if l])

  def wait_for_num_in_flight_queries(self, expected_val, timeout=10):
    """Waits for the number of in-flight queries to reach a certain value"""
    start_time = time()
    while (time() - start_time < timeout):
      num_in_flight_queries = self.get_num_in_flight_queries()
      if num_in_flight_queries == expected_val: return True
      sleep(1)
    LOG.info("The number of in flight queries: %s, expected: %s" %
        (num_in_flight_queries, expected_val))
    return False


  def wait_for_num_known_live_backends(self, expected_value, timeout=30, interval=1):
    start_time = time()
    while (time() - start_time < timeout):
      value = None
      try:
        value = self.get_num_known_live_backends(timeout=timeout, interval=interval)
      except Exception, e:
        LOG.error(e)
      if value == expected_value:
        LOG.info("num_known_live_backends has reached value: %s" % value)
        return value
      else:
        LOG.info("Waiting for num_known_live_backends=%s. Current value: %s" %\
            (expected_value, value))
      sleep(1)
    assert 0, 'num_known_live_backends did not reach expected value in time'

  def read_query_profile_page(self, query_id, timeout=10, interval=1):
    """Fetches the raw contents of the query's runtime profile webpage.
    Fails an assertion if Impala's webserver is unavailable or the query's
    profile page doesn't exist."""
    return self.read_debug_webpage("query_profile?query_id=%s&raw" % (query_id))

  def get_query_status(self, query_id):
    """Gets the 'Query Status' section of the query's runtime profile."""
    page = self.read_query_profile_page(query_id)
    status_line =\
        next((x for x in page.split('\n') if re.search('Query Status:', x)), None)
    return status_line.split('Query Status:')[1].strip()

  def wait_for_query_state(self, client, query_handle, target_state,
                              timeout=10, interval=1):
    """Keeps polling for the query's state using client in the given interval until
    the query's state reaches the target state or the given timeout has been reached."""
    start_time = time()
    while (time() - start_time < timeout):
      try:
        query_state = client.get_state(query_handle)
      except Exception as e:
        pass
      if query_state == target_state:
        return
      sleep(interval)
    assert target_state == query_state, 'Did not reach query state in time'
    return

  def wait_for_query_status(self, client, query_id, expected_content,
                               timeout=30, interval=1):
    """Polls for the query's status in the query profile web page to contain the
    specified content. Returns False if the timeout was reached before a successful
    match, True otherwise."""
    start_time = time()
    query_status = ""
    while (time() - start_time < timeout):
      try:
        query_status = self.get_query_status(query_id)
        if query_status is None:
          assert False, "Could not find 'Query Status' section in profile of "\
                    "query with id %s:\n%s" % (query_id)
      except Exception as e:
        pass
      if expected_content in query_status:
        return True
      sleep(interval)
    return False

  def create_beeswax_client(self, use_kerberos=False):
    """Creates a new beeswax client connection to the impalad"""
    client = create_connection('%s:%d' % (self.hostname, self.beeswax_port), use_kerberos)
    client.connect()
    return client

  def create_ldap_beeswax_client(self, user, password, use_ssl=False):
    client = create_ldap_connection('%s:%d' % (self.hostname, self.beeswax_port),
                                    user=user, password=password, use_ssl=use_ssl)
    client.connect()
    return client

  def get_catalog_object_dump(self, object_type, object_name):
    return self.read_debug_webpage('catalog_objects?object_type=%s&object_name=%s' %\
        (object_type, object_name))


# Allows for interacting with the StateStore service to perform operations such as
# accessing the debug webpage.
class StateStoredService(BaseImpalaService):
  def __init__(self, hostname, webserver_port):
    super(StateStoredService, self).__init__(hostname, webserver_port)

  def wait_for_live_subscribers(self, num_subscribers, timeout=15, interval=1):
    self.wait_for_metric_value('statestore.live-backends', num_subscribers,
                               timeout=timeout, interval=interval)


# Allows for interacting with the Catalog service to perform operations such as
# accessing the debug webpage.
class CatalogdService(BaseImpalaService):
  def __init__(self, hostname, webserver_port, service_port):
    super(CatalogdService, self).__init__(hostname, webserver_port)
    self.service_port = service_port

  def get_catalog_object_dump(self, object_type, object_name):
    return self.read_debug_webpage('catalog_objects?object_type=%s&object_name=%s' %\
        (object_type, object_name))
