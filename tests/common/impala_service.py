#!/usr/bin/env python
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
from tests.beeswax.impala_beeswax import ImpalaBeeswaxClient
from time import sleep, time

logging.basicConfig(level=logging.ERROR, format='%(threadName)s: %(message)s')
LOG = logging.getLogger('impala_service')
LOG.setLevel(level=logging.DEBUG)

# Base class for Impalad and Statestore services
class BaseImpalaService(object):
  def __init__(self, hostname, webserver_port):
    self.hostname = hostname
    self.webserver_port = webserver_port

  def read_debug_webpage(self, page_name, timeout=10, interval=1):
    start_time = time()

    while (time() - start_time < timeout):
      try:
        return urllib.urlopen("http://%s:%d/%s" %\
            (self.hostname, int(self.webserver_port), page_name)).read()
      except Exception:
        LOG.info("Debug webpage not yet available.")
      sleep(interval)
    assert 0, 'Debug webpage did not become available in expected time.'

  def get_metric_value(self, metric_name):
    """Returns the value of the the given metric name from the Impala debug webpage"""
    return json.loads(self.read_debug_webpage('jsonmetrics'))[metric_name]

  def wait_for_metric_value(self, metric_name, expected_value, timeout=10, interval=1):
    start_time = time()
    while (time() - start_time < timeout):
      LOG.info("Getting metric: %s from %s:%s" %\
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
        LOG.info("Waiting for metric value '%s'=%s. Current value: %s" %\
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
    LOG.info("Getting num_known_live_backends from %s:%s" %\
        (self.hostname, self.webserver_port))
    result = self.read_debug_webpage('backends?raw', timeout, interval)
    match = re.match(r'Known Backends \((\d+)\)', result)
    return None if match is None else int(match.group(1))

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
    return

  def create_beeswax_client(self, use_kerberos=False):
    """Creates a new beeswax client connection to the impalad"""
    client =\
        ImpalaBeeswaxClient('%s:%d' % (self.hostname, self.beeswax_port), use_kerberos)
    client.connect()
    return client

# Allows for interacting with an Impalad instance to perform operations such as creating
# new connections or accessing the debug webpage.
class StateStoredService(BaseImpalaService):
  def __init__(self, hostname, webserver_port):
    super(StateStoredService, self).__init__(hostname, webserver_port)

  def wait_for_live_backends(self, num_backends, timeout=15, interval=1):
    self.wait_for_metric_value('statestore.live-backends', num_backends,
                               timeout=timeout, interval=interval)
