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
#
# Basic object model of a Impala Services (impalad + statestored). Provides a way to
# programatically interact with the services and perform operations such as querying
# the debug webpage, getting metric values, or creating client connections.

import json
import logging
import re
import requests
from time import sleep, time

from tests.common.impala_connection import create_connection, create_ldap_connection
from TCLIService import TCLIService
from thrift.transport.TSocket import TSocket
from thrift.transport.TTransport import TBufferedTransport
from thrift.protocol import TBinaryProtocol, TCompactProtocol, TProtocol
from thrift.TSerialization import deserialize
from RuntimeProfile.ttypes import TRuntimeProfileTree
import base64
import zlib

LOG = logging.getLogger('impala_service')
LOG.setLevel(level=logging.DEBUG)

# Base class for all Impala services
# TODO: Refactor the retry/timeout logic into a common place.
class BaseImpalaService(object):
  def __init__(self, hostname, webserver_port, webserver_certificate_file):
    self.hostname = hostname
    self.webserver_port = webserver_port
    self.webserver_certificate_file = webserver_certificate_file

  def open_debug_webpage(self, page_name, timeout=10, interval=1):
    start_time = time()

    while (time() - start_time < timeout):
      try:
        protocol = "http"
        if self.webserver_certificate_file != "":
          protocol = "https"
        url = "%s://%s:%d/%s" % \
            (protocol, self.hostname, int(self.webserver_port), page_name)
        return requests.get(url, verify=self.webserver_certificate_file)
      except Exception as e:
        LOG.info("Debug webpage not yet available: %s", str(e))
      sleep(interval)
    assert 0, 'Debug webpage did not become available in expected time.'

  def read_debug_webpage(self, page_name, timeout=10, interval=1):
    return self.open_debug_webpage(page_name, timeout=timeout, interval=interval).text

  def get_debug_webpage_json(self, page_name):
    """Returns the json for the given Impala debug webpage, eg. '/queries'"""
    return json.loads(self.read_debug_webpage(page_name + "?json"))

  def get_metric_value(self, metric_name, default_value=None):
    """Returns the value of the the given metric name from the Impala debug webpage"""
    return self.get_metric_values([metric_name], [default_value])[0]

  def get_metric_values(self, metric_names, default_values=None):
    """Returns the value of the given metrics from the Impala debug webpage. If
    default_values is provided and a metric is not present, the default value
    is returned instead."""
    if default_values is None:
      default_values = [None for m in metric_names]
    assert len(metric_names) == len(default_values)
    metrics = json.loads(self.read_debug_webpage('jsonmetrics?json'))
    return [metrics.get(metric_name, default_value)
            for metric_name, default_value in zip(metric_names, default_values)]

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
        LOG.info("Metric '%s' has reached desired value: %s" % (metric_name, value))
        return value
      else:
        LOG.info("Waiting for metric value '%s'=%s. Current value: %s" %
            (metric_name, expected_value, value))
      LOG.info("Sleeping %ds before next retry." % interval)
      sleep(interval)
    assert 0, 'Metric value %s did not reach value %s in %ss\nDumping impalad debug ' \
              'pages:\nmemz: %s\nmetrics: %s\nqueries: %s\nsessions: %s\nthreadz: %s\n '\
              'rpcz: %s' % \
              (metric_name, expected_value, timeout,
               json.dumps(self.read_debug_webpage('memz?json')),
               json.dumps(self.read_debug_webpage('metrics?json')),
               json.dumps(self.read_debug_webpage('queries?json')),
               json.dumps(self.read_debug_webpage('sessions?json')),
               json.dumps(self.read_debug_webpage('threadz?json')),
               json.dumps(self.read_debug_webpage('rpcz?json')))

  def get_catalog_object_dump(self, object_type, object_name):
    """ Gets the web-page for the given 'object_type' and 'object_name'."""
    return self.read_debug_webpage('catalog_object?object_type=%s&object_name=%s' %\
        (object_type, object_name))

  def get_catalog_objects(self, excludes=['_impala_builtins']):
    """ Returns a dictionary containing all catalog objects. Each entry's key is the fully
        qualified object name and the value is a tuple of the form (type, version).
        Does not return databases listed in the 'excludes' list."""
    catalog = self.get_debug_webpage_json('catalog')
    objects = {}
    for db_desc in catalog["databases"]:
      db_name = db_desc["name"]
      if db_name in excludes:
        continue
      db = self.get_catalog_object_dump('DATABASE', db_name)
      objects[db_name] = ('DATABASE', self.extract_catalog_object_version(db))
      for table_desc in db_desc["tables"]:
        table_name = table_desc["fqtn"]
        table = self.get_catalog_object_dump('TABLE', table_name)
        objects[table_name] = ('TABLE', self.extract_catalog_object_version(table))
    return objects

  def extract_catalog_object_version(self, thrift_txt):
    """ Extracts and returns the version of the catalog object's 'thrift_txt' representation."""
    result = re.search(r'catalog_version \(i64\) = (\d+)', thrift_txt)
    assert result, 'Unable to find catalog version in object: ' + thrift_txt
    return int(result.group(1))

# Allows for interacting with an Impalad instance to perform operations such as creating
# new connections or accessing the debug webpage.
class ImpaladService(BaseImpalaService):
  def __init__(self, hostname, webserver_port=25000, beeswax_port=21000, be_port=22000,
               hs2_port=21050, webserver_certificate_file=""):
    super(ImpaladService, self).__init__(
        hostname, webserver_port, webserver_certificate_file)
    self.beeswax_port = beeswax_port
    self.be_port = be_port
    self.hs2_port = hs2_port

  def get_num_known_live_backends(self, timeout=30, interval=1,
      include_shutting_down=True):
    LOG.info("Getting num_known_live_backends from %s:%s" %
        (self.hostname, self.webserver_port))
    result = json.loads(self.read_debug_webpage('backends?json', timeout, interval))
    count = 0
    for backend in result['backends']:
      if include_shutting_down or not backend['is_quiescing']:
        count += 1
    return count

  def get_query_locations(self):
    # Returns a dictionary of the format <host_address, num_of_queries_running_there>
    result = json.loads(self.read_debug_webpage('queries?json', timeout=30, interval=1))
    if result['query_locations'] is not None:
      return dict([(loc["location"], loc["count"]) for loc in result['query_locations']])
    return None

  def get_in_flight_queries(self, timeout=30, interval=1):
    result = json.loads(self.read_debug_webpage('queries?json', timeout, interval))
    return result['in_flight_queries']

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

  def wait_for_num_known_live_backends(self, expected_value, timeout=30, interval=1,
      include_shutting_down=True):
    start_time = time()
    while (time() - start_time < timeout):
      value = None
      try:
        value = self.get_num_known_live_backends(timeout=timeout, interval=interval,
            include_shutting_down=include_shutting_down)
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
    assert target_state == query_state, \
        'Query {0} did not reach query state in time target={1} actual={2}'.format(
            query_handle.get_handle().id, target_state, query_state)
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
    client = create_connection('%s:%d' % (self.hostname, self.beeswax_port),
                               use_kerberos, 'beeswax')
    client.connect()
    return client

  def beeswax_port_is_open(self):
    """Test if the beeswax port is open. Does not need to authenticate."""
    try:
      # The beeswax client will connect successfully even if not authenticated.
      client = self.create_beeswax_client()
      client.close()
      return True
    except Exception:
      return False

  def create_ldap_beeswax_client(self, user, password, use_ssl=False):
    client = create_ldap_connection('%s:%d' % (self.hostname, self.beeswax_port),
                                    user=user, password=password, use_ssl=use_ssl)
    client.connect()
    return client

  def create_hs2_client(self):
    """Creates a new HS2 client connection to the impalad"""
    client = create_connection('%s:%d' % (self.hostname, self.hs2_port), protocol='hs2')
    client.connect()
    return client

  def hs2_port_is_open(self):
    """Test if the HS2 port is open. Does not need to authenticate."""
    # Impyla will try to authenticate as part of connecting, so preserve previous logic
    # that uses the HS2 thrift code directly.
    try:
      socket = TSocket(self.hostname, self.hs2_port)
      transport = TBufferedTransport(socket)
      transport.open()
      transport.close()
      return True
    except Exception, e:
      LOG.info(e)
      return False

# Allows for interacting with the StateStore service to perform operations such as
# accessing the debug webpage.
class StateStoredService(BaseImpalaService):
  def __init__(self, hostname, webserver_port, webserver_certificate_file):
    super(StateStoredService, self).__init__(
        hostname, webserver_port, webserver_certificate_file)

  def wait_for_live_subscribers(self, num_subscribers, timeout=15, interval=1):
    self.wait_for_metric_value('statestore.live-backends', num_subscribers,
                               timeout=timeout, interval=interval)


# Allows for interacting with the Catalog service to perform operations such as
# accessing the debug webpage.
class CatalogdService(BaseImpalaService):
  def __init__(self, hostname, webserver_port, webserver_certificate_file, service_port):
    super(CatalogdService, self).__init__(
        hostname, webserver_port, webserver_certificate_file)
    self.service_port = service_port

  def get_catalog_version(self, timeout=10, interval=1):
    """ Gets catalogd's latest catalog version. Retry for 'timeout'
        seconds, sleeping 'interval' seconds between tries. If the
        version cannot be obtained, this method fails."""
    start_time = time()
    while (time() - start_time < timeout):
      try:
        info = self.get_debug_webpage_json('catalog')
        if "version" in info: return info['version']
      except Exception:
        LOG.info('Catalogd version not yet available.')
      sleep(interval)
    assert False, 'Catalog version not ready in expected time.'
