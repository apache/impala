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

from __future__ import absolute_import, division, print_function
from multiprocessing import Process, Queue
import json
import requests

from tests.util.retry import retry


def get_num_completed_backends(service, query_id):
    """Get the number of completed backends for the given query_id from the
    'query_backends' web page."""
    query_backend_url = 'query_backends?query_id={0}&json'.format(query_id)
    query_backends_json = json.loads(service.read_debug_webpage(query_backend_url))
    assert 'backend_states' in query_backends_json
    num_complete_backends = 0
    for backend_state in query_backends_json['backend_states']:
      if backend_state['done']: num_complete_backends += 1
    return num_complete_backends


def get_mem_admitted_backends_debug_page(cluster, ac_process=None):
  """Helper method assumes a cluster using a dedicated coordinator. Returns the mem
  admitted to the backends extracted from the backends debug page of the coordinator
  impala daemon. Returns a dictionary with the keys 'coordinator' and 'executor' and
  their respective mem values in bytes. The entry for 'executor' is a list of the mem
  admitted for each executor."""
  # Based on how the cluster is setup, the first impalad in the cluster is the
  # coordinator.
  if ac_process is None:
    ac_process = cluster.impalads[0]
  response_json = ac_process.service.get_debug_webpage_json('backends')
  assert 'backends' in response_json
  assert len(response_json['backends']) >= 2
  ret = dict()
  ret['executor'] = []
  from tests.verifiers.mem_usage_verifier import parse_mem_value
  for backend in response_json['backends']:
    if backend['is_coordinator']:
      ret['coordinator'] = parse_mem_value(backend['mem_admitted'])
    else:
      ret['executor'].append(parse_mem_value(backend['mem_admitted']))
  return ret


def cancel(impalad, query_id):
  """Cancel a query via /cancel_query on impalad."""
  cancel_query_url = "http://{0}:{1}/cancel_query?json&query_id={2}"\
      .format(impalad.webserver_interface, impalad.webserver_port, query_id)
  response = requests.post(cancel_query_url)
  assert response.status_code == requests.codes.ok
  response_json = json.loads(response.text)
  assert response_json['contents'] == "Query cancellation successful"


def wait_for_state(impalad, state):
  """Wait for the inflight query to reach 'state' on impalad. If state=None, wait for
  no inflight queries."""
  def is_state():
    in_flight_queries = impalad.get_debug_webpage_json('queries')['in_flight_queries']
    if state:
      return len(in_flight_queries) > 0 and in_flight_queries[0]['state'] == state
    else:
      return len(in_flight_queries) <= 0

  assert retry(is_state)


def assert_query_stopped(impalad, query_id):
  """Assert all queries are complete, and query_id is in completed_queries, on impalad."""
  response_json = impalad.get_debug_webpage_json('queries')
  assert response_json['num_in_flight_queries'] == 0
  assert response_json['num_waiting_queries'] == 0

  expected_queries = [q for q in response_json['completed_queries']
                      if q['query_id'] == query_id]
  assert len(expected_queries) == 1


def run(test_class, query, options, queue):
  """Execute query and put results or errors into queue."""
  with test_class.create_impala_client() as client:
    if options:
      client.set_configuration(options)
    try:
        queue.put(str(client.execute(query)))
    except Exception as ex:
        queue.put(str(ex))


def start(test_class, query, options=None):
  """Execute a query in a separate process. Returns (process, queue)."""
  queue = Queue()
  proc = Process(target=run, args=(test_class, query, options, queue))
  proc.start()
  return proc, queue


def join(proc, queue):
  """Returns result from queue returned by 'start', and waits for proc to complete."""
  result = queue.get()
  proc.join()
  return result
