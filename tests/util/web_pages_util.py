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
import json


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
