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
import logging
import json
from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.impala_cluster import ImpalaCluster
from tests.verifiers.mem_usage_verifier import MemUsageVerifier, parse_mem_value

LOG = logging.getLogger('test_jvm_mem_tracking')


class TestJvmMemTracker(CustomClusterTestSuite):
  """Test that JVM heap memory consumption is counted against process usage."""
  @classmethod
  def get_workload(self):
    return 'functional-query'

  @CustomClusterTestSuite.with_args(impalad_args="--mem_limit_includes_jvm=true \
                                    --codegen_cache_capacity=0",
                                    start_args="--jvm_args=-Xmx1g", cluster_size=1)
  def test_jvm_mem_tracking(self, vector):
    service = ImpalaCluster.get_e2e_test_cluster().impalads[0].service
    verifier = MemUsageVerifier(service)
    proc_values = verifier.get_mem_usage_values('Process')
    proc_total = proc_values['total']
    proc_limit = proc_values['limit']
    max_heap_size = verifier.get_mem_usage_values('JVM: max heap size')['total']
    non_heap_committed = verifier.get_mem_usage_values('JVM: non-heap committed')['total']
    MB = 1024 * 1024
    LOG.info("proc_total={0}, max_heap_size={1} non_heap_committed={2}".format(
        proc_total, max_heap_size, non_heap_committed))
    # The max heap size will be lower than -Xmx but should be in the same general range.
    assert max_heap_size >= 900 * MB and max_heap_size <= 1024 * MB
    # The non-heap committed value is hard to predict but should be non-zero.
    assert non_heap_committed > 0
    # Process mem consumption should include both of the above values.
    assert proc_total > max_heap_size + non_heap_committed

    # Make sure that the admittable memory is within 100MB of the process limit
    # minus the heap size (there may be some rounding errors).
    backend_json = json.loads(service.read_debug_webpage('backends?json'))
    admit_limit_human_readable = backend_json['backends'][0]['admit_mem_limit']
    admit_limit = parse_mem_value(admit_limit_human_readable)
    LOG.info("proc_limit={0}, admit_limit={1}".format(proc_limit, admit_limit))
    assert abs(admit_limit - (proc_limit - max_heap_size)) <= 100 * MB
