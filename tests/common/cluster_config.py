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

# Common cluster configurations as decorators for custom cluster tests

from __future__ import absolute_import, division, print_function

import os
import shutil

from tests.common.custom_cluster_test_suite import CustomClusterTestSuite


# Same as in tests/authorization/test_ranger.py
ADMIN = "admin"

# The path to resources directory which contains the admission control config files.
RESOURCES_DIR = os.path.join(os.environ['IMPALA_HOME'], "fe", "src", "test", "resources")

enable_authorization = CustomClusterTestSuite.with_args(
    # Same as IMPALAD_ARGS and CATALOGD_ARGS in tests/authorization/test_ranger.py
    impalad_args="--server-name=server1 --ranger_service_type=hive "
                 "--ranger_app_id=impala --authorization_provider=ranger",
    catalogd_args="--server-name=server1 --ranger_service_type=hive "
                  "--ranger_app_id=impala --authorization_provider=ranger"
)


def impalad_admission_ctrl_flags(max_requests, max_queued, pool_max_mem,
                                 proc_mem_limit=None, queue_wait_timeout_ms=None,
                                 admission_control_slots=None, executor_groups=None,
                                 codegen_cache_capacity=0):
  extra_flags = ""
  if proc_mem_limit is not None:
    extra_flags += " -mem_limit={0}".format(proc_mem_limit)
  if queue_wait_timeout_ms is not None:
    extra_flags += " -queue_wait_timeout_ms={0}".format(queue_wait_timeout_ms)
  if admission_control_slots is not None:
    extra_flags += " -admission_control_slots={0}".format(admission_control_slots)
  if executor_groups is not None:
    extra_flags += " -executor_groups={0}".format(executor_groups)
  extra_flags += " -codegen_cache_capacity={0}".format(codegen_cache_capacity)

  return ("-vmodule admission-controller=3 -default_pool_max_requests {0} "
          "-default_pool_max_queued {1} -default_pool_mem_limit {2} {3}".format(
            max_requests, max_queued, pool_max_mem, extra_flags))


admit_one_query_at_a_time = CustomClusterTestSuite.with_args(
    impalad_args=impalad_admission_ctrl_flags(1, 1, 0)
)
admit_no_query = CustomClusterTestSuite.with_args(
    impalad_args=impalad_admission_ctrl_flags(0, 0, 0)
)
single_coordinator = CustomClusterTestSuite.with_args(
    num_exclusive_coordinators=1
)


def impalad_admission_ctrl_config_args(fs_allocation_file, llama_site_file,
                                        additional_args="", make_copy=False):
  """Generates impalad startup flags configuring the fair scheduler and llama site path
     options and setting logging for admission control to VLOG_ROW.

     The specified fair scheduler and llama site files are copied first, and the copies
     are used as the value for the relevant startup flags."""
  fs_allocation_path = os.path.join(RESOURCES_DIR, fs_allocation_file)
  llama_site_path = os.path.join(RESOURCES_DIR, llama_site_file)
  if make_copy:
    copy_fs_allocation_path = os.path.join(RESOURCES_DIR, "copy-" + fs_allocation_file)
    copy_llama_site_path = os.path.join(RESOURCES_DIR, "copy-" + llama_site_file)
    shutil.copy2(fs_allocation_path, copy_fs_allocation_path)
    shutil.copy2(llama_site_path, copy_llama_site_path)
    fs_allocation_path = copy_fs_allocation_path
    llama_site_path = copy_llama_site_path
  return ("-vmodule admission-controller=3 -fair_scheduler_allocation_path %s "
          "-llama_site_path %s %s" % (fs_allocation_path, llama_site_path,
                                      additional_args))
