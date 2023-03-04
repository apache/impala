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
from builtin import filter, map
from os import environ
from tests.util.cluster_controller import ClusterController
from tests.benchmark.plugins import Plugin
import datetime
import threading
import time

class VTunePlugin(Plugin):
  """
  This plugin runs Intel's VTune amplifier

  Before the query is executed, the plugin starts VTune collection. After the query has
  completed, the plugin stops the collection.
  """
  __name__ = "VTunePlugin"
  #TODO: We should make these configurable
  VTUNE_PATH = '/opt/intel/vtune_amplifier_xe_2013/'
  TARGET_PROCESS = 'impalad'
  RESULT_DIR_BASE = '/var/log/impala/vtune/' + '%s' + '/db=%s'
  RESULT_QUERY_SCOPE = '_query=%s_format=%s_iteration=%i'
  KILL_CMD = 'ps aux | grep vtune | grep -v grep | awk \'{print $2}\' | xargs kill -9'


  def __init__(self, *args, **kwargs):
    self.cluster_controller = ClusterController(*args, **kwargs)
    Plugin.__init__(self, *args, **kwargs)
    # This is the unique run identifier
    self.run_tag = environ.get('BUILD_TAG', datetime.datetime.now())
    # This method checks to ensure that VTune is installed in the expected path
    self._check_path_on_hosts()

  def run_pre_hook(self, context):
    # Source VTune variables and build the correct command string. For the workload
    # scope, the database name is added to the result path. For the query scope, the
    # query name and iteration is also added.
    result_dir = self.RESULT_DIR_BASE
    if context.get('scope') == 'Query': result_dir = result_dir + self.RESULT_QUERY_SCOPE
    pre_cmd = ('echo 0 > /proc/sys/kernel/nmi_watchdog\n'
        'source ' + self.VTUNE_PATH + 'amplxe-vars.sh\n'
        'amplxe-cl -collect advanced-hotspots '
        '-result-dir=' + result_dir + ' -target-process=' + self.TARGET_PROCESS)
    table_format_str = context.get('table_format', 'UNKNOWN').replace('/', '-')
    pre_cmd = pre_cmd % (self.run_tag, context.get('db_name', 'UNKNOWN'),
        context.get('short_query_name', 'UNKNOWN'),
        table_format_str,context.get('iteration', 1))
    self.thread = threading.Thread(target=self.cluster_controller.deprecated_run_cmd,
        args=[pre_cmd])
    self.thread.start()
    # TODO: Test whether this is a good time to wait
    # Because we start this colection asychronously, we need to ensure that all the
    # machines are running. For now this is simplier than doing the full check that we
    # do in the post hook.
    time.sleep(2)

  def run_post_hook(self, context):
    # Source VTune variables and build the correct command string. This process is
    # identical to that in run_pre_hook()
    result_dir = self.RESULT_DIR_BASE
    if context.get('scope') == 'Query': result_dir = result_dir + self.RESULT_QUERY_SCOPE
    post_cmd = ('source ' + self.VTUNE_PATH + 'amplxe-vars.sh \n'
        'amplxe-cl -command stop -result-dir=' + result_dir)
    table_format_str = context.get('table_format', 'UNKNOWN').replace('/', '-')
    # TODO: Fix the context dict to remove the ambiguity of the variable name
    # new_query_name
    post_cmd = post_cmd % (self.run_tag, context.get('db_name', 'UNKNOWN'),
        context.get('short_query_name', 'UNKNOWN'), table_format_str,
        context.get('iteration', 1))
    self.cluster_controller.deprecated_run_cmd(post_cmd)
    # Wait for reports to generate and kill hosts that are hanging around
    self._wait_for_completion(2)

  def _check_path_on_hosts(self):
    path_check_cmd = 'if [ -d "%s" ]; then echo "exists"\nfi' % (self.VTUNE_PATH)
    host_check_dict = self.cluster_controller.deprecated_run_cmd(path_check_cmd)
    bad_hosts = [k for k in host_check_dict.keys() if host_check_dict[k] != "exists"]
    if bad_hosts:
      raise RuntimeError('VTune is not installed in the expected path for hosts %s' %
          ",".join(bad_hosts))

  def _wait_for_completion(self, timeout):
    """
    Waits for VTune reports to finish generating.

    On large datasets it can take time for the reports to generate. This method waits for
    a timeout period, checking to see if any machine in the cluster is still running a
    VTune command. After the timeout period, _kill_vtune() is called which kills any
    unterminated VTune commands.
    """
    grep_dict = {}
    reports_done = True
    finish_time = datetime.datetime.now() + datetime.timedelta(minutes=timeout)
    while ((reports_done) and (datetime.datetime.now() < finish_time)):
      grep_dict = self.cluster_controller.deprecated_run_cmd(
          'ps aux|grep vtune|grep -v grep')
      reports_done = any(map(self.__is_not_none_or_empty_str, grep_dict.values()))
      # TODO: Investigate a better length of time for the sleep period between checks
      time.sleep(5)
    self._kill_vtune(grep_dict)

  def _kill_vtune(self, host_dict):
    # This method kills threads that are still hanging around after timeout
    kill_list = list(filter(self.__is_not_none_or_empty_str, host_dict.keys()))
    if kill_list:
      self.cluster_controller.deprecated_run_cmd(self.KILL_CMD, hosts=kill_list)

  def __is_not_none_or_empty_str(self, s):
    return s != None and s != ''
