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
# Basic helper class for making dynamic changes to the admission controller config files.
# This is pretty bare-bones at the moment and only contains functionality necessary for
# the tests it is used for. However, it is generic enough that it can be extended if
# more functionality is required for adding tests.

from __future__ import absolute_import, division, print_function
import os
from time import sleep, time
import xml.etree.ElementTree as ET


class ResourcePoolConfig(object):

  # Mapping of config strings used in the llama_site file with those used on the impala
  # metrics debug page. Add to this dictionary if other configs are need for tests.
  CONFIG_TO_METRIC_STR_MAPPING = {
      'max-query-mem-limit': 'pool-max-query-mem-limit',
      'max-query-cpu-core-per-node-limit': 'pool-max-query-cpu-core-per-node-limit',
      'max-query-cpu-core-coordinator-limit': 'pool-max-query-cpu-core-coordinator-limit'}

  """'impala_service' should point to an impalad to be used for running queries.
  'ac_service' should point to the service running the admission controller and is used
  for checking metrics values on the debug webui."""
  def __init__(self, impala_service, ac_service, llama_site_path):
    self.impala_service = impala_service
    self.ac_service = ac_service
    self.llama_site_path = llama_site_path
    tree = ET.parse(llama_site_path)
    self.root = tree.getroot()

  def set_config_value(self, pool_name, config_str, target_val):
    """Sets the value for the config parameter 'config_str' for the 'pool_name'
    resource pool"""
    node = self.__find_xml_node(self.root, pool_name, config_str)
    node.find('value').text = str(target_val)
    self.__write_xml_to_file(self.root, self.llama_site_path)
    self.__wait_for_impala_to_pickup_config_change(pool_name, config_str, str(target_val))

  def __wait_for_impala_to_pickup_config_change(
      self, pool_name, config_str, target_val, timeout=20):
    """Helper method that constantly sends a query for the 'pool_name' resource pool that
    will be rejected but as a side effect would initiate a refresh of the pool config.
    Then on every refresh it checks the pool metric corresponding to 'config_str' to see
    if impala as picked up the change to that metric and is now equal to the
    'target'val'. Times out after 'timeout' seconds"""
    metric_str = self.CONFIG_TO_METRIC_STR_MAPPING[config_str]
    client = self.impala_service.create_beeswax_client()
    client.set_configuration_option('request_pool', pool_name)
    # set mem_limit to something above the proc limit so that the query always gets
    # rejected.
    client.set_configuration_option('mem_limit', '10G')
    metric_key = "admission-controller.{0}.root.{1}".format(metric_str, pool_name)
    start_time = time()
    while (time() - start_time < timeout):
      client.execute("set enable_trivial_query_for_admission=false")
      handle = client.execute_async("select 'wait_for_config_change'")
      client.close_query(handle)
      current_val = str(self.ac_service.get_metric_value(metric_key))
      if current_val == target_val:
        return
      sleep(0.1)
    assert False, "Timed out waiting for {0} to reach {1}. Current: {2}".format(
      metric_key, target_val, current_val)

  def __write_xml_to_file(self, xml_root, file_name):
    # Make sure the change to the file is atomic. Write to a temp file and replace the
    # original with it.
    temp_path = file_name + "-temp"
    file_handle = open(temp_path, "w")
    file_handle.write(ET.tostring(xml_root))
    file_handle.flush()
    os.fsync(file_handle.fileno())
    file_handle.close()
    os.rename(temp_path, file_name)

  def __find_xml_node(self, xml_root, pool_name, pool_attribute):
    """Returns the xml node corresponding to the 'pool_attribute' for the 'pool_name'"""
    for property in xml_root.getiterator('property'):
      try:
        name = property.find('name').text
        # eg. of name = impala.admission-control.max-query-mem-limit-bytes.root.pool_name
        if pool_name == name.split('.')[-1] and pool_attribute in name:
          return property
      except Exception as e:
        print("Current DOM element being inspected: \n{0}".format(ET.dump(property)))
        raise e
    assert False, "{0} attribute not found for pool {1} in the config XML:\n{2}".format(
      pool_attribute, pool_name, ET.dump(xml_root))
