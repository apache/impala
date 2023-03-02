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
from tests.common.custom_cluster_test_suite import CustomClusterTestSuite


class TestThriftDebugStringExceptions(CustomClusterTestSuite):
  """Regression tests for IMPALA-10450"""

  @CustomClusterTestSuite.with_args(
    catalogd_args="--debug_actions=THRIFT_DEBUG_STRING:EXCEPTION@bad_alloc")
  def test_thrift_debug_str_bad_alloc(self):
    """The test executes a API call to get a catalog object from the debug UI and makes
    sure that catalogd does not crash if the ThriftDebugString throws bad_alloc
    exception."""
    obj = self._get_catalog_object()
    assert "Unexpected exception received" in obj

  @CustomClusterTestSuite.with_args(
    catalogd_args="--debug_actions=THRIFT_DEBUG_STRING:EXCEPTION@TException")
  def test_thrift_debug_str_texception(self):
    """The test executes a API call to get a catalog object from the debug UI and makes
    sure that catalogd does not crash if the ThriftDebugString throws a TException."""
    obj = self._get_catalog_object()
    assert "Unexpected exception received" in obj

  @CustomClusterTestSuite.with_args()
  def test_thrift_debug_str(self):
    """Sanity test which executes API call to get a catalog object and make sure that
    it does not return a error message under normal circumstances."""
    obj = self._get_catalog_object()
    assert "Unexpected exception received" not in obj

  def _get_catalog_object(self):
    """ Return the catalog object of functional.alltypes serialized to string. """
    return self.cluster.catalogd.service.read_debug_webpage(
      "catalog_object?object_type=TABLE&object_name=functional.alltypes")
