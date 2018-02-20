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

import pytest

from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.skip import SkipIf

class TestSkipIf(ImpalaTestSuite):
  """
  This suite tests the effectiveness of various SkipIf decorators.
  TODO: Remove this once we have tests that make use of these decorators.
  """

  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @SkipIf.not_krpc
  def test_skip_if_not_krpc(self):
    assert not pytest.config.option.test_no_krpc

  @SkipIf.not_thrift
  def test_skip_if_not_thrift(self):
    assert pytest.config.option.test_no_krpc
