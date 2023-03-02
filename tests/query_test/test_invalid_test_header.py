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
import pytest
from tests.common.impala_test_suite import ImpalaTestSuite

class TestInvalidTestHeader(ImpalaTestSuite):
  """Test correct handling of .test files with malformed header."""

  @classmethod
  def add_test_dimensions(cls):
    super(TestInvalidTestHeader, cls).add_test_dimensions()
    # It's sufficient to run this test once.
    cls.ImpalaTestMatrix.add_constraint(lambda v:\
        v.get_value('table_format').file_format == 'text' and\
        v.get_value('table_format').compression_codec == 'none')

  @classmethod
  def get_workload(cls):
    return 'functional-query'

  def test_invalid_test_header(self, vector):
    """Test that running a test file with an invalid header results in the right exception
    being thrown."""
    with pytest.raises(RuntimeError) as e:
      self.run_test_case('QueryTest/invalid_header', vector)
    assert 'Header must not start with' in str(e.value)

