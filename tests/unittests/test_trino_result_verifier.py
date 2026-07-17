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

from types import SimpleNamespace

import pytest

from tests.common.base_test_suite import BaseTestSuite
from tests.common.test_result_verifier import verify_trino_results


def _trino_result(data, column_types, query='SELECT 1'):
  return SimpleNamespace(
      data=data, column_labels=['value'], column_types=column_types, query=query)


class TestTrinoResultVerifier(BaseTestSuite):

  def test_verify_trino_results_checks_types(self):
    section = {'RESULTS': '1\n', 'TYPES': 'INT\n'}
    verify_trino_results(section, _trino_result(['1'], ['INT']))

    with pytest.raises(AssertionError):
      verify_trino_results(section, _trino_result(['1'], ['BIGINT']))

  def test_verify_trino_results_uses_types_without_types_section(self):
    # DOUBLE comparison uses a tolerance. The old opaque STRING comparison would
    # reject these two representations of the same floating-point value.
    section = {'RESULTS': '1.0\n'}
    verify_trino_results(section, _trino_result(['1.0000000001'], ['DOUBLE']))

  def test_verify_trino_results_requires_metadata_for_empty_results(self):
    section = {'RESULTS': ''}
    with pytest.raises(AssertionError, match='Trino result types were not collected'):
      verify_trino_results(section, _trino_result([], None))

  @pytest.mark.parametrize('column_type, value', [
      ('FLOAT', 'NaN'),
      ('FLOAT', 'Infinity'),
      ('FLOAT', '-Infinity'),
      ('DOUBLE', 'NaN'),
      ('DOUBLE', 'Infinity'),
      ('DOUBLE', '-Infinity'),
  ])
  def test_verify_trino_results_handles_non_finite_floats(
      self, column_type, value):
    section = {'RESULTS': value + '\n'}
    verify_trino_results(section, _trino_result([value], [column_type]))

  def test_verify_trino_results_updates_types(self):
    section = {'RESULTS': '1\n', 'TYPES': 'BIGINT\n'}
    verify_trino_results(
        section, _trino_result(['1'], ['INT']), update_section=True)
    assert section['TYPES'] == 'INT\n'
