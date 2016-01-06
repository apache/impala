# Copyright (c) 2016 Cloudera, Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Fixture parametrizations should go here, not in conftest.py.

import pytest

from tests.common.patterns import is_valid_impala_identifier


class UniqueDatabase(object):

  @staticmethod
  def parametrize(name_prefix='test_db'):
    name_prefix = str(name_prefix)
    if not is_valid_impala_identifier(name_prefix):
      raise ValueError('name_prefix "{0}" is not a valid Impala identifier; check '
                       'value for long length or invalid '
                       'characters.'.format(name_prefix))
    return pytest.mark.parametrize('unique_database', [name_prefix], indirect=True)
